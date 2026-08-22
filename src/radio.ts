import {
  Constants,
  SerialConnection,
  type MeshCoreWaitingMessage,
} from "@liamcottle/meshcore.js";
import { createHash } from "node:crypto";
import { SerialPort } from "serialport";

import { FIELDLINK_MAX_DATAGRAM_BYTES } from "./protocol.js";

const FLOOD_PATH_LENGTH = 0xff;
const INBOX_POLL_INTERVAL_MS = 500;
const DEFAULT_COMMAND_TIMEOUT_MS = 10_000;
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;
const DEFAULT_IDLE_TIMEOUT_MS = 60_000;
const MIN_CHANNEL_DATAGRAM_FIRMWARE_CODE = 12;

export const FIELDLINK_DATA_TYPE = Constants.DataTypes.Dev;

export interface ChannelDatagram {
  readonly channel: number;
  readonly dataType: number;
  readonly snrDb: number;
  readonly pathLength: number;
  readonly bytes: Uint8Array;
}

export type DatagramListener = (
  datagram: ChannelDatagram,
) => void | Promise<void>;

export interface DatagramRadio {
  send(channel: number, bytes: Uint8Array): Promise<void>;
  onDatagram(listener: DatagramListener): () => void;
  waitUntilIdle(): Promise<void>;
}

export interface RadioPort {
  readonly path: string;
  readonly manufacturer?: string;
  readonly serialNumber?: string;
  readonly vendorId?: string;
  readonly productId?: string;
}

export interface ChannelConfiguration {
  readonly index: number;
  readonly name: string;
  readonly secret: Uint8Array;
}

export interface RadioIdentity {
  readonly publicKey: Uint8Array;
  readonly fingerprint: string;
  readonly name: string;
  readonly model: string;
  readonly firmwareVersion: string;
  readonly firmwareBuildDate: string;
  readonly firmwareProtocolCode: number;
  readonly clientProtocolVersion: number;
  readonly radio: {
    readonly frequency: number;
    readonly bandwidth: number;
    readonly spreadingFactor: number;
    readonly codingRate: number;
    readonly transmitPower: number;
    readonly maximumTransmitPower: number;
  };
}

export type InboxMessage = MeshCoreWaitingMessage;

type CompanionConnection = Pick<
  SerialConnection,
  | "close"
  | "getChannel"
  | "getSelfInfo"
  | "deviceQuery"
  | "sendChannelData"
  | "syncNextMessage"
> & {
  connect(): Promise<void>;
  on(
    eventName: string | number,
    listener: (...arguments_: readonly unknown[]) => void,
  ): unknown;
  off(
    eventName: string | number,
    listener: (...arguments_: readonly unknown[]) => void,
  ): unknown;
};

export interface MeshCoreRadioOptions {
  readonly commandTimeoutMs?: number;
  readonly connection?: CompanionConnection;
  readonly onInboxMessage?: (message: InboxMessage) => void | Promise<void>;
  readonly onListenerError?: (error: Error) => void | Promise<void>;
}

/** MeshCore's Node adapter does not await the underlying serial-port close. */
class FieldLinkSerialConnection extends SerialConnection {
  readonly #serialPort: SerialPort;

  constructor(path: string) {
    super();
    this.#serialPort = new SerialPort({
      path,
      baudRate: 115_200,
      autoOpen: false,
    });
  }

  async connect(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.#serialPort.open((error) => {
        if (error) {
          reject(asError(error));
          return;
        }
        resolve();
      });
    });
    this.#serialPort.on("data", (data: Buffer) => {
      void this.onDataReceived(new Uint8Array(data));
    });
    this.#serialPort.once("close", () => {
      this.onDisconnected();
    });
    this.#serialPort.on("error", (error) => {
      this.emit("error", error);
    });
    await this.onConnected();
  }

  override async close(): Promise<void> {
    if (!this.#serialPort.isOpen) {
      if (!this.#serialPort.destroyed) {
        this.#serialPort.destroy();
      }
      return;
    }
    await new Promise<void>((resolve, reject) => {
      this.#serialPort.close((error) => {
        if (error) {
          reject(asError(error));
          return;
        }
        resolve();
      });
    });
  }

  protected override async write(bytes: Uint8Array): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.#serialPort.write(bytes, (error) => {
        if (error) {
          reject(asError(error));
          return;
        }
        resolve();
      });
    });
  }
}

export class MeshCoreRadio implements DatagramRadio {
  readonly #connection: CompanionConnection;
  readonly #listeners = new Set<DatagramListener>();
  readonly #path: string;
  readonly #commandTimeoutMs: number;
  readonly #onInboxMessage: MeshCoreRadioOptions["onInboxMessage"];
  readonly #onListenerError: MeshCoreRadioOptions["onListenerError"];
  readonly #messageWaitingListener: (...arguments_: readonly unknown[]) => void;
  readonly #disconnectedListener: (...arguments_: readonly unknown[]) => void;
  readonly #transportErrorListener: (...arguments_: readonly unknown[]) => void;
  #commandTail: Promise<void> = Promise.resolve();
  #drainPromise: Promise<void> | undefined;
  #drainRequestVersion = 0;
  #open = false;
  #closing = false;
  #lifecycleVersion = 0;
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #fatalError: Error | undefined;

  constructor(path: string, options: MeshCoreRadioOptions = {}) {
    this.#path = path;
    this.#commandTimeoutMs =
      options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
    this.#connection =
      options.connection ?? new FieldLinkSerialConnection(path);
    this.#onInboxMessage = options.onInboxMessage;
    this.#onListenerError = options.onListenerError;
    this.#messageWaitingListener = () => {
      this.#requestDrain();
    };
    this.#disconnectedListener = () => {
      if (this.#open && !this.#closing) {
        this.#makeFatal(new Error(`${this.#path} disconnected`));
      }
    };
    this.#transportErrorListener = (error: unknown) => {
      this.#makeFatal(asError(error));
    };
  }

  async open(connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
    if (this.#open) {
      return;
    }

    const lifecycleVersion = ++this.#lifecycleVersion;
    try {
      await withTimeout(
        this.#connection.connect(),
        connectTimeoutMs,
        `opening ${this.#path}`,
      );
      if (lifecycleVersion !== this.#lifecycleVersion) {
        throw new Error(`Opening ${this.#path} was cancelled by close`);
      }
    } catch (error: unknown) {
      const openError = asError(error);
      try {
        await this.#connection.close();
      } catch (closeError: unknown) {
        throw new AggregateError(
          [openError, asError(closeError)],
          `Could not open and clean up ${this.#path}`,
        );
      }
      throw openError;
    }

    this.#open = true;
    this.#connection.on(
      Constants.PushCodes.MsgWaiting,
      this.#messageWaitingListener,
    );
    this.#connection.on("disconnected", this.#disconnectedListener);
    this.#connection.on("error", this.#transportErrorListener);
    this.#pollTimer = setInterval(() => {
      this.#requestDrain();
    }, INBOX_POLL_INTERVAL_MS);
    await this.flushInbox();
  }

  async close(): Promise<void> {
    if (this.#closing) {
      return;
    }
    this.#closing = true;
    this.#lifecycleVersion += 1;
    this.#open = false;
    this.#stopPolling();
    this.#connection.off(
      Constants.PushCodes.MsgWaiting,
      this.#messageWaitingListener,
    );
    this.#connection.off("disconnected", this.#disconnectedListener);
    this.#connection.off("error", this.#transportErrorListener);

    const errors: Error[] = [];
    try {
      await this.waitUntilIdle();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    try {
      await this.#connection.close();
    } catch (error: unknown) {
      errors.push(asError(error));
    } finally {
      this.#closing = false;
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, `Could not cleanly close ${this.#path}`);
    }
  }

  async getChannel(index: number): Promise<ChannelConfiguration> {
    this.#throwIfUnavailable();
    const channel = await this.#runTimedCommand(
      () => this.#connection.getChannel(index),
      `reading channel ${index} from ${this.#path}`,
    );
    return {
      index: channel.channelIdx,
      name: channel.name,
      secret: channel.secret,
    };
  }

  async getIdentity(): Promise<RadioIdentity> {
    this.#throwIfUnavailable();
    const self = await this.#runTimedCommand(
      () => this.#connection.getSelfInfo(),
      `reading identity from ${this.#path}`,
    );
    const device = await this.#runTimedCommand(
      () =>
        this.#connection.deviceQuery(
          Constants.SupportedCompanionProtocolVersion,
        ),
      `querying firmware on ${this.#path}`,
    );
    if (device.firmwareVer < MIN_CHANNEL_DATAGRAM_FIRMWARE_CODE) {
      throw new Error(
        `${this.#path} reports Companion firmware code ${device.firmwareVer}; channel datagrams require 12 or newer`,
      );
    }
    const [model = "unknown", firmwareVersion = "unknown"] =
      device.manufacturerModel.split("\0").filter((part) => part.length > 0);
    return {
      publicKey: self.publicKey,
      fingerprint: createHash("sha256")
        .update(self.publicKey)
        .digest("hex")
        .slice(0, 16),
      name: self.name,
      model,
      firmwareVersion,
      firmwareBuildDate: device.firmware_build_date,
      firmwareProtocolCode: device.firmwareVer,
      clientProtocolVersion: Constants.SupportedCompanionProtocolVersion,
      radio: {
        frequency: self.radioFreq,
        bandwidth: self.radioBw,
        spreadingFactor: self.radioSf,
        codingRate: self.radioCr,
        transmitPower: self.txPower,
        maximumTransmitPower: self.maxTxPower,
      },
    };
  }

  async send(channel: number, bytes: Uint8Array): Promise<void> {
    this.#throwIfUnavailable();
    if (bytes.length > FIELDLINK_MAX_DATAGRAM_BYTES) {
      throw new RangeError(
        `MeshCore datagrams cannot exceed ${FIELDLINK_MAX_DATAGRAM_BYTES} bytes`,
      );
    }
    try {
      await this.#runTimedCommand(
        () =>
          this.#connection.sendChannelData(
            channel,
            FLOOD_PATH_LENGTH,
            new Uint8Array(),
            FIELDLINK_DATA_TYPE,
            bytes,
          ),
        `sending through ${this.#path}`,
      );
    } catch (error: unknown) {
      const sendError = asError(error);
      throw new Error(
        `Could not send through ${this.#path}: ${sendError.message}`,
        {
          cause: sendError,
        },
      );
    }
  }

  onDatagram(listener: DatagramListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  async flushInbox(): Promise<void> {
    await this.#startDrain();
  }

  async waitUntilIdle(): Promise<void> {
    if (this.#fatalError !== undefined) {
      throw new Error(
        `${this.#path} is unavailable: ${this.#fatalError.message}`,
        { cause: this.#fatalError },
      );
    }
    await withTimeout(
      Promise.all([this.#commandTail, this.#drainPromise]),
      DEFAULT_IDLE_TIMEOUT_MS,
      `waiting for ${this.#path} commands to finish`,
    );
  }

  #requestDrain(): void {
    this.#drainRequestVersion += 1;
    void this.#startDrain().catch((error: unknown) => {
      this.#makeFatal(asError(error));
    });
  }

  #startDrain(): Promise<void> {
    if (!this.#open) {
      return Promise.resolve();
    }
    if (this.#drainPromise !== undefined) {
      return this.#drainPromise;
    }
    this.#drainPromise = this.#drainInbox().finally(() => {
      this.#drainPromise = undefined;
    });
    return this.#drainPromise;
  }

  async #drainInbox(): Promise<void> {
    for (;;) {
      const observedRequestVersion = this.#drainRequestVersion;
      for (;;) {
        const waitingMessage = await this.#runTimedCommand(
          () => this.#connection.syncNextMessage(),
          `reading messages from ${this.#path}`,
        );
        if (waitingMessage === null) {
          break;
        }
        await this.#notifyInboxMessage(waitingMessage);
        if (!("channelData" in waitingMessage)) {
          continue;
        }
        const channelData = waitingMessage.channelData;
        const datagram: ChannelDatagram = {
          channel: channelData.channelIdx,
          dataType: channelData.dataType,
          snrDb: channelData.snr,
          pathLength: channelData.pathLen,
          bytes: channelData.data,
        };
        for (const listener of this.#listeners) {
          try {
            await listener(datagram);
          } catch (error: unknown) {
            await this.#notifyListenerError(asError(error));
          }
        }
      }
      if (!this.#open || observedRequestVersion === this.#drainRequestVersion) {
        return;
      }
    }
  }

  async #notifyInboxMessage(message: InboxMessage): Promise<void> {
    if (this.#onInboxMessage === undefined) {
      return;
    }
    try {
      await this.#onInboxMessage(message);
    } catch (error: unknown) {
      await this.#notifyListenerError(asError(error));
    }
  }

  async #notifyListenerError(error: Error): Promise<void> {
    if (this.#onListenerError !== undefined) {
      try {
        await this.#onListenerError(error);
      } catch {
        // The reporting hook must not stop the shared inbox drain.
      }
    }
  }

  #runCommand<Result>(operation: () => Promise<Result>): Promise<Result> {
    const result = this.#commandTail.then(operation);
    this.#commandTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  async #runTimedCommand<Result>(
    operation: () => Promise<Result>,
    description: string,
  ): Promise<Result> {
    try {
      return await withTimeout(
        this.#runCommand(operation),
        this.#commandTimeoutMs,
        description,
      );
    } catch (error: unknown) {
      const commandError = asError(error);
      if (commandError instanceof OperationTimeoutError) {
        this.#makeFatal(commandError);
      }
      throw commandError;
    }
  }

  #makeFatal(error: Error): void {
    this.#fatalError ??= error;
    this.#stopPolling();
  }

  #stopPolling(): void {
    if (this.#pollTimer !== undefined) {
      clearInterval(this.#pollTimer);
      this.#pollTimer = undefined;
    }
  }

  #throwIfUnavailable(): void {
    if (!this.#open) {
      throw new Error(`${this.#path} is not open`);
    }
    if (this.#fatalError !== undefined) {
      throw new Error(
        `${this.#path} is unavailable: ${this.#fatalError.message}`,
        {
          cause: this.#fatalError,
        },
      );
    }
  }
}

export async function listRadioPorts(): Promise<readonly RadioPort[]> {
  const ports = await SerialPort.list();
  return ports
    .map((port) => ({
      path: port.path,
      ...(port.manufacturer === undefined
        ? {}
        : { manufacturer: port.manufacturer }),
      ...(port.serialNumber === undefined
        ? {}
        : { serialNumber: port.serialNumber }),
      ...(port.vendorId === undefined ? {} : { vendorId: port.vendorId }),
      ...(port.productId === undefined ? {} : { productId: port.productId }),
    }))
    .sort((left, right) => left.path.localeCompare(right.path));
}

function withTimeout<Result>(
  promise: Promise<Result>,
  timeoutMs: number,
  operation: string,
): Promise<Result> {
  return new Promise<Result>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(
        new OperationTimeoutError(
          `Timed out ${operation} after ${timeoutMs} ms`,
        ),
      );
    }, timeoutMs);
    void promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(asError(error));
      },
    );
  });
}

class OperationTimeoutError extends Error {}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
