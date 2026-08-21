import { Constants, NodeJSSerialConnection } from "@liamcottle/meshcore.js";
import { SerialPort } from "serialport";

import { FIELDLINK_MAX_DATAGRAM_BYTES } from "./protocol.js";

const FLOOD_PATH_LENGTH = 0xff;
const INBOX_POLL_INTERVAL_MS = 500;
const DEFAULT_COMMAND_TIMEOUT_MS = 10_000;
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;

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

export class MeshCoreRadio implements DatagramRadio {
  readonly #connection: NodeJSSerialConnection;
  readonly #listeners = new Set<DatagramListener>();
  readonly #path: string;
  readonly #commandTimeoutMs: number;
  readonly #messageWaitingListener: (...arguments_: readonly unknown[]) => void;
  readonly #disconnectedListener: (...arguments_: readonly unknown[]) => void;
  #commandTail: Promise<void> = Promise.resolve();
  #draining = false;
  #open = false;
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #fatalError: Error | undefined;

  constructor(path: string, commandTimeoutMs = DEFAULT_COMMAND_TIMEOUT_MS) {
    this.#path = path;
    this.#commandTimeoutMs = commandTimeoutMs;
    this.#connection = new NodeJSSerialConnection(path);
    this.#messageWaitingListener = () => {
      this.#requestDrain();
    };
    this.#disconnectedListener = () => {
      if (this.#open) {
        this.#fatalError = new Error(`${this.#path} disconnected`);
      }
    };
  }

  async open(connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
    if (this.#open) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Timed out opening ${this.#path}`));
      }, connectTimeoutMs);

      const connected = (): void => {
        clearTimeout(timer);
        resolve();
      };

      this.#connection.once("connected", connected);
      void this.#connection.connect().catch((error: unknown) => {
        clearTimeout(timer);
        this.#connection.off("connected", connected);
        reject(asError(error));
      });
    });

    this.#open = true;
    this.#connection.on(
      Constants.PushCodes.MsgWaiting,
      this.#messageWaitingListener,
    );
    this.#connection.on("disconnected", this.#disconnectedListener);
    this.#pollTimer = setInterval(() => {
      this.#requestDrain();
    }, INBOX_POLL_INTERVAL_MS);
    await this.flushInbox();
  }

  async close(): Promise<void> {
    this.#open = false;
    if (this.#pollTimer !== undefined) {
      clearInterval(this.#pollTimer);
      this.#pollTimer = undefined;
    }
    this.#connection.off(
      Constants.PushCodes.MsgWaiting,
      this.#messageWaitingListener,
    );
    this.#connection.off("disconnected", this.#disconnectedListener);
    await this.#connection.close();
  }

  async getChannel(index: number): Promise<ChannelConfiguration> {
    this.#throwIfUnavailable();
    const channel = await withTimeout(
      this.#runCommand(() => this.#connection.getChannel(index)),
      this.#commandTimeoutMs,
      `reading channel ${index} from ${this.#path}`,
    );
    return {
      index: channel.channelIdx,
      name: channel.name,
      secret: channel.secret,
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
      await withTimeout(
        this.#runCommand(() =>
          this.#connection.sendChannelData(
            channel,
            FLOOD_PATH_LENGTH,
            new Uint8Array(),
            FIELDLINK_DATA_TYPE,
            bytes,
          ),
        ),
        this.#commandTimeoutMs,
        `sending through ${this.#path}`,
      );
    } catch (error: unknown) {
      const sendError = asError(error);
      if (sendError instanceof OperationTimeoutError) {
        this.#fatalError = sendError;
      }
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
    await this.#drainInbox();
  }

  #requestDrain(): void {
    void this.#drainInbox().catch((error: unknown) => {
      this.#fatalError = asError(error);
      if (this.#pollTimer !== undefined) {
        clearInterval(this.#pollTimer);
        this.#pollTimer = undefined;
      }
    });
  }

  async #drainInbox(): Promise<void> {
    if (!this.#open || this.#draining) {
      return;
    }

    this.#draining = true;
    try {
      for (;;) {
        const waitingMessage = await withTimeout(
          this.#runCommand(() => this.#connection.syncNextMessage()),
          this.#commandTimeoutMs,
          `reading messages from ${this.#path}`,
        );
        if (waitingMessage === null) {
          return;
        }
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
          await listener(datagram);
        }
      }
    } finally {
      this.#draining = false;
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
