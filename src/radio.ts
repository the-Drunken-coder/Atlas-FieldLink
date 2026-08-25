import {
  Constants,
  SerialConnection,
  type MeshCoreWaitingMessage,
} from "@liamcottle/meshcore.js";
import { createHash } from "node:crypto";
import { readdir } from "node:fs/promises";
import { SerialPort } from "serialport";

import { MESHCORE_DATAGRAM_BYTES } from "./frame.js";
import type { FieldLinkTransport, TransportDatagram } from "./node.js";
import { nodeIdFromPublicKey, type NodeId } from "./node-types.js";

const FLOOD_PATH_LENGTH = 0xff;
const INBOX_POLL_INTERVAL_MS = 500;
const DEFAULT_COMMAND_TIMEOUT_MS = 10_000;
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;
const DEFAULT_IDLE_TIMEOUT_MS = 60_000;
const MIN_CHANNEL_DATAGRAM_FIRMWARE_CODE = 12;

export const FIELDLINK_DATA_TYPE = Constants.DataTypes.Dev;

export interface RadioPort {
  readonly path: string;
  readonly manufacturer?: string;
  readonly serialNumber?: string;
  readonly vendorId?: string;
  readonly productId?: string;
}

type ListedRadioPort = Awaited<ReturnType<typeof SerialPort.list>>[number];

export interface ChannelConfiguration {
  readonly index: number;
  readonly name: string;
  readonly secret: Uint8Array;
}

export interface SafeChannelConfiguration {
  readonly index: number;
  readonly name: string;
  readonly configured: boolean;
  readonly keyFingerprint: string;
}

export interface RadioIdentity {
  readonly publicKey: Uint8Array;
  readonly nodeId: NodeId;
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

export type SafeRadioIdentity = Omit<RadioIdentity, "publicKey">;
export type InboxMessage = MeshCoreWaitingMessage;

export type CompanionConnection = Pick<
  SerialConnection,
  | "close"
  | "getChannel"
  | "getChannels"
  | "getSelfInfo"
  | "deviceQuery"
  | "getStatsCore"
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

export interface MeshCoreTransportOptions {
  readonly channel: number;
  readonly commandTimeoutMs?: number;
  readonly connection?: CompanionConnection;
  readonly onInboxMessage?: (message: InboxMessage) => void | Promise<void>;
  readonly onListenerError?: (error: Error) => void | Promise<void>;
  readonly onFatalError?: (error: Error) => void | Promise<void>;
}

/** MeshCore's bundled Node adapter does not await serial open or close. */
class FieldLinkSerialConnection extends SerialConnection {
  readonly #serialPort: SerialPort;

  constructor(path: string) {
    super();
    this.#serialPort = new SerialPort({
      path,
      baudRate: 115_200,
      autoOpen: false,
    });
    this.#serialPort.on("data", (data: Buffer) => {
      void this.onDataReceived(new Uint8Array(data));
    });
    this.#serialPort.on("close", () => {
      this.onDisconnected();
    });
    this.#serialPort.on("error", (error) => {
      this.emit("error", error);
    });
  }

  async connect(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.#serialPort.open((error) => {
        if (error) {
          reject(asError(error));
        } else {
          resolve();
        }
      });
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
        } else {
          resolve();
        }
      });
    });
  }

  protected override async write(bytes: Uint8Array): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.#serialPort.write(bytes, (error) => {
        if (error) {
          reject(asError(error));
        } else {
          resolve();
        }
      });
    });
  }
}

/** Owns one Companion USB radio and exposes only FieldLink channel datagrams. */
export class MeshCoreTransport implements FieldLinkTransport {
  readonly #connection: CompanionConnection;
  readonly #listeners = new Set<
    (datagram: TransportDatagram) => void | Promise<void>
  >();
  readonly #path: string;
  readonly #channel: number;
  readonly #commandTimeoutMs: number;
  readonly #onInboxMessage: MeshCoreTransportOptions["onInboxMessage"];
  readonly #onListenerError: MeshCoreTransportOptions["onListenerError"];
  readonly #onFatalError: MeshCoreTransportOptions["onFatalError"];
  readonly #messageWaitingListener: (...arguments_: readonly unknown[]) => void;
  readonly #disconnectedListener: (...arguments_: readonly unknown[]) => void;
  readonly #transportErrorListener: (...arguments_: readonly unknown[]) => void;
  #commandTail: Promise<void> = Promise.resolve();
  #drainPromise: Promise<void> | undefined;
  #drainRequestSequence = 0;
  #open = false;
  #inboxActive = false;
  #datagramDeliveryEnabled = true;
  #closing = false;
  #lifecycleGeneration = 0;
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #fatalError: Error | undefined;

  constructor(path: string, options: MeshCoreTransportOptions) {
    this.#path = path;
    this.#channel = options.channel;
    this.#commandTimeoutMs =
      options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
    this.#connection =
      options.connection ?? new FieldLinkSerialConnection(path);
    this.#onInboxMessage = options.onInboxMessage;
    this.#onListenerError = options.onListenerError;
    this.#onFatalError = options.onFatalError;
    this.#messageWaitingListener = () => {
      this.#requestDrain();
    };
    this.#disconnectedListener = () => {
      if (this.#open && !this.#closing) {
        this.#reportFatal(new Error(`${this.#path} disconnected`));
      }
    };
    this.#transportErrorListener = (error: unknown) => {
      this.#reportFatal(asError(error));
    };
  }

  async open(connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
    if (this.#open) {
      return;
    }
    const lifecycleGeneration = ++this.#lifecycleGeneration;
    try {
      await withTimeout(
        this.#connection.connect(),
        connectTimeoutMs,
        `opening ${this.#path}`,
      );
      if (lifecycleGeneration !== this.#lifecycleGeneration) {
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
    this.#connection.on("disconnected", this.#disconnectedListener);
    this.#connection.on("error", this.#transportErrorListener);
  }

  async startInbox(options?: {
    readonly deliverDatagrams?: boolean;
  }): Promise<void> {
    this.#throwIfUnavailable();
    if (this.#inboxActive) {
      return;
    }
    this.#datagramDeliveryEnabled = options?.deliverDatagrams ?? true;
    this.#inboxActive = true;
    this.#connection.on(
      Constants.PushCodes.MsgWaiting,
      this.#messageWaitingListener,
    );
    this.#pollTimer = setInterval(() => {
      this.#requestDrain();
    }, INBOX_POLL_INTERVAL_MS);
    this.#pollTimer.unref();
    await this.flushInbox();
  }

  async enableDatagramDelivery(): Promise<void> {
    this.#throwIfUnavailable();
    if (!this.#inboxActive) {
      throw new Error(`${this.#path} inbox is not active`);
    }
    await this.flushInbox();
    this.#datagramDeliveryEnabled = true;
  }

  async close(): Promise<void> {
    if (this.#closing) {
      return;
    }
    this.#closing = true;
    this.#lifecycleGeneration += 1;
    this.#open = false;
    this.#inboxActive = false;
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

  async getChannel(index = this.#channel): Promise<ChannelConfiguration> {
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

  async getChannels(): Promise<readonly ChannelConfiguration[]> {
    this.#throwIfUnavailable();
    const channels = await this.#runTimedCommand(
      () => this.#connection.getChannels(),
      `reading channels from ${this.#path}`,
    );
    if (channels.some((channel, index) => channel.channelIdx !== index)) {
      throw new Error(
        `${this.#path} returned channel slots out of order or with gaps`,
      );
    }
    return channels.map((channel) => ({
      index: channel.channelIdx,
      name: channel.name,
      secret: channel.secret,
    }));
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
        `${this.#path} reports Companion firmware code ${device.firmwareVer}; channel datagrams require ${MIN_CHANNEL_DATAGRAM_FIRMWARE_CODE} or newer`,
      );
    }
    const [model = "unknown", firmwareVersion = "unknown"] =
      device.manufacturerModel.split("\0").filter((part) => part.length > 0);
    const nodeId = nodeIdFromPublicKey(self.publicKey);
    return {
      publicKey: self.publicKey,
      nodeId,
      fingerprint: nodeId,
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

  async send(bytes: Uint8Array): Promise<void> {
    this.#throwIfUnavailable();
    if (bytes.length > MESHCORE_DATAGRAM_BYTES) {
      throw new RangeError(
        `MeshCore channel data cannot exceed ${MESHCORE_DATAGRAM_BYTES} bytes`,
      );
    }
    try {
      await this.#runTimedCommand(
        () =>
          this.#connection.sendChannelData(
            this.#channel,
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
        { cause: sendError },
      );
    }
  }

  async getQueueLength(): Promise<number> {
    this.#throwIfUnavailable();
    const stats = await this.#runTimedCommand(
      () => this.#connection.getStatsCore(),
      `reading Core Stats from ${this.#path}`,
    );
    if (!Number.isInteger(stats.data.queueLen) || stats.data.queueLen < 0) {
      throw new Error(`${this.#path} returned an invalid Core Stats queueLen`);
    }
    return stats.data.queueLen;
  }

  onDatagram(
    listener: (datagram: TransportDatagram) => void | Promise<void>,
  ): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  async flushInbox(): Promise<void> {
    this.#throwIfUnavailable();
    this.#drainRequestSequence += 1;
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
      Promise.all([this.#commandTail, this.#drainPromise]).then(
        () => undefined,
      ),
      DEFAULT_IDLE_TIMEOUT_MS,
      `waiting for ${this.#path} commands to finish`,
    );
  }

  #requestDrain(): void {
    this.#drainRequestSequence += 1;
    void this.#startDrain().catch((error: unknown) => {
      this.#reportFatal(asError(error));
    });
  }

  #startDrain(): Promise<void> {
    if (!this.#open || this.#fatalError !== undefined) {
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
      const observedRequestSequence = this.#drainRequestSequence;
      for (;;) {
        if (!this.#open) {
          return;
        }
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
        const data = waitingMessage.channelData;
        if (
          data.channelIdx !== this.#channel ||
          data.dataType !== FIELDLINK_DATA_TYPE ||
          !this.#datagramDeliveryEnabled
        ) {
          continue;
        }
        const datagram: TransportDatagram = {
          bytes: data.data,
          snrDb: data.snr,
          pathLength: data.pathLen,
        };
        for (const listener of this.#listeners) {
          try {
            await listener(datagram);
          } catch (error: unknown) {
            await this.#notifyListenerError(asError(error));
          }
        }
      }
      if (observedRequestSequence === this.#drainRequestSequence) {
        return;
      }
    }
  }

  async #notifyInboxMessage(message: InboxMessage): Promise<void> {
    await this.#onInboxMessage?.(message);
  }

  async #notifyListenerError(error: Error): Promise<void> {
    if (this.#onListenerError === undefined) {
      return;
    }
    try {
      await this.#onListenerError(error);
    } catch {
      // Evidence hooks cannot stop the shared inbox drain.
    }
  }

  async #notifyFatalError(error: Error): Promise<void> {
    try {
      await this.#onFatalError?.(error);
    } catch {
      // The transport is already fatal and must not resume inbox drains.
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
        this.#reportFatal(commandError);
      }
      throw commandError;
    }
  }

  #reportFatal(error: Error): void {
    if (this.#makeFatal(error)) {
      void this.#notifyFatalError(error);
    }
  }

  #makeFatal(error: Error): boolean {
    const firstFailure = this.#fatalError === undefined;
    this.#fatalError ??= error;
    this.#inboxActive = false;
    this.#stopPolling();
    this.#connection.off(
      Constants.PushCodes.MsgWaiting,
      this.#messageWaitingListener,
    );
    return firstFailure;
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
        { cause: this.#fatalError },
      );
    }
  }
}

export function safeRadioIdentity(identity: RadioIdentity): SafeRadioIdentity {
  return {
    nodeId: identity.nodeId,
    fingerprint: identity.fingerprint,
    name: identity.name,
    model: identity.model,
    firmwareVersion: identity.firmwareVersion,
    firmwareBuildDate: identity.firmwareBuildDate,
    firmwareProtocolCode: identity.firmwareProtocolCode,
    clientProtocolVersion: identity.clientProtocolVersion,
    radio: identity.radio,
  };
}

export function safeChannelConfiguration(
  channel: ChannelConfiguration,
): SafeChannelConfiguration {
  return {
    index: channel.index,
    name: channel.name,
    configured: channel.secret.some((byte) => byte !== 0),
    keyFingerprint: createHash("sha256")
      .update(channel.secret)
      .digest("hex")
      .slice(0, 16),
  };
}

export function selectMatchingChannel(
  a: readonly SafeChannelConfiguration[],
  b: readonly SafeChannelConfiguration[],
): SafeChannelConfiguration | undefined {
  const bByIndex = new Map(b.map((channel) => [channel.index, channel]));
  return [...a]
    .sort((left, right) => left.index - right.index)
    .find((channel) => {
      const other = bByIndex.get(channel.index);
      return (
        channel.configured &&
        other?.configured === true &&
        channel.name === other.name &&
        channel.keyFingerprint === other.keyFingerprint
      );
    });
}

export async function listRadioPorts(): Promise<readonly RadioPort[]> {
  const ports = (await SerialPort.list()).map(toRadioPort);
  let darwinDeviceNames: readonly string[] | undefined;
  if (process.platform === "darwin") {
    try {
      darwinDeviceNames = await readdir("/dev");
    } catch {
      darwinDeviceNames = undefined;
    }
  }
  return radioPortCandidates(ports, process.platform, darwinDeviceNames);
}

export function radioPortCandidates(
  ports: readonly RadioPort[],
  platform: NodeJS.Platform,
  darwinDeviceNames?: readonly string[],
): readonly RadioPort[] {
  if (platform !== "darwin") {
    return ports
      .filter(isUsbSerialCandidate)
      .sort((left, right) => left.path.localeCompare(right.path));
  }

  const byPath = new Map(ports.map((port) => [port.path, port]));
  const calloutPaths =
    darwinDeviceNames === undefined
      ? ports
          .filter(isUsbSerialCandidate)
          .map((port) => darwinCalloutPath(port.path))
      : darwinDeviceNames
          .filter((name) => name.startsWith("cu."))
          .map((name) => `/dev/${name}`)
          .filter((path) => {
            const metadata =
              byPath.get(path) ??
              byPath.get(path.replace("/dev/cu.", "/dev/tty."));
            return isUsbSerialCandidate(metadata ?? { path });
          });

  const candidates = new Map<string, RadioPort>();
  for (const path of calloutPaths) {
    const metadata =
      byPath.get(path) ?? byPath.get(path.replace("/dev/cu.", "/dev/tty."));
    candidates.set(path, { ...(metadata ?? {}), path });
  }
  return [...candidates.values()].sort((left, right) =>
    left.path.localeCompare(right.path),
  );
}

function toRadioPort(port: ListedRadioPort): RadioPort {
  return {
    path: port.path,
    ...(port.manufacturer === undefined
      ? {}
      : { manufacturer: port.manufacturer }),
    ...(port.serialNumber === undefined
      ? {}
      : { serialNumber: port.serialNumber }),
    ...(port.vendorId === undefined ? {} : { vendorId: port.vendorId }),
    ...(port.productId === undefined ? {} : { productId: port.productId }),
  };
}

function isUsbSerialCandidate(port: RadioPort): boolean {
  const path = port.path.toLowerCase();
  if (
    [
      "bluetooth",
      "debug-console",
      "debug_console",
      "debugconsole",
      "audio",
      "soundcore",
    ].some((marker) => path.includes(marker))
  ) {
    return false;
  }
  return (
    path.includes("usbserial") ||
    path.includes("usbmodem") ||
    path.includes("wchusb") ||
    path.includes("slab_usb") ||
    path.includes("ttyusb") ||
    path.includes("ttyacm") ||
    port.vendorId !== undefined ||
    port.productId !== undefined
  );
}

function darwinCalloutPath(path: string): string {
  return path.startsWith("/dev/tty.")
    ? `/dev/cu.${path.slice("/dev/tty.".length)}`
    : path;
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
