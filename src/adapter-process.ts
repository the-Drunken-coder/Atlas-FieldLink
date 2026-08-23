import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import type { Readable, Writable } from "node:stream";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import type { AdapterCommand } from "./args.js";
import {
  MeshCoreRadio,
  type ChannelConfiguration,
  type ChannelDatagram,
  type DatagramListener,
  type DatagramRadio,
  type InboxMessage,
  type MeshCoreRadioOptions,
  type RadioIdentity,
} from "./radio.js";

const REQUEST_TIMEOUT_MS = 75_000;
const EXIT_TIMEOUT_MS = 5_000;
const BYTES_MARKER = "$fieldlinkBytes";
const CONTROLLER_OPTIONS_WITH_VALUES = new Set([
  "--inspect-port",
  "--inspect-publish-uid",
  "--watch-kill-signal",
  "--watch-path",
]);

interface ManagedRadio extends DatagramRadio {
  open(): Promise<void>;
  close(): Promise<void>;
  getIdentity(): Promise<RadioIdentity>;
  getChannel(index: number): Promise<ChannelConfiguration>;
}

type RadioFactory = (
  path: string,
  options: Pick<MeshCoreRadioOptions, "onInboxMessage" | "onListenerError">,
) => ManagedRadio;

type AdapterOperation =
  | {
      readonly type: "send";
      readonly bytes: Uint8Array;
    }
  | { readonly type: "idle" }
  | { readonly type: "close" };

type AdapterRequest = AdapterOperation & { readonly id: number };

type AdapterMessage =
  | {
      readonly type: "ready";
      readonly processId: number;
      readonly identity: RadioIdentity;
      readonly channel: ChannelConfiguration;
    }
  | { readonly type: "datagram"; readonly datagram: ChannelDatagram }
  | { readonly type: "inbox-message"; readonly message: InboxMessage }
  | { readonly type: "listener-error"; readonly error: string }
  | { readonly type: "response"; readonly id: number; readonly ok: true }
  | {
      readonly type: "response";
      readonly id: number;
      readonly ok: false;
      readonly error: string;
    };

export interface AdapterProgram {
  readonly executable: string;
  readonly arguments: readonly string[];
}

export interface StartAdapterProcessOptions {
  readonly path: string;
  readonly channel: number;
  readonly allowInboxDrain: true;
  readonly program?: AdapterProgram;
  readonly onInboxMessage?: (message: InboxMessage) => void | Promise<void>;
  readonly onListenerError?: (error: Error) => void | Promise<void>;
  readonly onStderr?: (message: string) => void;
  readonly onStderrEnd?: () => void;
  readonly requestTimeoutMs?: number;
}

export interface ServeAdapterOptions {
  readonly path: string;
  readonly channel: number;
  readonly input: Readable;
  readonly output: Writable;
  readonly processId?: number;
  readonly signal?: AbortSignal;
  readonly createRadio?: RadioFactory;
}

/**
 * Owns one MeshCore radio and exposes it as a newline-delimited JSON process.
 * Standard output is reserved for the control protocol.
 */
export async function serveAdapter(
  options: ServeAdapterOptions,
): Promise<void> {
  const writer = new WireWriter(options.output);
  const createRadio =
    options.createRadio ??
    ((path: string, radioOptions: MeshCoreRadioOptions) =>
      new MeshCoreRadio(path, radioOptions));
  const radio = createRadio(options.path, {
    onInboxMessage: (message) =>
      writer.write({ type: "inbox-message", message } satisfies AdapterMessage),
    onListenerError: (error) =>
      writer.write({
        type: "listener-error",
        error: error.message,
      } satisfies AdapterMessage),
  });
  const lines = createInterface({ input: options.input, crlfDelay: Infinity });
  const requests = lines[Symbol.asyncIterator]();
  let nextRequest = requests.next();
  const stopReading = (): void => {
    lines.close();
  };
  options.signal?.addEventListener("abort", stopReading, { once: true });
  let closed = false;
  const closeRadio = async (): Promise<void> => {
    if (closed) {
      return;
    }
    closed = true;
    await radio.close();
  };
  const unsubscribe = radio.onDatagram((datagram) =>
    writer.write({ type: "datagram", datagram } satisfies AdapterMessage),
  );

  try {
    await radio.open();
    const [identity, channel] = await Promise.all([
      radio.getIdentity(),
      radio.getChannel(options.channel),
    ]);
    if (options.signal?.aborted === true) {
      return;
    }
    await writer.write({
      type: "ready",
      processId: options.processId ?? process.pid,
      identity,
      channel,
    } satisfies AdapterMessage);

    for (;;) {
      const requestLine = await nextRequest;
      if (requestLine.done) {
        break;
      }
      nextRequest = requests.next();
      const line = requestLine.value;
      if (line.trim().length === 0) {
        continue;
      }
      const request = parseAdapterRequest(line);
      const keepRunning = await handleRequest(
        request,
        radio,
        options.channel,
        writer,
        closeRadio,
      );
      if (!keepRunning) {
        break;
      }
    }
  } finally {
    options.signal?.removeEventListener("abort", stopReading);
    unsubscribe();
    lines.close();
    await closeRadio();
    await writer.flush();
  }
}

export async function runAdapterProcess(
  command: AdapterCommand,
): Promise<number> {
  const controller = new AbortController();
  const handleSignal = (signal: NodeJS.Signals): void => {
    if (!controller.signal.aborted) {
      controller.abort(signal);
    }
  };
  const onSigint = (): void => {
    handleSignal("SIGINT");
  };
  const onSigterm = (): void => {
    handleSignal("SIGTERM");
  };
  process.on("SIGINT", onSigint);
  process.on("SIGTERM", onSigterm);

  try {
    await serveAdapter({
      path: command.radio,
      channel: command.channel,
      input: process.stdin,
      output: process.stdout,
      signal: controller.signal,
    });
    return controller.signal.aborted ? 130 : 0;
  } finally {
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigterm);
    process.stdin.destroy();
  }
}

/** A parent-side radio proxy backed by one FieldLink adapter process. */
export class AdapterProcessRadio implements DatagramRadio {
  readonly path: string;
  readonly channel: number;
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #listeners = new Set<DatagramListener>();
  readonly #pending = new Map<number, PendingRequest>();
  readonly #readyPromise: Promise<void>;
  readonly #writer: WireWriter;
  readonly #onInboxMessage: StartAdapterProcessOptions["onInboxMessage"];
  readonly #onListenerError: StartAdapterProcessOptions["onListenerError"];
  readonly #requestTimeoutMs: number;
  #ready: Extract<AdapterMessage, { readonly type: "ready" }> | undefined;
  #resolveReady: () => void = () => undefined;
  #rejectReady: (error: Error) => void = () => undefined;
  #eventTail: Promise<void> = Promise.resolve();
  #nextRequestId = 1;
  #closing = false;
  #closed = false;
  #failure: Error | undefined;
  #closePromise: Promise<void> | undefined;
  #exit: ChildExit | undefined;

  private constructor(
    child: ChildProcessWithoutNullStreams,
    options: StartAdapterProcessOptions,
  ) {
    this.#child = child;
    this.#writer = new WireWriter(child.stdin);
    this.#onInboxMessage = options.onInboxMessage;
    this.#onListenerError = options.onListenerError;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? REQUEST_TIMEOUT_MS;
    this.path = options.path;
    this.channel = options.channel;
    this.#readyPromise = new Promise((resolve, reject) => {
      this.#resolveReady = resolve;
      this.#rejectReady = reject;
    });
  }

  get processId(): number {
    return this.#readyMessage().processId;
  }

  get identity(): RadioIdentity {
    return this.#readyMessage().identity;
  }

  get channelConfiguration(): ChannelConfiguration {
    return this.#readyMessage().channel;
  }

  static async start(
    options: StartAdapterProcessOptions,
  ): Promise<AdapterProcessRadio> {
    const program = options.program ?? currentAdapterProgram();
    const child = spawn(
      program.executable,
      [
        ...program.arguments,
        "adapter",
        "--radio",
        options.path,
        "--channel",
        String(options.channel),
        "--allow-inbox-drain",
      ],
      {
        stdio: "pipe",
        // The controller owns foreground signals and closes adapters over NDJSON.
        detached: process.platform !== "win32",
      },
    );
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      stderr = `${stderr}${chunk}`.slice(-8_192);
      options.onStderr?.(chunk);
    });
    child.stderr.once("end", () => {
      options.onStderrEnd?.();
    });

    const adapter = new AdapterProcessRadio(child, options);
    adapter.#readMessages();
    try {
      // Radio operations own their deadlines. Inbox draining has no fixed duration.
      await adapter.#readyPromise;
      return adapter;
    } catch (error: unknown) {
      child.stdin.end();
      await terminateChild(child);
      const cause = asError(error);
      const detail = stderr.trim().replace(/^fieldlink:\s*/, "");
      if (detail.length === 0) {
        throw cause;
      }
      throw new Error(`Adapter for ${options.path} failed: ${detail}`, {
        cause,
      });
    }
  }

  send(channel: number, bytes: Uint8Array): Promise<void> {
    if (channel !== this.channel) {
      return Promise.reject(
        new Error(
          `Adapter for ${this.path} opened channel ${this.channel}, not ${channel}`,
        ),
      );
    }
    return this.#request({ type: "send", bytes: Uint8Array.from(bytes) });
  }

  onDatagram(listener: DatagramListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  async waitUntilIdle(): Promise<void> {
    await this.#request({ type: "idle" });
    await this.#eventTail;
  }

  close(): Promise<void> {
    this.#closePromise ??= this.#close();
    return this.#closePromise;
  }

  async #close(): Promise<void> {
    if (this.#closed) {
      await this.#eventTail;
      if (this.#failure !== undefined) {
        throw this.#failure;
      }
      return;
    }
    this.#closing = true;
    let closeError: Error | undefined;
    try {
      await this.#request({ type: "close" });
    } catch (error: unknown) {
      closeError = asError(error);
    }
    this.#child.stdin.end();
    await terminateChild(this.#child);
    this.#closed = true;
    await this.#eventTail;
    if (closeError !== undefined) {
      throw closeError;
    }
  }

  #request(request: AdapterOperation): Promise<void> {
    if (this.#failure !== undefined) {
      return Promise.reject(this.#failure);
    }
    if (this.#closed) {
      return Promise.reject(new Error(`Adapter for ${this.path} is closed`));
    }
    const id = this.#nextRequestId;
    this.#nextRequestId += 1;
    const message: AdapterRequest = { ...request, id };
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#fail(
          new Error(
            `Adapter for ${this.path} did not answer ${request.type} after ${this.#requestTimeoutMs} ms`,
          ),
        );
      }, this.#requestTimeoutMs);
      this.#pending.set(id, {
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      void this.#writer.write(message).catch((error: unknown) => {
        this.#fail(asError(error));
      });
    });
  }

  #readMessages(): void {
    const lines = createInterface({
      input: this.#child.stdout,
      crlfDelay: Infinity,
    });
    void (async () => {
      try {
        for await (const line of lines) {
          if (line.trim().length === 0) {
            continue;
          }
          this.#handleMessage(parseAdapterMessage(line));
        }
        if (!this.#closing || this.#pending.size > 0) {
          const failure =
            this.#exit === undefined
              ? new Error(`Adapter for ${this.path} closed its output`)
              : new Error(
                  `Adapter for ${this.path} exited ${describeExit(this.#exit.code, this.#exit.signal)}`,
                );
          this.#fail(failure);
        }
      } catch (error: unknown) {
        this.#fail(asError(error));
      } finally {
        this.#closed = true;
        lines.close();
      }
    })();
    this.#child.once("error", (error) => {
      this.#fail(error);
    });
    this.#child.once("exit", (code, signal) => {
      this.#exit = { code, signal };
    });
  }

  #handleMessage(message: AdapterMessage): void {
    if (this.#ready === undefined) {
      if (message.type === "ready") {
        this.#ready = message;
        this.#resolveReady();
        return;
      }
      if (message.type === "inbox-message") {
        void Promise.resolve(this.#onInboxMessage?.(message.message)).catch(
          (error: unknown) => this.#notifyListenerError(asError(error)),
        );
        return;
      }
      if (message.type === "listener-error") {
        void this.#notifyListenerError(new Error(message.error));
        return;
      }
      if (message.type === "datagram") {
        return;
      }
      this.#fail(
        new Error(`Adapter for ${this.path} answered before it was ready`),
      );
      return;
    }
    if (message.type === "response") {
      const pending = this.#pending.get(message.id);
      if (pending === undefined) {
        this.#fail(
          new Error(
            `Adapter for ${this.path} answered unknown request ${message.id}`,
          ),
        );
        return;
      }
      this.#pending.delete(message.id);
      if (message.ok) {
        pending.resolve();
      } else {
        pending.reject(new Error(message.error));
      }
      return;
    }
    if (message.type === "datagram") {
      this.#eventTail = this.#eventTail.then(async () => {
        for (const listener of this.#listeners) {
          try {
            await listener(message.datagram);
          } catch (error: unknown) {
            await this.#notifyListenerError(asError(error));
          }
        }
      });
      return;
    }
    if (message.type === "inbox-message") {
      void Promise.resolve(this.#onInboxMessage?.(message.message)).catch(
        (error: unknown) => this.#notifyListenerError(asError(error)),
      );
      return;
    }
    if (message.type === "listener-error") {
      void this.#notifyListenerError(new Error(message.error));
      return;
    }
    this.#fail(new Error(`Adapter for ${this.path} sent ready more than once`));
  }

  async #notifyListenerError(error: Error): Promise<void> {
    try {
      await this.#onListenerError?.(error);
    } catch {
      // Reporting failures must not stop adapter traffic.
    }
  }

  #fail(error: Error): void {
    if (this.#failure !== undefined) {
      return;
    }
    this.#failure = error;
    this.#rejectReady(error);
    for (const pending of this.#pending.values()) {
      pending.reject(error);
    }
    this.#pending.clear();
  }

  #readyMessage(): Extract<AdapterMessage, { readonly type: "ready" }> {
    if (this.#ready === undefined) {
      throw new Error(`Adapter for ${this.path} is not ready`);
    }
    return this.#ready;
  }
}

interface PendingRequest {
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
}

interface ChildExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

class WireWriter {
  readonly #output: Writable;
  #tail: Promise<void> = Promise.resolve();

  constructor(output: Writable) {
    this.#output = output;
  }

  write(value: AdapterRequest | AdapterMessage): Promise<void> {
    const write = this.#tail.then(() => writeWireLine(this.#output, value));
    this.#tail = write.catch(() => undefined);
    return write;
  }

  flush(): Promise<void> {
    return this.#tail;
  }
}

async function handleRequest(
  request: AdapterRequest,
  radio: ManagedRadio,
  channel: number,
  writer: WireWriter,
  closeRadio: () => Promise<void>,
): Promise<boolean> {
  try {
    if (request.type === "send") {
      await radio.send(channel, request.bytes);
    } else if (request.type === "idle") {
      await radio.waitUntilIdle();
    } else {
      await closeRadio();
    }
    await writer.write({ type: "response", id: request.id, ok: true });
    return request.type !== "close";
  } catch (error: unknown) {
    await writer.write({
      type: "response",
      id: request.id,
      ok: false,
      error: asError(error).message,
    });
    return request.type !== "close";
  }
}

function currentAdapterProgram(): AdapterProgram {
  const extension = import.meta.url.endsWith(".ts") ? "ts" : "js";
  return {
    executable: process.execPath,
    arguments: [
      ...adapterNodeArguments(process.execArgv),
      fileURLToPath(new URL(`./cli.${extension}`, import.meta.url)),
    ],
  };
}

/** Keeps source loaders and preloads while removing controller-only Node modes. */
export function adapterNodeArguments(arguments_: readonly string[]): string[] {
  const filtered: string[] = [];
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === undefined) {
      continue;
    }
    if (CONTROLLER_OPTIONS_WITH_VALUES.has(argument)) {
      index += 1;
      continue;
    }
    if (
      argument.startsWith("--inspect") ||
      argument === "--watch" ||
      argument.startsWith("--watch-")
    ) {
      continue;
    }
    filtered.push(argument);
  }
  return filtered;
}

async function terminateChild(
  child: ChildProcessWithoutNullStreams,
): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  if (await waitForExit(child, EXIT_TIMEOUT_MS)) {
    return;
  }
  child.kill("SIGTERM");
  if (!(await waitForExit(child, EXIT_TIMEOUT_MS))) {
    child.kill("SIGKILL");
    await waitForExit(child, EXIT_TIMEOUT_MS);
  }
}

function waitForExit(
  child: ChildProcessWithoutNullStreams,
  timeoutMs: number,
): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve(true);
  }
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.removeListener("exit", onExit);
      resolve(false);
    }, timeoutMs);
    const onExit = (): void => {
      clearTimeout(timer);
      resolve(true);
    };
    child.once("exit", onExit);
  });
}

function writeWireLine(
  output: Writable,
  value: AdapterRequest | AdapterMessage,
): Promise<void> {
  const line = `${JSON.stringify(value, wireReplacer)}\n`;
  return new Promise((resolve, reject) => {
    output.write(line, "utf8", (error: Error | null | undefined) => {
      if (error === null || error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

function parseAdapterRequest(line: string): AdapterRequest {
  const value: unknown = JSON.parse(line, wireReviver);
  if (!isRecord(value) || !isRequestId(value.id)) {
    throw new Error("Adapter request must contain a positive integer id");
  }
  if (value.type === "idle" || value.type === "close") {
    return { id: value.id, type: value.type };
  }
  if (value.type === "send" && value.bytes instanceof Uint8Array) {
    return { id: value.id, type: "send", bytes: value.bytes };
  }
  throw new Error("Adapter request type is invalid");
}

function parseAdapterMessage(line: string): AdapterMessage {
  const value: unknown = JSON.parse(line, wireReviver);
  if (!isRecord(value) || typeof value.type !== "string") {
    throw new Error("Adapter message is invalid");
  }
  if (
    value.type === "ready" &&
    Number.isSafeInteger(value.processId) &&
    isRadioIdentity(value.identity) &&
    isChannelConfiguration(value.channel)
  ) {
    return {
      type: "ready",
      processId: value.processId as number,
      identity: value.identity,
      channel: value.channel,
    };
  }
  if (value.type === "datagram" && isChannelDatagram(value.datagram)) {
    return { type: "datagram", datagram: value.datagram };
  }
  if (value.type === "inbox-message" && "message" in value) {
    return { type: "inbox-message", message: value.message as InboxMessage };
  }
  if (value.type === "listener-error" && typeof value.error === "string") {
    return { type: "listener-error", error: value.error };
  }
  if (value.type === "response" && isRequestId(value.id) && value.ok === true) {
    return { type: "response", id: value.id, ok: true };
  }
  if (
    value.type === "response" &&
    isRequestId(value.id) &&
    value.ok === false &&
    typeof value.error === "string"
  ) {
    return {
      type: "response",
      id: value.id,
      ok: false,
      error: value.error,
    };
  }
  throw new Error(`Adapter message type ${value.type} is invalid`);
}

function wireReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return { [BYTES_MARKER]: Buffer.from(value).toString("base64") };
  }
  return value;
}

function wireReviver(_key: string, value: unknown): unknown {
  if (
    isRecord(value) &&
    Object.keys(value).length === 1 &&
    typeof value[BYTES_MARKER] === "string"
  ) {
    return Uint8Array.from(Buffer.from(value[BYTES_MARKER], "base64"));
  }
  return value;
}

function isRadioIdentity(value: unknown): value is RadioIdentity {
  if (!isRecord(value) || !isRecord(value.radio)) {
    return false;
  }
  return (
    value.publicKey instanceof Uint8Array &&
    typeof value.fingerprint === "string" &&
    typeof value.name === "string" &&
    typeof value.model === "string" &&
    typeof value.firmwareVersion === "string" &&
    typeof value.firmwareBuildDate === "string" &&
    typeof value.firmwareProtocolCode === "number" &&
    typeof value.clientProtocolVersion === "number" &&
    typeof value.radio.frequency === "number" &&
    typeof value.radio.bandwidth === "number" &&
    typeof value.radio.spreadingFactor === "number" &&
    typeof value.radio.codingRate === "number" &&
    typeof value.radio.transmitPower === "number" &&
    typeof value.radio.maximumTransmitPower === "number"
  );
}

function isChannelConfiguration(value: unknown): value is ChannelConfiguration {
  return (
    isRecord(value) &&
    typeof value.index === "number" &&
    typeof value.name === "string" &&
    value.secret instanceof Uint8Array
  );
}

function isChannelDatagram(value: unknown): value is ChannelDatagram {
  return (
    isRecord(value) &&
    typeof value.channel === "number" &&
    typeof value.dataType === "number" &&
    typeof value.snrDb === "number" &&
    typeof value.pathLength === "number" &&
    value.bytes instanceof Uint8Array
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isRequestId(value: unknown): value is number {
  return Number.isSafeInteger(value) && typeof value === "number" && value > 0;
}

function describeExit(
  code: number | null,
  signal: NodeJS.Signals | null,
): string {
  if (signal !== null) {
    return `from ${signal}`;
  }
  return `with code ${code ?? "unknown"}`;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
