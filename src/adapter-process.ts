import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { extname } from "node:path";
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";

import type { AdapterCommand } from "./args.js";
import { AdapterEvidence } from "./evidence.js";
import { messageRegistry, type SupportedMessage } from "./messages/index.js";
import {
  FieldLinkNode,
  type FieldLinkEvent,
  type NodeId,
  type Priority,
  type ReceivedMessage,
  type SendOptions,
  type SendResult,
} from "./node.js";
import {
  retryStrategies,
  type RetryStrategyName,
} from "./retry-strategies/index.js";
import {
  FIELDLINK_DATA_TYPE,
  MeshCoreTransport,
  safeChannelConfiguration,
  safeRadioIdentity,
  type InboxMessage,
  type SafeChannelConfiguration,
  type SafeRadioIdentity,
} from "./radio.js";

const REQUEST_TIMEOUT_MS = 31 * 60 * 1000;
const EXIT_TIMEOUT_MS = 5_000;
const STDOUT_EXIT_GRACE_MS = 25;
const BYTES_MARKER = "$fieldlinkBytes";
const CONTROLLER_OPTIONS_WITH_VALUES = new Set([
  "--inspect-port",
  "--inspect-publish-uid",
  "--watch-kill-signal",
  "--watch-path",
]);

export interface AdapterReady {
  readonly processId: number;
  readonly identity: SafeRadioIdentity;
  readonly channel: SafeChannelConfiguration;
  readonly nodeId: NodeId;
  readonly supportedMessages: readonly {
    readonly id: number;
    readonly name: string;
    readonly defaultPriority: Priority;
  }[];
  readonly retryStrategies: readonly {
    readonly id: number;
    readonly name: RetryStrategyName;
  }[];
  readonly delivery: {
    readonly meshCoreDataType: number;
    readonly meshCoreMode: "flood";
    readonly maximumChannelDatagramBytes: 163;
  };
}

interface AdapterRuntime {
  readonly node: FieldLinkNode;
  readonly ready: AdapterReady;
  readonly start?: () => Promise<void>;
  readonly activate?: () => Promise<void>;
}

interface RuntimeFactoryOptions {
  readonly path: string;
  readonly channel: number;
  readonly processId: number;
  readonly onInboxMessage: (message: InboxMessage) => void | Promise<void>;
  readonly onListenerError: (error: Error) => void | Promise<void>;
}

type RuntimeFactory = (
  options: RuntimeFactoryOptions,
) => Promise<AdapterRuntime>;

type AdapterRequest =
  | {
      readonly id: number;
      readonly type: "send";
      readonly message: SupportedMessage;
      readonly destination: string;
      readonly priority?: Priority;
      readonly retryStrategy?: RetryStrategyName;
    }
  | { readonly id: number; readonly type: "activate" }
  | { readonly id: number; readonly type: "abort"; readonly targetId: number }
  | { readonly id: number; readonly type: "close" };

type AdapterMessage =
  | ({ readonly type: "ready" } & AdapterReady)
  | { readonly type: "message"; readonly message: WireReceivedMessage }
  | { readonly type: "event"; readonly event: FieldLinkEvent }
  | { readonly type: "inbox-message"; readonly message: InboxMessage }
  | { readonly type: "listener-error"; readonly error: string }
  | {
      readonly type: "response";
      readonly id: number;
      readonly ok: true;
      readonly result?: SendResult;
    }
  | {
      readonly type: "response";
      readonly id: number;
      readonly ok: false;
      readonly error: string;
    };

interface WireReceivedMessage extends Omit<ReceivedMessage, "receivedAt"> {
  readonly receivedAt: string;
}

export interface ServeAdapterOptions {
  readonly path: string;
  readonly channel: number;
  readonly input: Readable;
  readonly output: Writable;
  readonly processId?: number;
  readonly signal?: AbortSignal;
  readonly createRuntime?: RuntimeFactory;
  readonly onInboxMessage?: (message: InboxMessage) => void | Promise<void>;
}

/** Owns one FieldLinkNode and reserves stdout for typed NDJSON. */
export async function serveAdapter(
  options: ServeAdapterOptions,
): Promise<void> {
  const writer = new WireWriter(options.output);
  const createRuntime = options.createRuntime ?? createDefaultRuntime;
  const runtime = await createRuntime({
    path: options.path,
    channel: options.channel,
    processId: options.processId ?? process.pid,
    onInboxMessage: async (message) => {
      await options.onInboxMessage?.(message);
      await writer.write({ type: "inbox-message", message });
    },
    onListenerError: (error) =>
      writer.write({ type: "listener-error", error: error.message }),
  });
  const lines = createInterface({ input: options.input, crlfDelay: Infinity });
  const requests = lines[Symbol.asyncIterator]();
  let nextRequest = requests.next();
  const active = new Map<number, AbortController>();
  const activeOperations = new Map<number, Promise<void>>();
  let activated = false;
  let closing = false;
  const stopReading = (): void => {
    lines.close();
  };
  options.signal?.addEventListener("abort", stopReading, { once: true });
  const unsubscribeMessage = runtime.node.onMessage((received) =>
    writer.write({
      type: "message",
      message: { ...received, receivedAt: received.receivedAt.toISOString() },
    }),
  );
  const unsubscribeEvent = runtime.node.onEvent((event) =>
    writer.write({ type: "event", event }),
  );

  try {
    await runtime.start?.();
    await writer.write({ type: "ready", ...runtime.ready });
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
      if (request.type === "activate") {
        try {
          await runtime.activate?.();
          activated = true;
          await writer.write({ type: "response", id: request.id, ok: true });
        } catch (error: unknown) {
          await writer.write({
            type: "response",
            id: request.id,
            ok: false,
            error: asError(error).message,
          });
        }
        continue;
      }
      if (request.type === "abort") {
        active
          .get(request.targetId)
          ?.abort(new Error(`Adapter request ${request.targetId} aborted`));
        await writer.write({ type: "response", id: request.id, ok: true });
        continue;
      }
      if (request.type === "close") {
        closing = true;
        for (const controller of active.values()) {
          controller.abort(new Error("Adapter is closing"));
        }
        await Promise.allSettled(activeOperations.values());
        await runtime.node.close();
        await writer.write({ type: "response", id: request.id, ok: true });
        break;
      }
      if (!activated) {
        await writer.write({
          type: "response",
          id: request.id,
          ok: false,
          error: "Adapter is not activated",
        });
        continue;
      }

      const controller = new AbortController();
      active.set(request.id, controller);
      const operation = runtime.node
        .send(request.message, {
          destination: request.destination,
          signal: controller.signal,
          ...(request.priority === undefined
            ? {}
            : { priority: request.priority }),
          ...(request.retryStrategy === undefined
            ? {}
            : { retryStrategy: request.retryStrategy }),
        })
        .then(
          (result) =>
            writer.write({
              type: "response",
              id: request.id,
              ok: true,
              result,
            }),
          (error: unknown) =>
            writer.write({
              type: "response",
              id: request.id,
              ok: false,
              error: asError(error).message,
            }),
        )
        .finally(() => {
          active.delete(request.id);
          activeOperations.delete(request.id);
        });
      activeOperations.set(request.id, operation);
    }
  } finally {
    options.signal?.removeEventListener("abort", stopReading);
    unsubscribeMessage();
    unsubscribeEvent();
    lines.close();
    for (const controller of active.values()) {
      controller.abort(new Error("Adapter input closed"));
    }
    await Promise.allSettled(activeOperations.values());
    if (!closing) {
      await runtime.node.close();
    }
    await writer.flush();
  }
}

async function createDefaultRuntime(
  options: RuntimeFactoryOptions,
): Promise<AdapterRuntime> {
  const transport = new MeshCoreTransport(options.path, {
    channel: options.channel,
    onInboxMessage: options.onInboxMessage,
    onListenerError: options.onListenerError,
  });
  try {
    await transport.open();
    const [identity, channel] = await Promise.all([
      transport.getIdentity(),
      transport.getChannel(),
    ]);
    const safeChannel = safeChannelConfiguration(channel);
    if (!safeChannel.configured) {
      throw new Error(`Channel ${channel.index} is not configured`);
    }
    const node = new FieldLinkNode({
      nodeId: identity.nodeId,
      transport,
    });
    return {
      node,
      start: () => transport.startInbox({ deliverDatagrams: false }),
      activate: () => transport.enableDatagramDelivery(),
      ready: {
        processId: options.processId,
        identity: safeRadioIdentity(identity),
        channel: safeChannel,
        nodeId: identity.nodeId,
        supportedMessages: messageRegistry.map((definition) => ({
          id: definition.id,
          name: definition.name,
          defaultPriority: definition.defaultPriority,
        })),
        retryStrategies: retryStrategies.map((strategy) => ({
          id: strategy.id,
          name: strategy.name,
        })),
        delivery: {
          meshCoreDataType: FIELDLINK_DATA_TYPE,
          meshCoreMode: "flood",
          maximumChannelDatagramBytes: 163,
        },
      },
    };
  } catch (error: unknown) {
    await transport.close().catch(() => undefined);
    throw error;
  }
}

export async function runAdapterProcess(
  command: AdapterCommand,
): Promise<number> {
  const controller = new AbortController();
  const abort = (): void => {
    controller.abort(new Error("Adapter interrupted"));
  };
  process.once("SIGINT", abort);
  process.once("SIGTERM", abort);
  let evidence: AdapterEvidence | undefined;
  try {
    if (!command.evidenceManagedByParent) {
      if (command.output === undefined) {
        throw new Error("Adapter evidence output is required");
      }
      evidence = await AdapterEvidence.create(command.output);
    }
    if (!controller.signal.aborted) {
      const adapterEvidence = evidence;
      await serveAdapter({
        path: command.radio,
        channel: command.channel,
        input: process.stdin,
        output: process.stdout,
        signal: controller.signal,
        ...(adapterEvidence === undefined
          ? {}
          : {
              onInboxMessage: (message: InboxMessage) =>
                adapterEvidence.record("inbox-message", { message }),
            }),
      });
    }
    return controller.signal.aborted ? 130 : 0;
  } finally {
    process.removeListener("SIGINT", abort);
    process.removeListener("SIGTERM", abort);
    await evidence?.close();
  }
}

export interface AdapterProgram {
  readonly executable: string;
  readonly arguments: readonly string[];
}

export interface StartAdapterProcessOptions {
  readonly path: string;
  readonly channel: number;
  readonly allowInboxDrain: true;
  readonly program?: AdapterProgram;
  readonly signal?: AbortSignal;
  readonly onInboxMessage?: (message: InboxMessage) => void | Promise<void>;
  readonly onListenerError?: (error: Error) => void | Promise<void>;
  readonly onStderr?: (message: string) => void;
  readonly onStderrEnd?: () => void;
  readonly requestTimeoutMs?: number;
  readonly exitTimeoutMs?: number;
}

interface PendingRequest {
  readonly resolve: (result: SendResult | undefined) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
  readonly cleanup: () => void;
}

interface AdapterExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

/** Parent-side FieldLinkNode proxy for a radio-owning adapter process. */
export class AdapterProcessNode {
  readonly processId: number;
  readonly identity: SafeRadioIdentity;
  readonly channel: SafeChannelConfiguration;
  readonly nodeId: NodeId;
  readonly supportedMessages: AdapterReady["supportedMessages"];
  readonly retryStrategies: AdapterReady["retryStrategies"];
  readonly delivery: AdapterReady["delivery"];
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #requestTimeoutMs: number;
  readonly #exitTimeoutMs: number;
  readonly #pending = new Map<number, PendingRequest>();
  readonly #messageListeners = new Set<
    (message: ReceivedMessage) => void | Promise<void>
  >();
  readonly #eventListeners = new Set<
    (event: FieldLinkEvent) => void | Promise<void>
  >();
  #nextRequestId = 1;
  #failure: Error | undefined;
  #activation: Promise<void> | undefined;
  #activated = false;
  #closed = false;
  #exit: Promise<AdapterExit>;
  #readerDone: Promise<void> = Promise.resolve();

  private constructor(
    child: ChildProcessWithoutNullStreams,
    ready: AdapterReady,
    options: StartAdapterProcessOptions,
    exit: Promise<AdapterExit>,
  ) {
    this.#child = child;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? REQUEST_TIMEOUT_MS;
    this.#exitTimeoutMs = options.exitTimeoutMs ?? EXIT_TIMEOUT_MS;
    this.#exit = exit;
    this.processId = ready.processId;
    this.identity = ready.identity;
    this.channel = ready.channel;
    this.nodeId = ready.nodeId;
    this.supportedMessages = ready.supportedMessages;
    this.retryStrategies = ready.retryStrategies;
    this.delivery = ready.delivery;
  }

  static async start(
    options: StartAdapterProcessOptions,
  ): Promise<AdapterProcessNode> {
    // JavaScript callers are not protected by the literal TypeScript type.
    const allowInboxDrain: unknown = options.allowInboxDrain;
    if (allowInboxDrain !== true) {
      throw new Error(
        "Adapter process startup requires explicit inbox-drain acknowledgement",
      );
    }
    const program = options.program ?? defaultAdapterProgram(options);
    const child = spawn(program.executable, [...program.arguments], {
      stdio: ["pipe", "pipe", "pipe"],
      detached: process.platform !== "win32",
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      options.onStderr?.(chunk);
    });
    child.stderr.once("end", () => {
      options.onStderrEnd?.();
    });

    const exit = new Promise<AdapterExit>((resolve) => {
      child.once("exit", (code, signal) => {
        resolve({ code, signal });
      });
    });
    const closed = new Promise<AdapterExit>((resolve) => {
      child.once("close", (code, signal) => {
        resolve({ code, signal });
      });
    });
    let rejectSpawnError: ((error: Error) => void) | undefined;
    const onSpawnError = (error: unknown): void => {
      rejectSpawnError?.(asError(error));
    };
    const spawnError = new Promise<never>((_resolve, reject) => {
      rejectSpawnError = reject;
      child.once("error", onSpawnError);
    });
    const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
    const iterator = lines[Symbol.asyncIterator]();
    let rejectStartupAbort: ((error: Error) => void) | undefined;
    const startupAbort = new Promise<never>((_resolve, reject) => {
      rejectStartupAbort = reject;
    });
    const abort = (): void => {
      child.kill("SIGTERM");
      if (options.signal !== undefined) {
        rejectStartupAbort?.(abortError(options.signal));
      }
    };
    options.signal?.addEventListener("abort", abort, { once: true });
    let ready: ({ readonly type: "ready" } & AdapterReady) | undefined;
    try {
      if (options.signal?.aborted === true) {
        throw abortError(options.signal);
      }
      while (ready === undefined) {
        const first = await Promise.race([
          iterator.next(),
          spawnError,
          startupAbort,
        ]);
        if (first.done) {
          throw new Error("Adapter stdout ended before ready");
        }
        const message = parseAdapterMessage(first.value);
        switch (message.type) {
          case "ready":
            ready = message;
            break;
          case "inbox-message":
            await options.onInboxMessage?.(message.message);
            break;
          case "listener-error":
            await options.onListenerError?.(new Error(message.error));
            break;
          case "event":
          case "message":
            // Initial inbox traffic is evidence, not part of the new run.
            break;
          case "response":
            throw new Error("Adapter sent response before ready");
        }
      }
    } catch (error: unknown) {
      lines.close();
      const startError = asError(error);
      try {
        await terminateAndReapAdapter(
          child,
          closed,
          options.exitTimeoutMs ?? EXIT_TIMEOUT_MS,
        );
      } catch (cleanupError: unknown) {
        throw new AggregateError(
          [startError, asError(cleanupError)],
          "Could not start and clean up adapter process",
        );
      }
      throw startError;
    } finally {
      options.signal?.removeEventListener("abort", abort);
    }
    child.off("error", onSpawnError);
    const node = new AdapterProcessNode(child, ready, options, exit);
    child.on("error", (error) => {
      node.#fail(asError(error));
    });
    node.#readerDone = node.#read(iterator, lines, options);
    void exit.then(async ({ code, signal }) => {
      await node.#readerDone;
      if (!node.#closed || code !== 0) {
        node.#fail(
          new Error(
            `Adapter exited (${signal ?? `code ${code ?? "unknown"}`})`,
          ),
        );
      }
    });
    return node;
  }

  activate(signal?: AbortSignal): Promise<void> {
    if (this.#activation !== undefined) {
      return this.#activation;
    }
    const abort = (): void => {
      if (signal === undefined) {
        return;
      }
      this.#fail(abortError(signal));
      this.#child.kill("SIGTERM");
    };
    signal?.addEventListener("abort", abort, { once: true });
    this.#activation = this.#request({ type: "activate" }, undefined)
      .then(() => {
        this.#activated = true;
      })
      .finally(() => {
        signal?.removeEventListener("abort", abort);
      });
    if (signal?.aborted === true) {
      abort();
    }
    return this.#activation;
  }

  send(message: SupportedMessage, options: SendOptions): Promise<SendResult> {
    if (!this.#activated) {
      return Promise.reject(new Error("Adapter is not activated"));
    }
    return this.#request(
      {
        type: "send",
        message,
        destination: options.destination,
        ...(options.priority === undefined
          ? {}
          : { priority: options.priority }),
        ...(options.retryStrategy === undefined
          ? {}
          : { retryStrategy: options.retryStrategy }),
      },
      options.signal,
    ).then((result) => {
      if (result === undefined) {
        throw new Error("Adapter send returned no result");
      }
      return result;
    });
  }

  onMessage(
    listener: (message: ReceivedMessage) => void | Promise<void>,
  ): () => void {
    this.#messageListeners.add(listener);
    return () => {
      this.#messageListeners.delete(listener);
    };
  }

  onEvent(
    listener: (event: FieldLinkEvent) => void | Promise<void>,
  ): () => void {
    this.#eventListeners.add(listener);
    return () => {
      this.#eventListeners.delete(listener);
    };
  }

  async close(): Promise<void> {
    if (this.#closed) {
      await Promise.all([this.#exit, this.#readerDone]);
      return;
    }
    this.#closed = true;
    const closeErrors: Error[] = [];
    try {
      await this.#request({ type: "close" }, undefined, this.#exitTimeoutMs);
    } catch (error: unknown) {
      closeErrors.push(asError(error));
    }
    this.#child.stdin.end();
    const reaped = Promise.all([this.#exit, this.#readerDone]);
    let exitResult: AdapterExit | undefined;
    try {
      [exitResult] = await withTimeout(
        reaped,
        this.#exitTimeoutMs,
        "adapter process exit and stdout drain",
      );
    } catch (error: unknown) {
      closeErrors.push(asError(error));
      try {
        [exitResult] = await terminateAndReapAdapter(
          this.#child,
          reaped,
          this.#exitTimeoutMs,
        );
      } catch (cleanupError: unknown) {
        closeErrors.push(asError(cleanupError));
      }
    }
    if (
      exitResult !== undefined &&
      (exitResult.code !== 0 || exitResult.signal !== null)
    ) {
      closeErrors.push(
        new Error(
          `Adapter exited (${exitResult.signal ?? `code ${exitResult.code ?? "unknown"}`})`,
        ),
      );
    }
    const [closeError] = closeErrors;
    if (closeError !== undefined && closeErrors.length === 1) {
      throw closeError;
    }
    if (closeError !== undefined) {
      throw new AggregateError(closeErrors, "Could not close adapter process");
    }
  }

  #request(
    operation:
      | Omit<Extract<AdapterRequest, { type: "send" }>, "id">
      | Omit<Extract<AdapterRequest, { type: "activate" }>, "id">
      | Omit<Extract<AdapterRequest, { type: "close" }>, "id">
      | Omit<Extract<AdapterRequest, { type: "abort" }>, "id">,
    signal: AbortSignal | undefined,
    timeoutMs = this.#requestTimeoutMs,
  ): Promise<SendResult | undefined> {
    if (this.#failure !== undefined) {
      return Promise.reject(this.#failure);
    }
    if (signal?.aborted === true) {
      return Promise.reject(abortError(signal));
    }
    const id = this.#nextRequestId++;
    const request = { ...operation, id } as AdapterRequest;
    return new Promise<SendResult | undefined>((resolve, reject) => {
      const abort = (): void => {
        void this.#write({
          type: "abort",
          id: this.#nextRequestId++,
          targetId: id,
        }).catch(() => undefined);
      };
      const cleanup = (): void => {
        signal?.removeEventListener("abort", abort);
      };
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        cleanup();
        const error = new Error(
          `Adapter request ${id} timed out after ${timeoutMs} ms`,
        );
        this.#fail(error);
        reject(error);
      }, timeoutMs);
      const pending: PendingRequest = { resolve, reject, timer, cleanup };
      this.#pending.set(id, pending);
      signal?.addEventListener("abort", abort, { once: true });
      void this.#write(request).catch((error: unknown) => {
        cleanup();
        this.#pending.delete(id);
        clearTimeout(timer);
        const failure = asError(error);
        this.#fail(failure);
        reject(failure);
      });
    });
  }

  #read(
    iterator: AsyncIterator<string>,
    lines: ReturnType<typeof createInterface>,
    options: StartAdapterProcessOptions,
  ): Promise<void> {
    return (async () => {
      try {
        for (;;) {
          const next = await iterator.next();
          if (next.done) {
            if (
              !this.#closed &&
              this.#child.exitCode === null &&
              this.#child.signalCode === null
            ) {
              // Stdio EOF can arrive just before a real child exit is reported.
              const exitState = await Promise.race([
                this.#exit.then(() => "exited" as const),
                new Promise<"open">((resolve) => {
                  setTimeout(() => {
                    resolve("open");
                  }, STDOUT_EXIT_GRACE_MS);
                }),
              ]);
              if (exitState === "open") {
                this.#fail(new Error("Adapter stdout ended unexpectedly"));
                await terminateAndReapAdapter(
                  this.#child,
                  this.#exit,
                  this.#exitTimeoutMs,
                );
              }
            }
            return;
          }
          const message = parseAdapterMessage(next.value);
          switch (message.type) {
            case "response": {
              const pending = this.#pending.get(message.id);
              if (pending === undefined) {
                break;
              }
              this.#pending.delete(message.id);
              clearTimeout(pending.timer);
              pending.cleanup();
              if (message.ok) {
                pending.resolve(message.result);
              } else {
                pending.reject(new Error(message.error));
              }
              break;
            }
            case "message": {
              const received: ReceivedMessage = {
                ...message.message,
                receivedAt: new Date(message.message.receivedAt),
              };
              for (const listener of this.#messageListeners) {
                void Promise.resolve()
                  .then(() => listener(received))
                  .catch(async (error: unknown) => {
                    await options.onListenerError?.(asError(error));
                  });
              }
              break;
            }
            case "event":
              for (const listener of this.#eventListeners) {
                void Promise.resolve()
                  .then(() => listener(message.event))
                  .catch(async (error: unknown) => {
                    await options.onListenerError?.(asError(error));
                  });
              }
              break;
            case "inbox-message":
              await options.onInboxMessage?.(message.message);
              break;
            case "listener-error":
              await options.onListenerError?.(new Error(message.error));
              break;
            case "ready":
              throw new Error("Adapter sent ready more than once");
          }
        }
      } catch (error: unknown) {
        this.#fail(asError(error));
      } finally {
        lines.close();
      }
    })();
  }

  #write(request: AdapterRequest): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.#child.stdin.write(`${stringifyWire(request)}\n`, (error) => {
        if (error) {
          reject(asError(error));
        } else {
          resolve();
        }
      });
    });
  }

  #fail(error: Error): void {
    this.#failure ??= error;
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.cleanup();
      pending.reject(this.#failure);
    }
    this.#pending.clear();
  }
}

class WireWriter {
  readonly #output: Writable;
  #tail: Promise<void> = Promise.resolve();

  constructor(output: Writable) {
    this.#output = output;
  }

  write(message: AdapterMessage): Promise<void> {
    const write = this.#tail.then(
      () =>
        new Promise<void>((resolve, reject) => {
          this.#output.write(`${stringifyWire(message)}\n`, (error) => {
            if (error) {
              reject(asError(error));
            } else {
              resolve();
            }
          });
        }),
    );
    this.#tail = write.catch(() => undefined);
    return write;
  }

  flush(): Promise<void> {
    return this.#tail;
  }
}

function parseAdapterRequest(line: string): AdapterRequest {
  const value = parseWire(line);
  if (!isRecord(value) || !isRequestId(value.id)) {
    throw new Error("Malformed adapter request");
  }
  if (value.type === "close") {
    return { type: "close", id: value.id };
  }
  if (value.type === "activate") {
    return { type: "activate", id: value.id };
  }
  if (value.type === "abort" && isRequestId(value.targetId)) {
    return { type: "abort", id: value.id, targetId: value.targetId };
  }
  if (
    value.type === "send" &&
    typeof value.destination === "string" &&
    isRecord(value.message)
  ) {
    const priority = value.priority;
    const retryStrategy = value.retryStrategy;
    if (
      priority !== undefined &&
      priority !== "high" &&
      priority !== "normal" &&
      priority !== "bulk"
    ) {
      throw new Error("Malformed adapter priority");
    }
    if (retryStrategy !== undefined && retryStrategy !== "selective-window") {
      throw new Error("Malformed adapter retry strategy");
    }
    return {
      type: "send",
      id: value.id,
      message: value.message as SupportedMessage,
      destination: value.destination,
      ...(priority === undefined ? {} : { priority }),
      ...(retryStrategy === undefined ? {} : { retryStrategy }),
    };
  }
  throw new Error("Unknown adapter request");
}

function parseAdapterMessage(line: string): AdapterMessage {
  const value = parseWire(line);
  if (!isAdapterMessage(value)) {
    throw new Error("Malformed adapter message");
  }
  return value;
}

function isAdapterMessage(value: unknown): value is AdapterMessage {
  if (!isRecord(value)) {
    return false;
  }
  switch (value.type) {
    case "ready":
      return isAdapterReady(value);
    case "message":
      return isWireReceivedMessage(value.message);
    case "event":
      return (
        isRecord(value.event) &&
        typeof value.event.type === "string" &&
        typeof value.event.at === "string"
      );
    case "inbox-message":
      return isInboxMessage(value.message);
    case "listener-error":
      return typeof value.error === "string";
    case "response":
      if (!isRequestId(value.id) || typeof value.ok !== "boolean") {
        return false;
      }
      return value.ok
        ? value.result === undefined || isSendResult(value.result)
        : typeof value.error === "string";
    default:
      return false;
  }
}

function isAdapterReady(value: unknown): value is {
  readonly type: "ready";
} & AdapterReady {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isRequestId(value.processId) &&
    isNodeId(value.nodeId) &&
    isSafeIdentity(value.identity) &&
    isSafeChannel(value.channel) &&
    Array.isArray(value.supportedMessages) &&
    value.supportedMessages.every(isSupportedMessageMetadata) &&
    Array.isArray(value.retryStrategies) &&
    value.retryStrategies.every(isRetryStrategyMetadata) &&
    isRecord(value.delivery) &&
    value.delivery.meshCoreDataType === FIELDLINK_DATA_TYPE &&
    value.delivery.meshCoreMode === "flood" &&
    value.delivery.maximumChannelDatagramBytes === 163
  );
}

function isWireReceivedMessage(value: unknown): value is WireReceivedMessage {
  return (
    isRecord(value) &&
    messageRegistry.some((definition) => definition.validate(value.message)) &&
    isNodeId(value.source) &&
    isNodeId(value.destination) &&
    typeof value.logicalId === "string" &&
    (value.delivery === "complete" || value.delivery === "transfer") &&
    typeof value.receivedAt === "string" &&
    (value.snrDb === undefined || typeof value.snrDb === "number")
  );
}

function isSendResult(value: unknown): value is SendResult {
  return (
    isRecord(value) &&
    typeof value.logicalId === "string" &&
    isUint16(value.messageType) &&
    typeof value.messageName === "string" &&
    isNodeId(value.destination) &&
    isPriority(value.priority) &&
    (value.delivery === "complete" || value.delivery === "transfer") &&
    isNonnegativeInteger(value.encodedBytes) &&
    isNonnegativeInteger(value.fragments) &&
    (value.retryStrategy === undefined ||
      value.retryStrategy === "selective-window") &&
    isNonnegativeInteger(value.retransmissions) &&
    isNonnegativeInteger(value.receipts) &&
    typeof value.durationMs === "number"
  );
}

function isSafeIdentity(value: unknown): value is SafeRadioIdentity {
  return (
    isRecord(value) &&
    isNodeId(value.nodeId) &&
    typeof value.fingerprint === "string" &&
    typeof value.name === "string" &&
    typeof value.model === "string" &&
    typeof value.firmwareVersion === "string" &&
    typeof value.firmwareBuildDate === "string" &&
    isNonnegativeInteger(value.firmwareProtocolCode) &&
    isNonnegativeInteger(value.clientProtocolVersion) &&
    isRecord(value.radio) &&
    [
      value.radio.frequency,
      value.radio.bandwidth,
      value.radio.spreadingFactor,
      value.radio.codingRate,
      value.radio.transmitPower,
      value.radio.maximumTransmitPower,
    ].every((number) => typeof number === "number")
  );
}

function isSafeChannel(value: unknown): value is SafeChannelConfiguration {
  return (
    isRecord(value) &&
    typeof value.index === "number" &&
    Number.isInteger(value.index) &&
    value.index >= 0 &&
    value.index <= 0xff &&
    typeof value.name === "string" &&
    typeof value.configured === "boolean" &&
    typeof value.keyFingerprint === "string"
  );
}

function isSupportedMessageMetadata(value: unknown): value is {
  readonly id: number;
  readonly name: string;
  readonly defaultPriority: Priority;
} {
  return (
    isRecord(value) &&
    isUint16(value.id) &&
    typeof value.name === "string" &&
    isPriority(value.defaultPriority)
  );
}

function isRetryStrategyMetadata(value: unknown): value is {
  readonly id: number;
  readonly name: RetryStrategyName;
} {
  return (
    isRecord(value) &&
    isNonnegativeInteger(value.id) &&
    value.id <= 0xff &&
    value.name === "selective-window"
  );
}

function isInboxMessage(value: unknown): value is InboxMessage {
  return (
    isRecord(value) &&
    (("channelData" in value && isRecord(value.channelData)) ||
      ("channelMessage" in value && isRecord(value.channelMessage)) ||
      ("contactMessage" in value && isRecord(value.contactMessage)))
  );
}

function isPriority(value: unknown): value is Priority {
  return value === "high" || value === "normal" || value === "bulk";
}

function isNodeId(value: unknown): value is NodeId {
  return typeof value === "string" && /^[0-9a-f]{16}$/.test(value);
}

function isUint16(value: unknown): value is number {
  return isNonnegativeInteger(value) && value <= 0xffff;
}

function isNonnegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function stringifyWire(value: unknown): string {
  return JSON.stringify(value, (_key, nested: unknown) =>
    nested instanceof Uint8Array
      ? { [BYTES_MARKER]: Buffer.from(nested).toString("base64") }
      : nested,
  );
}

function parseWire(line: string): unknown {
  return JSON.parse(line, (_key, nested: unknown) => {
    if (
      isRecord(nested) &&
      Object.keys(nested).length === 1 &&
      typeof nested[BYTES_MARKER] === "string"
    ) {
      return Uint8Array.from(Buffer.from(nested[BYTES_MARKER], "base64"));
    }
    return nested;
  }) as unknown;
}

function defaultAdapterProgram(
  options: StartAdapterProcessOptions,
): AdapterProgram {
  const current = fileURLToPath(import.meta.url);
  const extension = extname(current);
  const cli = fileURLToPath(new URL(`./cli${extension}`, import.meta.url));
  return {
    executable: process.execPath,
    arguments: [
      ...filteredExecArguments(process.execArgv),
      cli,
      "adapter",
      "--radio",
      options.path,
      "--channel",
      String(options.channel),
      "--evidence-managed-by-parent",
      "--allow-inbox-drain",
    ],
  };
}

export function filteredExecArguments(
  arguments_: readonly string[],
): readonly string[] {
  const filtered: string[] = [];
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === undefined) {
      continue;
    }
    const [option] = argument.split("=", 1);
    if (
      option === "--inspect" ||
      option === "--inspect-brk" ||
      option === "--watch"
    ) {
      continue;
    }
    if (option !== undefined && CONTROLLER_OPTIONS_WITH_VALUES.has(option)) {
      if (!argument.includes("=")) {
        index += 1;
      }
      continue;
    }
    filtered.push(argument);
  }
  return filtered;
}

function withTimeout<Result>(
  promise: Promise<Result>,
  timeoutMs: number,
  description: string,
): Promise<Result> {
  return new Promise<Result>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(
        new Error(`Timed out waiting for ${description} after ${timeoutMs} ms`),
      );
    }, timeoutMs);
    void promise.then(
      (result) => {
        clearTimeout(timer);
        resolve(result);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(asError(error));
      },
    );
  });
}

async function terminateAndReapAdapter<Result>(
  child: ChildProcessWithoutNullStreams,
  reaped: Promise<Result>,
  timeoutMs: number,
): Promise<Result> {
  child.kill("SIGTERM");
  try {
    return await withTimeout(reaped, timeoutMs, "adapter process termination");
  } catch (terminationError: unknown) {
    child.kill("SIGKILL");
    try {
      return await withTimeout(reaped, timeoutMs, "adapter process kill");
    } catch (killError: unknown) {
      throw new AggregateError(
        [asError(terminationError), asError(killError)],
        "Could not reap adapter process",
      );
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isRequestId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new Error("Operation aborted");
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
