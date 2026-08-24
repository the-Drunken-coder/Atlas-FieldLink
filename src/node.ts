import { createHash, randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";

import {
  COMPLETE_MESSAGE_BODY_BYTES,
  decodeFrame,
  encodeFrame,
  FIELDLINK_MAX_MESSAGE_BYTES,
  FrameKind,
  TRANSFER_FRAGMENT_BYTES,
  type FieldLinkFrame,
} from "./frame.js";
import {
  definitionForMessage,
  definitionForType,
  type SupportedMessage,
} from "./messages/index.js";
import { parseNodeId, type NodeId, type Priority } from "./node-types.js";
import {
  retryStrategies,
  retryStrategyById,
  retryStrategyByName,
  type RetryStrategyName,
} from "./retry-strategies/index.js";
import {
  TransferRejectedError,
  type RetryResult,
  type RetryStrategy,
  type TransferReceiverState,
  type TransferSenderSession,
} from "./retry.js";

const MAX_PENDING_SENDS = 64;
const MAX_INBOUND_TRANSFERS = 4;
const INBOUND_TRANSFER_IDLE_MS = 2 * 60 * 1000;
const DEFAULT_RETRY_TIMEOUT_MS = 30_000;
const QUEUE_POLL_MS = 25;

export interface TransportDatagram {
  readonly bytes: Uint8Array;
  readonly snrDb?: number;
  readonly pathLength?: number;
}

export interface FieldLinkTransport {
  send(bytes: Uint8Array): Promise<void>;
  getQueueLength(): Promise<number>;
  onDatagram(
    listener: (datagram: TransportDatagram) => void | Promise<void>,
  ): () => void;
  close(): Promise<void>;
}

export type FieldLinkEvent = Readonly<
  { type: string; at: string } & Record<string, unknown>
>;

export interface ReceivedMessage {
  readonly message: SupportedMessage;
  readonly source: NodeId;
  readonly destination: NodeId;
  readonly logicalId: string;
  readonly delivery: "complete" | "transfer";
  readonly receivedAt: Date;
  readonly snrDb?: number;
}

export interface SendOptions {
  readonly destination: NodeId | string;
  readonly priority?: Priority;
  readonly retryStrategy?: RetryStrategyName;
  readonly signal?: AbortSignal;
}

export interface SendResult {
  readonly logicalId: string;
  readonly messageType: number;
  readonly messageName: string;
  readonly destination: NodeId;
  readonly priority: Priority;
  readonly delivery: "complete" | "transfer";
  readonly encodedBytes: number;
  readonly fragments: number;
  readonly retryStrategy?: RetryStrategyName;
  readonly retransmissions: number;
  readonly receipts: number;
  readonly durationMs: number;
}

export interface FieldLinkNodeOptions {
  readonly nodeId: NodeId | string;
  readonly transport: FieldLinkTransport;
  readonly retryTimeoutMs?: number;
  readonly inboundTransferIdleMs?: number;
  readonly now?: () => number;
}

interface InboundTransferBase {
  readonly source: NodeId;
  readonly messageType: number;
  readonly totalLength: number;
  readonly fragmentCount: number;
  readonly fragmentSize: number;
  readonly digest: Uint8Array;
  readonly retryStrategy: number;
  lastActivity: number;
}

interface ActiveInboundTransfer extends InboundTransferBase {
  readonly completed: false;
  readonly receiver: TransferReceiverState;
  readonly bytes: Uint8Array;
  readonly received: Uint8Array;
  receivedCount: number;
  snrDb?: number;
}

interface CompletedInboundTransfer extends InboundTransferBase {
  readonly completed: true;
}

type InboundTransfer = ActiveInboundTransfer | CompletedInboundTransfer;

interface ScheduledFrame {
  readonly bytes: Uint8Array;
  readonly priority: Priority;
  readonly signal?: AbortSignal;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
}

type WithoutTransmissionId<Frame> = Frame extends FieldLinkFrame
  ? Omit<Frame, "transmissionId">
  : never;
type OutboundFieldLinkFrame = WithoutTransmissionId<FieldLinkFrame>;

export class FieldLinkNode {
  readonly nodeId: NodeId;
  readonly #transport: FieldLinkTransport;
  readonly #scheduler: FrameScheduler;
  readonly #messageListeners = new Set<
    (message: ReceivedMessage) => void | Promise<void>
  >();
  readonly #eventListeners = new Set<
    (event: FieldLinkEvent) => void | Promise<void>
  >();
  readonly #inbound = new Map<string, InboundTransfer>();
  readonly #outbound = new Map<string, OutboundSignals>();
  readonly #activeReceives = new Set<Promise<void>>();
  readonly #retryTimeoutMs: number;
  readonly #inboundTransferIdleMs: number;
  readonly #now: () => number;
  readonly #unsubscribeTransport: () => void;
  readonly #cleanupTimer: ReturnType<typeof setInterval>;
  #transferTail: Promise<void> = Promise.resolve();
  #nextTransmissionId = randomUint16();
  #pendingSends = 0;
  #closed = false;
  #closePromise: Promise<void> | undefined;

  constructor(options: FieldLinkNodeOptions) {
    this.nodeId = parseNodeId(options.nodeId);
    this.#transport = options.transport;
    this.#retryTimeoutMs = options.retryTimeoutMs ?? DEFAULT_RETRY_TIMEOUT_MS;
    this.#inboundTransferIdleMs =
      options.inboundTransferIdleMs ?? INBOUND_TRANSFER_IDLE_MS;
    this.#now = options.now ?? Date.now;
    this.#scheduler = new FrameScheduler(options.transport, (event) => {
      this.#emit(event);
    });
    this.#unsubscribeTransport = this.#transport.onDatagram((datagram) => {
      const receive = this.#receive(datagram).finally(() => {
        this.#activeReceives.delete(receive);
      });
      void receive.catch((error: unknown) => {
        this.#protocolError(
          `Inbound handling failed: ${asError(error).message}`,
        );
      });
      this.#activeReceives.add(receive);
      return receive;
    });
    this.#cleanupTimer = setInterval(
      () => {
        this.#cleanupInactiveTransfers();
      },
      Math.min(30_000, this.#inboundTransferIdleMs),
    );
    this.#cleanupTimer.unref();
  }

  async send(
    message: SupportedMessage,
    options: SendOptions,
  ): Promise<SendResult> {
    this.#throwIfClosed();
    throwIfAborted(options.signal);
    if (this.#pendingSends >= MAX_PENDING_SENDS) {
      throw new Error(
        `FieldLink has reached its ${MAX_PENDING_SENDS}-send limit`,
      );
    }
    this.#pendingSends += 1;
    try {
      const definition = definitionForMessage(message);
      const body = definition.encode(message);
      if (body.length > FIELDLINK_MAX_MESSAGE_BYTES) {
        throw new RangeError(
          `Encoded message is ${body.length} bytes; maximum is ${FIELDLINK_MAX_MESSAGE_BYTES}`,
        );
      }
      const destination = parseNodeId(options.destination);
      const priority = options.priority ?? definition.defaultPriority;
      const logicalId = randomLogicalId();
      const startedAt = performance.now();

      if (body.length <= COMPLETE_MESSAGE_BODY_BYTES) {
        await this.#submit(
          {
            kind: FrameKind.complete,
            source: this.nodeId,
            destination,
            logicalId,
            messageType: definition.id,
            body,
          },
          priority,
          options.signal,
        );
        return {
          logicalId: logicalIdHex(logicalId),
          messageType: definition.id,
          messageName: definition.name,
          destination,
          priority,
          delivery: "complete",
          encodedBytes: body.length,
          fragments: 1,
          retransmissions: 0,
          receipts: 0,
          durationMs: performance.now() - startedAt,
        };
      }

      const retryStrategyName =
        options.retryStrategy ?? retryStrategies[0].name;
      const strategy = retryStrategyByName(retryStrategyName);
      if (strategy === undefined) {
        throw new Error(`Unsupported retry strategy ${retryStrategyName}`);
      }
      const retry = await this.#withOutboundTransfer(() =>
        this.#sendTransfer({
          body,
          destination,
          logicalId,
          messageType: definition.id,
          priority,
          strategy,
          ...(options.signal === undefined ? {} : { signal: options.signal }),
        }),
      );
      return {
        logicalId: logicalIdHex(logicalId),
        messageType: definition.id,
        messageName: definition.name,
        destination,
        priority,
        delivery: "transfer",
        encodedBytes: body.length,
        fragments: Math.ceil(body.length / TRANSFER_FRAGMENT_BYTES),
        retryStrategy: strategy.name,
        retransmissions: retry.retransmissions,
        receipts: retry.receipts,
        durationMs: performance.now() - startedAt,
      };
    } finally {
      this.#pendingSends -= 1;
    }
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

  #cleanupInactiveTransfers(): void {
    const cutoff = this.#now() - this.#inboundTransferIdleMs;
    for (const [id, transfer] of this.#inbound) {
      if (transfer.lastActivity >= cutoff) {
        continue;
      }
      this.#inbound.delete(id);
      this.#emit({
        type: "transfer-expired",
        at: new Date().toISOString(),
        logicalId: id,
        source: transfer.source,
      });
    }
  }

  close(): Promise<void> {
    this.#closePromise ??= this.#close();
    return this.#closePromise;
  }

  async #close(): Promise<void> {
    this.#closed = true;
    clearInterval(this.#cleanupTimer);
    this.#unsubscribeTransport();
    const errors: Error[] = [];
    try {
      await this.#scheduler.close();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    await Promise.allSettled(this.#activeReceives);
    for (const signals of this.#outbound.values()) {
      signals.reject(new Error("FieldLink node closed"));
    }
    this.#outbound.clear();
    this.#inbound.clear();
    try {
      await this.#transport.close();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, "Could not close FieldLink node");
    }
  }

  async #sendTransfer(options: {
    readonly body: Uint8Array;
    readonly destination: NodeId;
    readonly logicalId: bigint;
    readonly messageType: number;
    readonly priority: Priority;
    readonly strategy: RetryStrategy;
    readonly signal?: AbortSignal;
  }): Promise<RetryResult> {
    this.#throwIfClosed();
    const fragmentCount = Math.ceil(
      options.body.length / TRANSFER_FRAGMENT_BYTES,
    );
    const digest = createHash("sha256").update(options.body).digest();
    const key = logicalIdHex(options.logicalId);
    const signals = new OutboundSignals();
    this.#outbound.set(key, signals);
    const base = {
      source: this.nodeId,
      destination: options.destination,
      logicalId: options.logicalId,
    } as const;
    const timeout = (requested: number): number =>
      Math.min(requested, this.#retryTimeoutMs);
    const session: TransferSenderSession = {
      fragmentCount,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
      open: async (timeoutMs) => {
        await this.#submit(
          {
            ...base,
            kind: FrameKind.transferStart,
            messageType: options.messageType,
            totalLength: options.body.length,
            fragmentCount,
            fragmentSize: TRANSFER_FRAGMENT_BYTES,
            digest,
            retryStrategy: options.strategy.id,
          },
          options.priority,
          options.signal,
        );
        await signals.waitForReady(timeout(timeoutMs), options.signal);
      },
      sendFragment: async (index, retransmission) => {
        const start = index * TRANSFER_FRAGMENT_BYTES;
        await this.#submit(
          {
            ...base,
            kind: FrameKind.fragment,
            fragmentIndex: index,
            body: options.body.slice(start, start + TRANSFER_FRAGMENT_BYTES),
          },
          options.priority,
          options.signal,
        );
        this.#emit({
          type: retransmission ? "fragment-retransmitted" : "fragment-sent",
          at: new Date().toISOString(),
          logicalId: key,
          fragmentIndex: index,
        });
      },
      requestReceipt: async (windowStart, windowCount, timeoutMs) => {
        const sequence = signals.receiptSequence(windowStart);
        await this.#submit(
          {
            ...base,
            kind: FrameKind.receiptRequest,
            windowStart,
            windowCount,
          },
          options.priority,
          options.signal,
        );
        return signals.waitForReceipt(
          windowStart,
          sequence,
          timeout(timeoutMs),
          options.signal,
        );
      },
      waitForCompletion: (timeoutMs) =>
        signals.waitForCompletion(timeout(timeoutMs), options.signal),
    };

    this.#emit({
      type: "transfer-started",
      at: new Date().toISOString(),
      logicalId: key,
      destination: options.destination,
      encodedBytes: options.body.length,
      fragmentCount,
      retryStrategy: options.strategy.name,
    });
    try {
      const result = await options.strategy.createSender().run(session);
      this.#emit({
        type: "transfer-completed",
        at: new Date().toISOString(),
        logicalId: key,
        ...result,
      });
      return result;
    } catch (error: unknown) {
      const failure = asError(error);
      this.#emit({
        type: "transfer-failed",
        at: new Date().toISOString(),
        logicalId: key,
        error: failure.message,
      });
      if (!this.#closed) {
        await this.#submit(
          { ...base, kind: FrameKind.cancellation, code: 1 },
          "high",
          undefined,
        ).catch(() => undefined);
      }
      throw failure;
    } finally {
      this.#outbound.delete(key);
    }
  }

  async #receive(datagram: TransportDatagram): Promise<void> {
    let frame: FieldLinkFrame;
    try {
      frame = decodeFrame(datagram.bytes);
    } catch (error: unknown) {
      this.#protocolError(asError(error).message);
      return;
    }
    if (frame.destination !== this.nodeId) {
      return;
    }
    this.#emit({
      type: "frame-received",
      at: new Date().toISOString(),
      frameKind: FrameKind[frame.kind],
      logicalId: logicalIdHex(frame.logicalId),
      source: frame.source,
      bytes: datagram.bytes.length,
      ...(datagram.snrDb === undefined ? {} : { snrDb: datagram.snrDb }),
    });

    switch (frame.kind) {
      case FrameKind.complete:
        this.#receiveComplete(frame, datagram.snrDb);
        return;
      case FrameKind.transferStart:
        await this.#receiveTransferStart(frame, datagram.snrDb);
        return;
      case FrameKind.fragment:
        await this.#receiveFragment(frame, datagram.snrDb);
        return;
      case FrameKind.receiptRequest:
        await this.#receiveReceiptRequest(frame);
        return;
      case FrameKind.transferReady:
      case FrameKind.receipt:
      case FrameKind.completion:
      case FrameKind.rejection:
        this.#receiveOutboundControl(frame);
        return;
      case FrameKind.cancellation:
        this.#inbound.delete(logicalIdHex(frame.logicalId));
        this.#emit({
          type: "transfer-cancelled",
          at: new Date().toISOString(),
          logicalId: logicalIdHex(frame.logicalId),
          source: frame.source,
        });
        return;
    }
  }

  #receiveComplete(
    frame: Extract<FieldLinkFrame, { kind: FrameKind.complete }>,
    snrDb: number | undefined,
  ): void {
    const definition = definitionForType(frame.messageType);
    if (definition === undefined) {
      this.#protocolError(`Unknown message type ${frame.messageType}`);
      return;
    }
    let message: SupportedMessage;
    try {
      message = definition.decode(frame.body);
    } catch (error: unknown) {
      this.#protocolError(asError(error).message);
      return;
    }
    this.#deliverMessage(
      message,
      frame.source,
      frame.destination,
      frame.logicalId,
      "complete",
      snrDb,
    );
  }

  async #receiveTransferStart(
    frame: Extract<FieldLinkFrame, { kind: FrameKind.transferStart }>,
    snrDb: number | undefined,
  ): Promise<void> {
    const key = logicalIdHex(frame.logicalId);
    const existing = this.#inbound.get(key);
    if (existing !== undefined) {
      existing.lastActivity = this.#now();
      if (
        existing.source !== frame.source ||
        existing.messageType !== frame.messageType ||
        existing.totalLength !== frame.totalLength ||
        existing.fragmentCount !== frame.fragmentCount ||
        existing.fragmentSize !== frame.fragmentSize ||
        existing.retryStrategy !== frame.retryStrategy ||
        !Buffer.from(existing.digest).equals(frame.digest)
      ) {
        await this.#reject(frame, 6, "Conflicting transfer start");
        return;
      }
      await this.#submit(
        responseFrame(frame, FrameKind.transferReady),
        "high",
        undefined,
      );
      return;
    }
    if (definitionForType(frame.messageType) === undefined) {
      await this.#reject(frame, 1, `Unknown message type ${frame.messageType}`);
      return;
    }
    const strategy = retryStrategyById(frame.retryStrategy);
    if (strategy === undefined) {
      await this.#reject(
        frame,
        2,
        `Unsupported retry strategy ${frame.retryStrategy}`,
      );
      return;
    }
    const expectedFragments = Math.ceil(
      frame.totalLength / TRANSFER_FRAGMENT_BYTES,
    );
    if (
      frame.totalLength <= COMPLETE_MESSAGE_BODY_BYTES ||
      frame.totalLength > FIELDLINK_MAX_MESSAGE_BYTES ||
      frame.fragmentSize !== TRANSFER_FRAGMENT_BYTES ||
      frame.fragmentCount !== expectedFragments ||
      frame.fragmentCount === 0
    ) {
      await this.#reject(frame, 3, "Invalid transfer bounds");
      return;
    }
    const activeInboundTransfers = [...this.#inbound.values()].filter(
      (transfer) => !transfer.completed,
    ).length;
    if (activeInboundTransfers >= MAX_INBOUND_TRANSFERS) {
      await this.#reject(frame, 4, "Inbound transfer limit reached");
      return;
    }
    this.#inbound.set(key, {
      source: frame.source,
      messageType: frame.messageType,
      totalLength: frame.totalLength,
      fragmentCount: frame.fragmentCount,
      fragmentSize: frame.fragmentSize,
      digest: frame.digest,
      receiver: strategy.createReceiver(),
      bytes: new Uint8Array(frame.totalLength),
      received: new Uint8Array(frame.fragmentCount),
      retryStrategy: frame.retryStrategy,
      receivedCount: 0,
      lastActivity: this.#now(),
      completed: false as const,
      ...(snrDb === undefined ? {} : { snrDb }),
    });
    this.#emit({
      type: "transfer-accepted",
      at: new Date().toISOString(),
      logicalId: key,
      source: frame.source,
      messageType: frame.messageType,
      fragmentCount: frame.fragmentCount,
      retryStrategy: strategy.name,
    });
    await this.#submit(
      responseFrame(frame, FrameKind.transferReady),
      "high",
      undefined,
    );
  }

  async #receiveFragment(
    frame: Extract<FieldLinkFrame, { kind: FrameKind.fragment }>,
    snrDb: number | undefined,
  ): Promise<void> {
    const key = logicalIdHex(frame.logicalId);
    const transfer = this.#inbound.get(key);
    if (transfer === undefined || transfer.source !== frame.source) {
      this.#protocolError(
        `Fragment ${frame.fragmentIndex} has no active start`,
        {
          logicalId: key,
        },
      );
      return;
    }
    if (transfer.completed) {
      transfer.lastActivity = this.#now();
      return;
    }
    if (frame.fragmentIndex >= transfer.fragmentCount) {
      this.#protocolError(
        `Fragment index ${frame.fragmentIndex} is out of range`,
        {
          logicalId: key,
        },
      );
      return;
    }
    const offset = frame.fragmentIndex * transfer.fragmentSize;
    const expectedLength = Math.min(
      transfer.fragmentSize,
      transfer.totalLength - offset,
    );
    if (frame.body.length !== expectedLength) {
      this.#protocolError(
        `Fragment ${frame.fragmentIndex} has ${frame.body.length} bytes; expected ${expectedLength}`,
        { logicalId: key },
      );
      return;
    }
    transfer.lastActivity = this.#now();
    if (snrDb !== undefined) {
      transfer.snrDb = snrDb;
    }
    if (transfer.received[frame.fragmentIndex] === 0) {
      transfer.bytes.set(frame.body, offset);
      transfer.received[frame.fragmentIndex] = 1;
      transfer.receivedCount += 1;
      this.#emit({
        type: "fragment-received",
        at: new Date().toISOString(),
        logicalId: key,
        fragmentIndex: frame.fragmentIndex,
      });
    } else {
      const existing = transfer.bytes.slice(offset, offset + expectedLength);
      if (!Buffer.from(existing).equals(frame.body)) {
        this.#inbound.delete(key);
        await this.#reject(frame, 6, "Duplicate fragment bytes differ");
      }
      return;
    }
    if (transfer.receivedCount !== transfer.fragmentCount) {
      return;
    }

    const digest = createHash("sha256").update(transfer.bytes).digest();
    if (!Buffer.from(digest).equals(transfer.digest)) {
      this.#inbound.delete(key);
      await this.#reject(frame, 5, "Transfer digest does not match");
      return;
    }
    const definition = definitionForType(transfer.messageType);
    if (definition === undefined) {
      this.#inbound.delete(key);
      await this.#reject(frame, 1, "Message type disappeared from registry");
      return;
    }
    let message: SupportedMessage;
    try {
      message = definition.decode(transfer.bytes);
    } catch (error: unknown) {
      this.#inbound.delete(key);
      await this.#reject(frame, 7, asError(error).message);
      return;
    }
    this.#inbound.set(key, {
      completed: true,
      source: transfer.source,
      messageType: transfer.messageType,
      totalLength: transfer.totalLength,
      fragmentCount: transfer.fragmentCount,
      fragmentSize: transfer.fragmentSize,
      digest: transfer.digest,
      retryStrategy: transfer.retryStrategy,
      lastActivity: transfer.lastActivity,
    });
    this.#deliverMessage(
      message,
      frame.source,
      frame.destination,
      frame.logicalId,
      "transfer",
      transfer.snrDb,
    );
    await this.#submit(
      responseFrame(frame, FrameKind.completion),
      "high",
      undefined,
    );
  }

  async #receiveReceiptRequest(
    frame: Extract<FieldLinkFrame, { kind: FrameKind.receiptRequest }>,
  ): Promise<void> {
    const key = logicalIdHex(frame.logicalId);
    const transfer = this.#inbound.get(key);
    if (transfer === undefined || transfer.source !== frame.source) {
      this.#protocolError("Receipt request has no active transfer", {
        logicalId: key,
      });
      return;
    }
    if (
      frame.windowStart >= transfer.fragmentCount ||
      frame.windowStart + frame.windowCount > transfer.fragmentCount
    ) {
      this.#protocolError("Receipt request window is out of range", {
        logicalId: key,
      });
      return;
    }
    transfer.lastActivity = this.#now();
    const bitmap = transfer.completed
      ? (1 << frame.windowCount) - 1
      : transfer.receiver.receipt(
          frame.windowStart,
          frame.windowCount,
          (index) => transfer.received[index] === 1,
        );
    await this.#submit(
      {
        ...responseBase(frame),
        kind: FrameKind.receipt,
        windowStart: frame.windowStart,
        windowCount: frame.windowCount,
        bitmap,
      },
      "high",
      undefined,
    );
    this.#emit({
      type: "receipt-sent",
      at: new Date().toISOString(),
      logicalId: key,
      windowStart: frame.windowStart,
      windowCount: frame.windowCount,
      bitmap,
    });
    if (transfer.completed) {
      await this.#submit(
        responseFrame(frame, FrameKind.completion),
        "high",
        undefined,
      );
    }
  }

  #receiveOutboundControl(
    frame: Extract<
      FieldLinkFrame,
      {
        kind:
          | FrameKind.transferReady
          | FrameKind.receipt
          | FrameKind.completion
          | FrameKind.rejection;
      }
    >,
  ): void {
    const key = logicalIdHex(frame.logicalId);
    const signals = this.#outbound.get(key);
    if (signals === undefined || frame.source === this.nodeId) {
      return;
    }
    switch (frame.kind) {
      case FrameKind.transferReady:
        signals.ready();
        break;
      case FrameKind.receipt:
        signals.receipt(frame.windowStart, frame.bitmap);
        this.#emit({
          type: "receipt-received",
          at: new Date().toISOString(),
          logicalId: key,
          windowStart: frame.windowStart,
          windowCount: frame.windowCount,
          bitmap: frame.bitmap,
        });
        break;
      case FrameKind.completion:
        signals.complete();
        break;
      case FrameKind.rejection:
        signals.reject(
          new TransferRejectedError(
            `Transfer rejected with code ${frame.code}`,
          ),
        );
        this.#emit({
          type: "transfer-rejected",
          at: new Date().toISOString(),
          logicalId: key,
          code: frame.code,
        });
        break;
    }
  }

  #deliverMessage(
    message: SupportedMessage,
    source: NodeId,
    destination: NodeId,
    logicalId: bigint,
    delivery: "complete" | "transfer",
    snrDb: number | undefined,
  ): void {
    const definition = definitionForMessage(message);
    const receivedAt = new Date();
    const received: ReceivedMessage = {
      message,
      source,
      destination,
      logicalId: logicalIdHex(logicalId),
      delivery,
      receivedAt,
      ...(snrDb === undefined ? {} : { snrDb }),
    };
    for (const listener of this.#messageListeners) {
      void Promise.resolve(listener(received)).catch((error: unknown) => {
        this.#protocolError(
          `Message listener failed: ${asError(error).message}`,
        );
      });
    }
    this.#emit({
      type: "message-received",
      at: receivedAt.toISOString(),
      logicalId: received.logicalId,
      source,
      messageType: definition.id,
      messageName: definition.name,
      delivery,
      ...(snrDb === undefined ? {} : { snrDb }),
    });
    if (definition.onMessage !== undefined) {
      void Promise.resolve(
        definition.onMessage(message, {
          source,
          destination,
          receivedAt,
          reply: async (reply, priority) => {
            await this.send(reply as SupportedMessage, {
              destination: source,
              ...(priority === undefined ? {} : { priority }),
            });
          },
        }),
      ).catch((error: unknown) => {
        this.#protocolError(
          `Message handler failed: ${asError(error).message}`,
          {
            logicalId: received.logicalId,
          },
        );
      });
    }
  }

  async #reject(
    frame: FieldLinkFrame,
    code: number,
    reason: string,
  ): Promise<void> {
    this.#protocolError(reason, { logicalId: logicalIdHex(frame.logicalId) });
    await this.#submit(
      { ...responseBase(frame), kind: FrameKind.rejection, code },
      "high",
      undefined,
    );
  }

  #protocolError(message: string, details: Record<string, unknown> = {}): void {
    this.#emit({
      type: "protocol-error",
      at: new Date().toISOString(),
      message,
      ...details,
    });
  }

  #submit(
    frame: OutboundFieldLinkFrame,
    priority: Priority,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const transmissionId = this.#nextTransmissionId;
    this.#nextTransmissionId = (this.#nextTransmissionId + 1) & 0xffff;
    return this.#scheduler.submit(
      encodeFrame({ ...frame, transmissionId }),
      priority,
      signal,
    );
  }

  async #withOutboundTransfer<Result>(
    operation: () => Promise<Result>,
  ): Promise<Result> {
    const previous = this.#transferTail;
    let release = (): void => undefined;
    this.#transferTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      this.#throwIfClosed();
      return await operation();
    } finally {
      release();
    }
  }

  #emit(event: FieldLinkEvent): void {
    for (const listener of this.#eventListeners) {
      void Promise.resolve(listener(event)).catch(() => undefined);
    }
  }

  #throwIfClosed(): void {
    if (this.#closed) {
      throw new Error("FieldLink node is closed");
    }
  }
}

class FrameScheduler {
  readonly #queues: Record<Priority, ScheduledFrame[]> = {
    high: [],
    normal: [],
    bulk: [],
  };
  readonly #transport: FieldLinkTransport;
  readonly #emit: (event: FieldLinkEvent) => void;
  #running: Promise<void> | undefined;
  #closed = false;

  constructor(
    transport: FieldLinkTransport,
    emit: (event: FieldLinkEvent) => void,
  ) {
    this.#transport = transport;
    this.#emit = emit;
  }

  submit(
    bytes: Uint8Array,
    priority: Priority,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    if (this.#closed) {
      return Promise.reject(new Error("Frame scheduler is closed"));
    }
    return new Promise<void>((resolve, reject) => {
      this.#queues[priority].push({
        bytes,
        priority,
        ...(signal === undefined ? {} : { signal }),
        resolve,
        reject,
      });
      this.#ensureRunning();
    });
  }

  async close(): Promise<void> {
    this.#closed = true;
    const error = new Error("Frame scheduler closed");
    for (const queue of Object.values(this.#queues)) {
      for (const item of queue.splice(0)) {
        item.reject(error);
      }
    }
    await this.#running;
  }

  async #run(): Promise<void> {
    for (;;) {
      const item = this.#takeNext();
      if (item === undefined) {
        return;
      }
      try {
        throwIfAborted(item.signal);
        await this.#waitForShallowQueue(item.signal);
        await this.#transport.send(item.bytes);
        await this.#waitForShallowQueue(item.signal);
        this.#emit({
          type: "frame-sent",
          at: new Date().toISOString(),
          priority: item.priority,
          bytes: item.bytes.length,
        });
        item.resolve();
      } catch (error: unknown) {
        const failure = asError(error);
        this.#emit({
          type: "transport-error",
          at: new Date().toISOString(),
          message: failure.message,
        });
        item.reject(failure);
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }

  async #waitForShallowQueue(signal: AbortSignal | undefined): Promise<void> {
    for (;;) {
      this.#throwIfClosed();
      throwIfAborted(signal);
      if ((await this.#transport.getQueueLength()) === 0) {
        this.#throwIfClosed();
        return;
      }
      await wait(QUEUE_POLL_MS, signal);
    }
  }

  #takeNext(): ScheduledFrame | undefined {
    return (
      this.#queues.high.shift() ??
      this.#queues.normal.shift() ??
      this.#queues.bulk.shift()
    );
  }

  #next(): ScheduledFrame | undefined {
    return (
      this.#queues.high[0] ?? this.#queues.normal[0] ?? this.#queues.bulk[0]
    );
  }

  #ensureRunning(): void {
    if (this.#running !== undefined) {
      return;
    }
    this.#running = this.#run().finally(() => {
      this.#running = undefined;
      if (!this.#closed && this.#next() !== undefined) {
        this.#ensureRunning();
      }
    });
  }

  #throwIfClosed(): void {
    if (this.#closed) {
      throw new Error("Frame scheduler closed");
    }
  }
}

class OutboundSignals {
  #ready = false;
  #completed = false;
  #failure: Error | undefined;
  readonly #receiptSequences = new Map<number, number>();
  readonly #receiptBitmaps = new Map<number, number>();
  readonly #waiters = new Set<() => void>();

  ready(): void {
    this.#ready = true;
    this.#pulse();
  }

  receipt(windowStart: number, bitmap: number): void {
    this.#receiptSequences.set(
      windowStart,
      (this.#receiptSequences.get(windowStart) ?? 0) + 1,
    );
    this.#receiptBitmaps.set(windowStart, bitmap);
    this.#pulse();
  }

  complete(): void {
    this.#completed = true;
    this.#pulse();
  }

  reject(error: Error): void {
    this.#failure ??= error;
    this.#pulse();
  }

  receiptSequence(windowStart: number): number {
    return this.#receiptSequences.get(windowStart) ?? 0;
  }

  waitForReady(
    timeoutMs: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    return this.#waitFor(() => this.#ready, timeoutMs, signal).then(
      () => undefined,
    );
  }

  async waitForReceipt(
    windowStart: number,
    afterSequence: number,
    timeoutMs: number,
    signal: AbortSignal | undefined,
  ): Promise<number> {
    await this.#waitFor(
      () => (this.#receiptSequences.get(windowStart) ?? 0) > afterSequence,
      timeoutMs,
      signal,
    );
    const bitmap = this.#receiptBitmaps.get(windowStart);
    if (bitmap === undefined) {
      throw new Error("Receipt arrived without a bitmap");
    }
    return bitmap;
  }

  waitForCompletion(
    timeoutMs: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    return this.#waitFor(() => this.#completed, timeoutMs, signal).then(
      () => undefined,
    );
  }

  #waitFor(
    predicate: () => boolean,
    timeoutMs: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    if (signal?.aborted === true) {
      return Promise.reject(abortError(signal));
    }
    if (this.#failure !== undefined) {
      return Promise.reject(this.#failure);
    }
    if (predicate()) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      const check = (): void => {
        if (this.#failure !== undefined) {
          cleanup();
          reject(this.#failure);
        } else if (predicate()) {
          cleanup();
          resolve();
        }
      };
      const abort = (): void => {
        cleanup();
        reject(abortError(signal));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(
          new Error(
            `Timed out waiting for transfer control after ${timeoutMs} ms`,
          ),
        );
      }, timeoutMs);
      const cleanup = (): void => {
        clearTimeout(timer);
        this.#waiters.delete(check);
        signal?.removeEventListener("abort", abort);
      };
      this.#waiters.add(check);
      signal?.addEventListener("abort", abort, { once: true });
      check();
    });
  }

  #pulse(): void {
    for (const waiter of [...this.#waiters]) {
      waiter();
    }
  }
}

function responseBase(frame: FieldLinkFrame): {
  readonly source: NodeId;
  readonly destination: NodeId;
  readonly logicalId: bigint;
} {
  return {
    source: frame.destination,
    destination: frame.source,
    logicalId: frame.logicalId,
  };
}

function responseFrame(
  frame: FieldLinkFrame,
  kind: FrameKind.transferReady | FrameKind.completion,
): OutboundFieldLinkFrame {
  return { ...responseBase(frame), kind };
}

function randomUint16(): number {
  const bytes = randomBytes(2);
  return new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength,
  ).getUint16(0, true);
}

function randomLogicalId(): bigint {
  const bytes = randomBytes(8);
  return new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength,
  ).getBigUint64(0, true);
}

function logicalIdHex(logicalId: bigint): string {
  return logicalId.toString(16).padStart(16, "0");
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) {
    throw abortError(signal);
  }
}

function abortError(signal: AbortSignal | undefined): Error {
  return signal?.reason instanceof Error
    ? signal.reason
    : new Error("Operation aborted");
}

function wait(
  milliseconds: number,
  signal: AbortSignal | undefined,
): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.reject(abortError(signal));
  }
  return new Promise<void>((resolve, reject) => {
    const finish = (): void => {
      signal?.removeEventListener("abort", abort);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    const abort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      reject(abortError(signal));
    };
    signal?.addEventListener("abort", abort, { once: true });
  });
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export type { NodeId, Priority } from "./node-types.js";
export type { SupportedMessage } from "./messages/index.js";
export { nodeIdFromPublicKey, parseNodeId } from "./node-types.js";
