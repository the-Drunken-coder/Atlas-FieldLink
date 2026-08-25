import { describe, expect, it, vi } from "vitest";

import { AdapterProcessNode } from "../src/adapter-process.js";
import {
  buildMessageCatalog,
  main,
  waitForExerciseCompletion,
  type ExerciseNode,
} from "../src/cli.js";
import { TestArtifacts } from "../src/evidence.js";
import {
  COMPLETE_MESSAGE_BODY_BYTES,
  FIELDLINK_MAX_MESSAGE_BYTES,
  TRANSFER_FRAGMENT_BYTES,
} from "../src/frame.js";
import { testMessage } from "../src/messages/test.js";
import {
  FieldLinkNode,
  parseNodeId,
  type FieldLinkEvent,
  type ReceivedMessage,
} from "../src/node.js";
import { memoryTransportPair } from "./helpers.js";

describe("CLI message catalog", () => {
  it("lists every registered message with runnable payload presets", () => {
    const catalog = buildMessageCatalog();

    expect(catalog.messages).toEqual([
      {
        id: 1,
        name: "test",
        defaultPriority: "normal",
        exercise: {
          defaultPayloadBytes: 64,
          maximumPayloadBytes: 1024 * 1024 - 5,
          presets: [
            {
              payloadBytes: 64,
              encodedBytes: 69,
              delivery: "complete",
              fragments: 1,
            },
            {
              payloadBytes: 127,
              encodedBytes: 132,
              delivery: "complete",
              fragments: 1,
            },
            {
              payloadBytes: 4096,
              encodedBytes: 4101,
              delivery: "transfer",
              fragments: 32,
            },
          ],
        },
      },
    ]);
    expect(catalog.retryStrategies).toEqual([
      { id: 1, name: "selective-window" },
    ]);
    expect(catalog.delivery).toEqual({
      maximumEncodedMessageBytes: FIELDLINK_MAX_MESSAGE_BYTES,
      maximumCompleteMessageBytes: COMPLETE_MESSAGE_BODY_BYTES,
      transferFragmentBytes: TRANSFER_FRAGMENT_BYTES,
    });
  });
});

describe("CLI message exercise", () => {
  it("waits for parent inbox evidence before acknowledging the adapter", async () => {
    let releaseInboxEvidence = (): void => undefined;
    const inboxEvidence = new Promise<void>((resolve) => {
      releaseInboxEvidence = resolve;
    });
    const artifacts = {
      paths: {
        directory: "test-artifacts",
        manifest: "test-artifacts/manifest.json",
        events: "test-artifacts/events.jsonl",
        summary: "test-artifacts/summary.json",
      },
      record: (type: string) =>
        type === "inbox-message" ? inboxEvidence : Promise.resolve(),
      flush: () => Promise.resolve(),
      finish: () => Promise.resolve(),
    } as unknown as TestArtifacts;
    const create = vi
      .spyOn(TestArtifacts, "create")
      .mockResolvedValue(artifacts);
    let acknowledged = false;
    let acknowledgedBeforePersistence = false;
    const start = vi.spyOn(AdapterProcessNode, "start");
    start.mockImplementationOnce(async (options) => {
      const acknowledgement = Promise.resolve(
        options.onInboxMessage?.({
          channelMessage: {
            channelIdx: 1,
            pathLen: 1,
            txtType: 0,
            senderTimestamp: 1,
            text: "preserve me",
          },
        }),
      );
      void acknowledgement.then(() => {
        acknowledged = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      acknowledgedBeforePersistence = acknowledged;
      releaseInboxEvidence();
      await acknowledgement;
      throw new Error("adapter A stopped after evidence test");
    });
    start.mockRejectedValueOnce(new Error("adapter B stopped"));
    try {
      await expect(
        main([
          "test",
          "--a",
          "/dev/cu.a",
          "--b",
          "/dev/cu.b",
          "--channel",
          "1",
          "--timeout-ms",
          "1000",
          "--allow-inbox-drain",
        ]),
      ).resolves.toBe(1);
      expect(acknowledgedBeforePersistence).toBe(false);
      expect(acknowledged).toBe(true);
      expect(
        start.mock.calls.map(([options]) => options.evidenceDirectory),
      ).toEqual(["test-artifacts/adapters/a", "test-artifacts/adapters/b"]);
    } finally {
      releaseInboxEvidence();
      start.mockRestore();
      create.mockRestore();
    }
  });

  it("cancels sibling startup when one adapter fails", async () => {
    const artifacts = {
      paths: {
        directory: "test-artifacts",
        manifest: "test-artifacts/manifest.json",
        events: "test-artifacts/events.jsonl",
        summary: "test-artifacts/summary.json",
      },
      record: () => Promise.resolve(),
      flush: () => Promise.resolve(),
      finish: () => Promise.resolve(),
    } as unknown as TestArtifacts;
    const create = vi
      .spyOn(TestArtifacts, "create")
      .mockResolvedValue(artifacts);
    let siblingAborted = false;
    const start = vi.spyOn(AdapterProcessNode, "start");
    start.mockRejectedValueOnce(new Error("adapter A failed"));
    start.mockImplementationOnce(
      (options) =>
        new Promise<AdapterProcessNode>((_resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error("sibling startup was not cancelled"));
          }, 100);
          const abort = (): void => {
            siblingAborted = true;
            clearTimeout(timeout);
            reject(new Error("sibling startup cancelled"));
          };
          options.signal?.addEventListener("abort", abort, { once: true });
          if (options.signal?.aborted === true) {
            abort();
          }
        }),
    );
    try {
      await expect(
        main([
          "test",
          "--a",
          "/dev/cu.a",
          "--b",
          "/dev/cu.b",
          "--channel",
          "1",
          "--timeout-ms",
          "1000",
          "--allow-inbox-drain",
        ]),
      ).resolves.toBe(1);
      expect(siblingAborted).toBe(true);
    } finally {
      start.mockRestore();
      create.mockRestore();
    }
  });

  it("handles interruption while artifacts are being created", async () => {
    const initialListeners = process.listeners("SIGINT");
    const records: { readonly type: string; readonly data: unknown }[] = [];
    let finishedSummary: unknown;
    const artifacts = {
      paths: {
        directory: "test-artifacts",
        manifest: "test-artifacts/manifest.json",
        events: "test-artifacts/events.jsonl",
        summary: "test-artifacts/summary.json",
      },
      record(type: string, data: unknown): Promise<void> {
        records.push({ type, data });
        return Promise.resolve();
      },
      flush(): Promise<void> {
        return Promise.resolve();
      },
      finish(summary: unknown): Promise<void> {
        finishedSummary = summary;
        return Promise.resolve();
      },
    } as unknown as TestArtifacts;
    const create = vi.spyOn(TestArtifacts, "create").mockImplementation(() => {
      const interrupt = process
        .listeners("SIGINT")
        .find((listener) => !initialListeners.includes(listener));
      expect(interrupt).toBeDefined();
      interrupt?.("SIGINT");
      return Promise.resolve(artifacts);
    });
    try {
      await expect(
        main([
          "test",
          "--a",
          "/dev/cu.a",
          "--b",
          "/dev/cu.b",
          "--channel",
          "1",
          "--timeout-ms",
          "1000",
          "--allow-inbox-drain",
        ]),
      ).resolves.toBe(130);
      expect(records).toContainEqual({
        type: "interrupted",
        data: { signal: "SIGINT" },
      });
      expect(finishedSummary).toMatchObject({
        status: "interrupted",
        interrupted: true,
        interruptedBy: "SIGINT",
        partial: true,
      });
      expect(process.listeners("SIGINT")).toEqual(initialListeners);
    } finally {
      create.mockRestore();
    }
  });

  it("waits for the echoed transfer handshake before shutdown", async () => {
    const [transportA, transportB] = memoryTransportPair();
    const a = new FieldLinkNode({
      nodeId: parseNodeId("aaaaaaaaaaaaaaaa"),
      transport: transportA,
      retryTimeoutMs: 10,
    });
    const b = new FieldLinkNode({
      nodeId: parseNodeId("bbbbbbbbbbbbbbbb"),
      transport: transportB,
      retryTimeoutMs: 10,
    });
    const destinationEvents: FieldLinkEvent[] = [];
    b.onEvent((event) => {
      destinationEvents.push(event);
    });

    const sent = testMessage.exercise.create(4096);
    const controller = new AbortController();
    const completionPromise = waitForExerciseCompletion(
      a,
      b,
      testMessage,
      sent,
      controller.signal,
    );
    const sendResult = await a.send(sent, {
      destination: b.nodeId,
      retryStrategy: "selective-window",
      signal: controller.signal,
    });
    const completion = await completionPromise;
    await Promise.all([a.close(), b.close()]);

    expect(sendResult.delivery).toBe("transfer");
    expect(completion.received.message).toMatchObject({
      type: "test",
      kind: "response",
    });
    expect(
      destinationEvents.some(
        (event) =>
          event.type === "transfer-completed" &&
          event.logicalId === completion.received.logicalId,
      ),
    ).toBe(true);
    expect(
      destinationEvents.some((event) => event.type === "transfer-failed"),
    ).toBe(false);
  });

  it("fails as soon as the echo transfer fails", async () => {
    const source = new ExerciseNodeProbe("aaaaaaaaaaaaaaaa");
    const destination = new ExerciseNodeProbe("bbbbbbbbbbbbbbbb");
    const controller = new AbortController();
    const sent = testMessage.exercise.create(4096);
    const completion = waitForExerciseCompletion(
      source,
      destination,
      testMessage,
      sent,
      controller.signal,
    );

    destination.emitEvent({
      type: "transfer-started",
      at: new Date().toISOString(),
      logicalId: "0000000000000001",
      destination: source.nodeId,
      exerciseKey: testMessage.exercise.key(sent),
    });
    destination.emitEvent({
      type: "transfer-failed",
      at: new Date().toISOString(),
      logicalId: "0000000000000001",
      error: "repairs exhausted",
    });

    await expect(completion).rejects.toThrow(
      "Echo transfer failed: repairs exhausted",
    );
  });

  it("fails as soon as the destination cannot send the echo", async () => {
    const source = new ExerciseNodeProbe("aaaaaaaaaaaaaaaa");
    const destination = new ExerciseNodeProbe("bbbbbbbbbbbbbbbb");
    const controller = new AbortController();
    const sent = testMessage.exercise.create(64);
    const completion = waitForExerciseCompletion(
      source,
      destination,
      testMessage,
      sent,
      controller.signal,
    );

    destination.emitMessage({
      message: sent,
      source: source.nodeId,
      destination: destination.nodeId,
      logicalId: "0000000000000001",
      delivery: "complete",
      receivedAt: new Date(),
    });
    destination.emitEvent({
      type: "protocol-error",
      at: new Date().toISOString(),
      logicalId: "0000000000000001",
      message: "Message handler failed: radio rejected",
    });

    await expect(completion).rejects.toThrow(
      "Echo handler failed: radio rejected",
    );
  });

  it("ignores an unrelated echo transfer failure", async () => {
    const source = new ExerciseNodeProbe("aaaaaaaaaaaaaaaa");
    const destination = new ExerciseNodeProbe("bbbbbbbbbbbbbbbb");
    const controller = new AbortController();
    const sent = testMessage.exercise.create(4096);
    const completion = waitForExerciseCompletion(
      source,
      destination,
      testMessage,
      sent,
      controller.signal,
    );

    destination.emitEvent({
      type: "transfer-started",
      at: new Date().toISOString(),
      logicalId: "0000000000000001",
      destination: source.nodeId,
      exerciseKey: "unrelated",
    });
    destination.emitEvent({
      type: "transfer-failed",
      at: new Date().toISOString(),
      logicalId: "0000000000000001",
      error: "unrelated failure",
    });
    destination.emitEvent({
      type: "transfer-started",
      at: new Date().toISOString(),
      logicalId: "0000000000000002",
      destination: source.nodeId,
      exerciseKey: testMessage.exercise.key(sent),
    });
    destination.emitEvent({
      type: "transfer-failed",
      at: new Date().toISOString(),
      logicalId: "0000000000000002",
      error: "current failure",
    });

    await expect(completion).rejects.toThrow(
      "Echo transfer failed: current failure",
    );
  });
});

class ExerciseNodeProbe implements ExerciseNode {
  readonly nodeId;
  readonly #messageListeners = new Set<
    (message: ReceivedMessage) => void | Promise<void>
  >();
  readonly #eventListeners = new Set<
    (event: FieldLinkEvent) => void | Promise<void>
  >();

  constructor(nodeId: string) {
    this.nodeId = parseNodeId(nodeId);
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

  emitEvent(event: FieldLinkEvent): void {
    for (const listener of this.#eventListeners) {
      void listener(event);
    }
  }

  emitMessage(message: ReceivedMessage): void {
    for (const listener of this.#messageListeners) {
      void listener(message);
    }
  }
}
