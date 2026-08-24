import { describe, expect, it } from "vitest";

import {
  buildMessageCatalog,
  waitForExerciseCompletion,
  type ExerciseNode,
} from "../src/cli.js";
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
    const completion = waitForExerciseCompletion(
      source,
      destination,
      testMessage,
      testMessage.exercise.create(4096),
      controller.signal,
    );

    destination.emitEvent({
      type: "transfer-started",
      at: new Date().toISOString(),
      logicalId: "0000000000000001",
      destination: source.nodeId,
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
}
