import { describe, expect, it } from "vitest";

import { buildMessageCatalog, waitForExerciseCompletion } from "../src/cli.js";
import { testMessage } from "../src/messages/test.js";
import {
  FieldLinkNode,
  parseNodeId,
  type FieldLinkEvent,
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
});
