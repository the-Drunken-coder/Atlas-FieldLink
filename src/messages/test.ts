import { randomInt } from "node:crypto";

import {
  MessageValidationError,
  type MessageDefinition,
} from "./definition.js";

const HEADER_BYTES = 5;
const MAX_ENCODED_BYTES = 1024 * 1024;
let nextExerciseCorrelationId = randomInt(0x1_0000_0000);

/**
 * Test proves end-to-end FieldLink delivery without introducing Atlas domain
 * semantics. A request or response contains a uint32 correlation ID and zero
 * or more arbitrary payload bytes. The encoded message is limited to 1 MiB,
 * including its one-byte variant and four-byte correlation header.
 *
 * Receivers echo requests to the source with the same correlation ID and an
 * exact copy of the payload. Responses are delivered to listeners and are
 * never echoed, which prevents response loops.
 *
 * The hardware exercise sends a request with a process-unique correlation ID
 * and deterministic payload. It completes when the source receives the
 * matching response. Payload presets cover a normal single frame, the largest
 * single frame, and a fragmented transfer.
 *
 * Examples:
 * - `{ type: "test", kind: "request", correlationId: 1, payload: new Uint8Array() }`
 * - `{ type: "test", kind: "response", correlationId: 1, payload: Uint8Array.of(0, 255) }`
 */
export type TestMessage =
  | {
      readonly type: "test";
      readonly kind: "request";
      readonly correlationId: number;
      readonly payload: Uint8Array;
    }
  | {
      readonly type: "test";
      readonly kind: "response";
      readonly correlationId: number;
      readonly payload: Uint8Array;
    };

export const testMessage = {
  id: 1,
  name: "test",
  defaultPriority: "normal",
  examples: [
    {
      type: "test",
      kind: "request",
      correlationId: 1,
      payload: new Uint8Array(),
    },
    {
      type: "test",
      kind: "response",
      correlationId: 0xffff_ffff,
      payload: Uint8Array.of(0, 1, 0xff),
    },
  ],
  validate(value: unknown): value is TestMessage {
    if (!isRecord(value)) {
      return false;
    }
    return (
      value.type === "test" &&
      (value.kind === "request" || value.kind === "response") &&
      isUint32(value.correlationId) &&
      value.payload instanceof Uint8Array &&
      value.payload.length <= MAX_ENCODED_BYTES - HEADER_BYTES
    );
  },
  encode(message: TestMessage): Uint8Array {
    if (!this.validate(message)) {
      throw new MessageValidationError("Invalid Test message");
    }
    const bytes = new Uint8Array(HEADER_BYTES + message.payload.length);
    const view = new DataView(bytes.buffer);
    view.setUint8(0, message.kind === "request" ? 1 : 2);
    view.setUint32(1, message.correlationId, true);
    bytes.set(message.payload, HEADER_BYTES);
    return bytes;
  },
  decode(bytes: Uint8Array): TestMessage {
    if (bytes.length < HEADER_BYTES || bytes.length > MAX_ENCODED_BYTES) {
      throw new MessageValidationError(
        `Test message must be ${HEADER_BYTES} to ${MAX_ENCODED_BYTES} bytes`,
      );
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const variant = view.getUint8(0);
    if (variant !== 1 && variant !== 2) {
      throw new MessageValidationError(`Unknown Test variant ${variant}`);
    }
    return {
      type: "test",
      kind: variant === 1 ? "request" : "response",
      correlationId: view.getUint32(1, true),
      payload: bytes.slice(HEADER_BYTES),
    };
  },
  exercise: {
    defaultPayloadBytes: 64,
    maximumPayloadBytes: MAX_ENCODED_BYTES - HEADER_BYTES,
    payloadPresets: [64, 127, 4096],
    create(payloadBytes: number): TestMessage {
      return {
        type: "test",
        kind: "request",
        correlationId: takeExerciseCorrelationId(),
        payload: Uint8Array.from(
          { length: payloadBytes },
          (_value, index) => (index * 31 + 17) & 0xff,
        ),
      };
    },
    key(message): string {
      return message.correlationId.toString(16).padStart(8, "0");
    },
    isComplete({ sent, received, side }): boolean {
      return (
        side === "source" &&
        sent.kind === "request" &&
        received.kind === "response" &&
        received.correlationId === sent.correlationId &&
        equalBytes(received.payload, sent.payload)
      );
    },
  },
  async onMessage(message, context): Promise<void> {
    if (message.kind === "response") {
      return;
    }
    await context.reply({
      type: "test",
      kind: "response",
      correlationId: message.correlationId,
      payload: message.payload.slice(),
    });
  },
} satisfies MessageDefinition<TestMessage>;

function takeExerciseCorrelationId(): number {
  const correlationId = nextExerciseCorrelationId;
  nextExerciseCorrelationId = (nextExerciseCorrelationId + 1) >>> 0;
  return correlationId;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isUint32(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 0 &&
    value <= 0xffff_ffff
  );
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false;
  }
  return a.every((value, index) => value === b[index]);
}
