import { describe, expect, it } from "vitest";

import { FIELDLINK_MAX_MESSAGE_BYTES } from "../src/frame.js";
import {
  definitionForType,
  messageRegistry,
  validateRegistry,
  type MessageDefinition,
  type SupportedMessage,
} from "../src/messages/index.js";
import { testMessage } from "../src/messages/test.js";

describe("message registry contracts", () => {
  it("has unique IDs and names and round-trips every example", () => {
    expect(() => {
      validateRegistry(messageRegistry);
    }).not.toThrow();
    for (const definition of messageRegistry) {
      for (const example of definition.examples) {
        expect(definition.validate(example)).toBe(true);
        expect(definition.decode(definition.encode(example))).toEqual(example);
      }
    }
  });

  it("builds a valid hardware exercise for every registered message", () => {
    for (const definition of messageRegistry) {
      for (const payloadBytes of [
        definition.exercise.defaultPayloadBytes,
        definition.exercise.maximumPayloadBytes,
        ...definition.exercise.payloadPresets,
      ]) {
        const message = definition.exercise.create(payloadBytes);
        expect(definition.validate(message)).toBe(true);
        expect(definition.exercise.key(message)).not.toBe("");
        expect(definition.encode(message).length).toBeLessThanOrEqual(
          FIELDLINK_MAX_MESSAGE_BYTES,
        );
      }
    }
  });

  it("rejects duplicate IDs and names", () => {
    const duplicate = {
      ...testMessage,
      examples: testMessage.examples,
    } satisfies MessageDefinition<SupportedMessage>;
    expect(() => {
      validateRegistry([testMessage, duplicate]);
    }).toThrow("Duplicate message ID");
    expect(() => {
      validateRegistry([
        { ...duplicate, id: 2 },
        { ...duplicate, id: 3 },
      ]);
    }).toThrow("Duplicate message name");
  });

  it("does not resolve unknown numeric types", () => {
    expect(definitionForType(0xffff)).toBeUndefined();
  });
});

describe("Test message", () => {
  it("validates request and response values with arbitrary binary payloads", () => {
    const payload = Uint8Array.of(0, 255, 0, 128);
    const request = {
      type: "test",
      kind: "request",
      correlationId: 42,
      payload,
    } as const;
    const response = { ...request, kind: "response" } as const;
    expect(testMessage.validate(request)).toBe(true);
    expect(testMessage.validate(response)).toBe(true);
    expect(testMessage.decode(testMessage.encode(request))).toEqual(request);
    expect(testMessage.decode(testMessage.encode(response))).toEqual(response);
  });

  it("rejects malformed control values and radio bytes", () => {
    expect(
      testMessage.validate({
        type: "test",
        kind: "request",
        correlationId: -1,
        payload: new Uint8Array(),
      }),
    ).toBe(false);
    expect(() => testMessage.decode(Uint8Array.of(1, 0))).toThrow();
    expect(() => testMessage.decode(Uint8Array.of(9, 0, 0, 0, 0))).toThrow(
      "Unknown Test variant",
    );
  });

  it("completes its exercise only for an exact response at the source", () => {
    const sent = testMessage.exercise.create(4096);
    const response = { ...sent, kind: "response" } as const;
    expect(
      testMessage.exercise.isComplete({
        sent,
        received: response,
        side: "source",
      }),
    ).toBe(true);
    expect(
      testMessage.exercise.isComplete({
        sent,
        received: response,
        side: "destination",
      }),
    ).toBe(false);
    expect(
      testMessage.exercise.isComplete({
        sent,
        received: { ...response, payload: Uint8Array.of(1) },
        side: "source",
      }),
    ).toBe(false);
  });

  it("uses a new correlation ID for every exercise", () => {
    const first = testMessage.exercise.create(64);
    const second = testMessage.exercise.create(64);

    expect(second.correlationId).not.toBe(first.correlationId);
    expect(second.payload).toEqual(first.payload);
  });
});
