import { describe, expect, it } from "vitest";

import { FIELDLINK_MAX_MESSAGE_BYTES } from "../src/frame.js";
import {
  definitionForType,
  attachResourceRequestHandler,
  isJsonValue,
  messageRegistry,
  resourceMessage,
  validateRegistry,
  type MessageDefinition,
  type ResourceMessage,
  type ResourceRequest,
  type ResourceResponse,
  type SupportedMessage,
} from "../src/messages/index.js";
import { testMessage } from "../src/messages/test.js";
import {
  parseNodeId,
  type NodeId,
  type ReceivedMessage,
  type SendOptions,
  type SendResult,
} from "../src/node.js";

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

describe("Resource message", () => {
  it("supports the approved JSON CRUD request and response variants", () => {
    const messages = [
      {
        type: "resource",
        kind: "request",
        operation: "create",
        request_id: "create-entity",
        resource_type: "entity",
        body: { name: "Rescue 1", enabled: true },
      },
      {
        type: "resource",
        kind: "request",
        operation: "get",
        request_id: "get-task",
        resource_type: "task",
        resource_id: "task-123",
      },
      {
        type: "resource",
        kind: "request",
        operation: "list",
        request_id: "list-tasks",
        resource_type: "task",
        query: { limit: 25, cursor: "page-2" },
      },
      {
        type: "resource",
        kind: "request",
        operation: "patch",
        request_id: "patch-object",
        resource_type: "object",
        resource_id: "object-123",
        body: { metadata: { title: "Updated" } },
      },
      {
        type: "resource",
        kind: "request",
        operation: "delete",
        request_id: "delete-entity",
        resource_type: "entity",
        resource_id: "entity-123",
      },
      {
        type: "resource",
        kind: "response",
        request_id: "get-task",
        status: 200,
        body: { id: "task-123", tags: ["urgent", null] },
      },
    ] as const satisfies readonly ResourceMessage[];

    for (const message of messages) {
      expect(resourceMessage.validate(message)).toBe(true);
      expect(resourceMessage.decode(resourceMessage.encode(message))).toEqual(
        message,
      );
    }
  });

  it("keeps Task creation, mutation, and deletion out of generic CRUD", () => {
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "create",
        request_id: "create-task",
        resource_type: "task",
        body: { title: "Inspect site" },
      }),
    ).toBe(false);
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "patch",
        request_id: "patch-task",
        resource_type: "task",
        resource_id: "task-123",
        body: { status: "complete" },
      }),
    ).toBe(false);
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "delete",
        request_id: "delete-task",
        resource_type: "task",
        resource_id: "task-123",
      }),
    ).toBe(false);
  });

  it("requires bounded pagination and exact operation envelopes", () => {
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "list",
        request_id: "list-entities",
        resource_type: "entity",
        query: { limit: 0 },
      }),
    ).toBe(false);
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "list",
        request_id: "list-entities",
        resource_type: "entity",
        query: { limit: 1001 },
      }),
    ).toBe(false);
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "list",
        request_id: "list-entities",
        resource_type: "entity",
        query: {},
      }),
    ).toBe(false);
    expect(
      resourceMessage.validate({
        type: "resource",
        kind: "request",
        operation: "get",
        request_id: "get-entity",
        resource_type: "entity",
        resource_id: "entity-123",
        path: "/entities/entity-123",
      }),
    ).toBe(false);
  });

  it("accepts JSON values without silently coercing JavaScript values", () => {
    const response = {
      type: "resource",
      kind: "response",
      request_id: "response",
      status: 200,
    } as const;
    expect(resourceMessage.validate({ ...response, body: null })).toBe(true);
    expect(resourceMessage.validate({ ...response, body: new Date() })).toBe(
      false,
    );
    expect(resourceMessage.validate({ ...response, body: undefined })).toBe(
      false,
    );
    expect(resourceMessage.validate({ ...response, body: Number.NaN })).toBe(
      false,
    );
    const shared = { unit: "m" };
    expect(
      resourceMessage.validate({
        ...response,
        body: { width: shared, height: shared },
      }),
    ).toBe(true);
    const cycle: { self?: unknown } = {};
    cycle.self = cycle;
    expect(isJsonValue(cycle)).toBe(false);
  });

  it("bounds Resource request IDs so every correlation response can fit", () => {
    const request = {
      type: "resource",
      kind: "request",
      operation: "get",
      resource_type: "task",
      resource_id: "task-1",
    } as const;
    expect(
      resourceMessage.validate({ ...request, request_id: "x".repeat(256) }),
    ).toBe(true);
    expect(
      resourceMessage.validate({ ...request, request_id: "x".repeat(257) }),
    ).toBe(false);
  });

  it("rejects invalid UTF-8 JSON and oversized encoded messages", () => {
    expect(() => resourceMessage.decode(Uint8Array.of(0xff))).toThrow(
      "valid UTF-8 JSON",
    );
    expect(() =>
      resourceMessage.decode(new TextEncoder().encode("not-json")),
    ).toThrow("valid UTF-8 JSON");
    expect(() =>
      resourceMessage.encode({
        type: "resource",
        kind: "response",
        request_id: "oversized",
        status: 200,
        body: "x".repeat(FIELDLINK_MAX_MESSAGE_BYTES),
      }),
    ).toThrow("exceeds");
  });

  it("completes its delivery exercise only at the destination", () => {
    const sent = resourceMessage.exercise.create(4096);
    const received = resourceMessage.decode(resourceMessage.encode(sent));

    expect(
      resourceMessage.exercise.isComplete({
        sent,
        received,
        side: "destination",
      }),
    ).toBe(true);
    expect(
      resourceMessage.exercise.isComplete({
        sent,
        received,
        side: "source",
      }),
    ).toBe(false);
    expect(
      resourceMessage.exercise.isComplete({
        sent,
        received: { ...received, request_id: "different" },
        side: "destination",
      }),
    ).toBe(false);
  });

  it("matches a Resource response at the original request source", () => {
    const sent = {
      type: "resource",
      kind: "request",
      operation: "get",
      request_id: "request-1",
      resource_type: "task",
      resource_id: "task-1",
    } as const;
    const received = {
      type: "resource",
      kind: "response",
      request_id: "request-1",
      status: 200,
      body: { task_id: "task-1" },
    } as const;

    expect(
      resourceMessage.exercise.isComplete({
        sent,
        received,
        side: "source",
      }),
    ).toBe(true);
    expect(
      resourceMessage.exercise.isComplete({
        sent,
        received,
        side: "destination",
      }),
    ).toBe(false);
  });

  it("executes requests only for the allowed source and replays cached results", async () => {
    const node = new ResourceHandlerNode();
    const executor = new ResourceExecutorProbe();
    const allowed = parseNodeId("aaaaaaaaaaaaaaaa");
    const dispose = attachResourceRequestHandler(node, executor, allowed);
    const request = {
      type: "resource",
      kind: "request",
      operation: "get",
      request_id: "request-1",
      resource_type: "task",
      resource_id: "task-1",
    } as const;

    await node.emit(request, parseNodeId("cccccccccccccccc"));
    await node.emit(request, allowed);
    await node.emit(request, allowed);

    expect(executor.requests).toHaveLength(1);
    expect(node.sent).toHaveLength(2);
    expect(node.sent[0]?.message).toMatchObject({
      kind: "response",
      request_id: "request-1",
      status: 200,
    });
    expect(node.sent[0]?.options.signal?.aborted).toBe(false);
    await dispose();
    expect(node.sent[0]?.options.signal?.aborted).toBe(true);
  });

  it("rejects request ID reuse with different JSON", async () => {
    const node = new ResourceHandlerNode();
    const executor = new ResourceExecutorProbe();
    const allowed = parseNodeId("aaaaaaaaaaaaaaaa");
    attachResourceRequestHandler(node, executor, allowed);
    const request = {
      type: "resource",
      kind: "request",
      operation: "get",
      request_id: "request-1",
      resource_type: "task",
      resource_id: "task-1",
    } as const;

    await node.emit(request, allowed);
    await node.emit({ ...request, resource_id: "task-2" }, allowed);

    expect(executor.requests).toHaveLength(1);
    expect(node.sent[1]?.message).toMatchObject({ status: 409 });
  });

  it("does not evict in-flight Resource requests at cache capacity", async () => {
    const node = new ResourceHandlerNode();
    const executor = new BlockingResourceExecutor();
    const allowed = parseNodeId("aaaaaaaaaaaaaaaa");
    const dispose = attachResourceRequestHandler(node, executor, allowed);
    const requests = Array.from({ length: 64 }, (_, index) => ({
      type: "resource" as const,
      kind: "request" as const,
      operation: "get" as const,
      request_id: `request-${index}`,
      resource_type: "task" as const,
      resource_id: `task-${index}`,
    }));
    const first = requests[0];
    if (first === undefined) {
      throw new Error("Expected one Resource request");
    }
    const pending = requests.map((request) => node.emit(request, allowed));
    await Promise.resolve();

    await node.emit({ ...first, request_id: "request-at-capacity" }, allowed);
    const replay = node.emit(first, allowed);
    await Promise.resolve();

    expect(executor.requests).toHaveLength(64);
    expect(node.sent[0]?.message).toMatchObject({ status: 503 });
    executor.resolveAll();
    await Promise.all([...pending, replay]);
    await dispose();
  });

  it("aborts active Resource executions before detaching the gateway", async () => {
    const node = new ResourceHandlerNode();
    const executor = new AbortingResourceExecutor();
    const allowed = parseNodeId("aaaaaaaaaaaaaaaa");
    const dispose = attachResourceRequestHandler(node, executor, allowed);
    const received = node.emit(
      {
        type: "resource",
        kind: "request",
        operation: "get",
        request_id: "request-1",
        resource_type: "task",
        resource_id: "task-1",
      },
      allowed,
    );
    await executor.started;

    await dispose();
    await received;

    expect(executor.aborted).toBe(true);
    expect(node.sent).toHaveLength(0);
  });
});

class ResourceExecutorProbe {
  readonly requests: ResourceRequest[] = [];

  execute(request: ResourceRequest): Promise<ResourceResponse> {
    this.requests.push(request);
    return Promise.resolve({
      type: "resource",
      kind: "response",
      request_id: request.request_id,
      status: 200,
      body: { ok: true },
    });
  }
}

class BlockingResourceExecutor {
  readonly requests: ResourceRequest[] = [];
  readonly #resolvers: ((response: ResourceResponse) => void)[] = [];

  execute(request: ResourceRequest): Promise<ResourceResponse> {
    this.requests.push(request);
    return new Promise((resolve) => {
      this.#resolvers.push(resolve);
    });
  }

  resolveAll(): void {
    for (const [index, resolve] of this.#resolvers.entries()) {
      resolve({
        type: "resource",
        kind: "response",
        request_id: this.requests[index]?.request_id ?? "missing",
        status: 200,
      });
    }
  }
}

class AbortingResourceExecutor {
  aborted = false;
  readonly started: Promise<void>;
  readonly #markStarted: () => void;

  constructor() {
    let markStarted: () => void = () => undefined;
    this.started = new Promise((resolve) => {
      markStarted = resolve;
    });
    this.#markStarted = markStarted;
  }

  execute(
    _request: ResourceRequest,
    signal?: AbortSignal,
  ): Promise<ResourceResponse> {
    this.#markStarted();
    return new Promise((_resolve, reject) => {
      const abort = () => {
        this.aborted = true;
        reject(
          signal?.reason instanceof Error
            ? signal.reason
            : new Error("Resource execution aborted"),
        );
      };
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted === true) {
        abort();
      }
    });
  }
}

class ResourceHandlerNode {
  readonly sent: {
    readonly message: SupportedMessage;
    readonly options: SendOptions;
  }[] = [];
  #listener: ((message: ReceivedMessage) => void | Promise<void>) | undefined;

  onMessage(
    listener: (message: ReceivedMessage) => void | Promise<void>,
  ): () => void {
    this.#listener = listener;
    return () => {
      this.#listener = undefined;
    };
  }

  send(message: SupportedMessage, options: SendOptions): Promise<SendResult> {
    this.sent.push({ message, options });
    return Promise.resolve({
      logicalId: "0000000000000001",
      messageType: 2,
      messageName: "resource",
      destination: parseNodeId(options.destination),
      priority: "normal",
      delivery: "complete",
      encodedBytes: 1,
      fragments: 1,
      transferOpenRetries: 0,
      completionRetries: 0,
      retransmissions: 0,
      receiptRequests: 0,
      receiptRequestRetries: 0,
      receipts: 0,
      durationMs: 1,
    });
  }

  async emit(message: ResourceRequest, source: NodeId): Promise<void> {
    await this.#listener?.({
      message,
      source,
      destination: parseNodeId("bbbbbbbbbbbbbbbb"),
      logicalId: "0000000000000001",
      delivery: "complete",
      receivedAt: new Date(),
    });
  }
}
