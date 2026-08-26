import { randomInt } from "node:crypto";

import { FIELDLINK_MAX_MESSAGE_BYTES } from "../frame.js";
import type { FieldLinkNode, NodeId, ReceivedMessage } from "../node.js";
import {
  MessageValidationError,
  type MessageDefinition,
} from "./definition.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });
let nextExerciseRequestId = randomInt(0x1_0000_0000);

export type JsonValue =
  null | boolean | number | string | readonly JsonValue[] | JsonObject;

export interface JsonObject {
  readonly [key: string]: JsonValue;
}

export type ResourceType = "entity" | "object" | "task";

export interface ResourceListQuery {
  readonly limit: number;
  readonly cursor?: string;
}

/**
 * Resource carries broad Atlas API operations as UTF-8 JSON. FieldLink
 * validates the operation envelope and JSON representation; the application
 * receiving the message remains responsible for validating the resource body,
 * authenticating the caller, authorizing the operation, and applying it.
 *
 * Tasks are read through this message but keep their explicit lifecycle
 * operations outside generic CRUD. Atlas Object bytes also stay outside this
 * message; Object create and patch bodies contain metadata only.
 */
export type ResourceMessage =
  | {
      readonly type: "resource";
      readonly kind: "request";
      readonly operation: "create";
      readonly request_id: string;
      readonly resource_type: "entity" | "object";
      readonly body: JsonObject;
    }
  | {
      readonly type: "resource";
      readonly kind: "request";
      readonly operation: "get";
      readonly request_id: string;
      readonly resource_type: ResourceType;
      readonly resource_id: string;
    }
  | {
      readonly type: "resource";
      readonly kind: "request";
      readonly operation: "list";
      readonly request_id: string;
      readonly resource_type: ResourceType;
      readonly query: ResourceListQuery;
    }
  | {
      readonly type: "resource";
      readonly kind: "request";
      readonly operation: "patch";
      readonly request_id: string;
      readonly resource_type: "entity" | "object";
      readonly resource_id: string;
      readonly body: JsonObject;
    }
  | {
      readonly type: "resource";
      readonly kind: "request";
      readonly operation: "delete";
      readonly request_id: string;
      readonly resource_type: "entity" | "object";
      readonly resource_id: string;
    }
  | {
      readonly type: "resource";
      readonly kind: "response";
      readonly request_id: string;
      readonly status: number;
      readonly body?: JsonValue;
    };

export type ResourceRequest = Extract<ResourceMessage, { kind: "request" }>;
export type ResourceResponse = Extract<ResourceMessage, { kind: "response" }>;

export interface ResourceRequestExecutor {
  execute(request: ResourceRequest): Promise<ResourceResponse>;
}

const exerciseEnvelopeBytes = textEncoder.encode(
  JSON.stringify({
    type: "resource",
    kind: "response",
    request_id: "exercise-00000000",
    status: 200,
    body: "",
  } satisfies ResourceMessage),
).length;

export const resourceMessage = {
  id: 2,
  name: "resource",
  defaultPriority: "normal",
  examples: [
    {
      type: "resource",
      kind: "request",
      operation: "create",
      request_id: "req-create-entity",
      resource_type: "entity",
      body: { entity_id: "rescue-1", entity_type: "vehicle" },
    },
    {
      type: "resource",
      kind: "request",
      operation: "get",
      request_id: "req-get-task",
      resource_type: "task",
      resource_id: "task-123",
    },
    {
      type: "resource",
      kind: "request",
      operation: "list",
      request_id: "req-list-objects",
      resource_type: "object",
      query: { limit: 50, cursor: "next-page" },
    },
    {
      type: "resource",
      kind: "request",
      operation: "patch",
      request_id: "req-patch-entity",
      resource_type: "entity",
      resource_id: "entity-123",
      body: { alias: "Rescue 2" },
    },
    {
      type: "resource",
      kind: "request",
      operation: "delete",
      request_id: "req-delete-object",
      resource_type: "object",
      resource_id: "object-123",
    },
    {
      type: "resource",
      kind: "response",
      request_id: "req-get-task",
      status: 200,
      body: { task_id: "task-123", status: "assigned" },
    },
  ],
  validate(value: unknown): value is ResourceMessage {
    if (!isPlainRecord(value) || value.type !== "resource") {
      return false;
    }
    if (value.kind === "response") {
      return isResponse(value);
    }
    if (value.kind !== "request" || typeof value.operation !== "string") {
      return false;
    }
    switch (value.operation) {
      case "create":
        return (
          hasShape(value, [
            "type",
            "kind",
            "operation",
            "request_id",
            "resource_type",
            "body",
          ]) &&
          isRequestId(value.request_id) &&
          isMutableResourceType(value.resource_type) &&
          isJsonObject(value.body)
        );
      case "get":
      case "delete":
        return (
          hasShape(value, [
            "type",
            "kind",
            "operation",
            "request_id",
            "resource_type",
            "resource_id",
          ]) &&
          isRequestId(value.request_id) &&
          (value.operation === "get"
            ? isResourceType(value.resource_type)
            : isMutableResourceType(value.resource_type)) &&
          isNonEmptyString(value.resource_id)
        );
      case "list":
        return (
          hasShape(value, [
            "type",
            "kind",
            "operation",
            "request_id",
            "resource_type",
            "query",
          ]) &&
          isRequestId(value.request_id) &&
          isResourceType(value.resource_type) &&
          isListQuery(value.query)
        );
      case "patch":
        return (
          hasShape(value, [
            "type",
            "kind",
            "operation",
            "request_id",
            "resource_type",
            "resource_id",
            "body",
          ]) &&
          isRequestId(value.request_id) &&
          isMutableResourceType(value.resource_type) &&
          isNonEmptyString(value.resource_id) &&
          isJsonObject(value.body)
        );
      default:
        return false;
    }
  },
  encode(message: ResourceMessage): Uint8Array {
    if (!this.validate(message)) {
      throw new MessageValidationError("Invalid Resource message");
    }
    let encoded: Uint8Array;
    try {
      encoded = textEncoder.encode(JSON.stringify(message));
    } catch (error: unknown) {
      throw new MessageValidationError(
        `Resource message is not encodable JSON: ${asErrorMessage(error)}`,
      );
    }
    if (encoded.length > FIELDLINK_MAX_MESSAGE_BYTES) {
      throw new MessageValidationError(
        `Resource message exceeds ${FIELDLINK_MAX_MESSAGE_BYTES} encoded bytes`,
      );
    }
    return encoded;
  },
  decode(bytes: Uint8Array): ResourceMessage {
    if (bytes.length === 0 || bytes.length > FIELDLINK_MAX_MESSAGE_BYTES) {
      throw new MessageValidationError(
        `Resource message must be 1 to ${FIELDLINK_MAX_MESSAGE_BYTES} bytes`,
      );
    }
    let value: unknown;
    try {
      value = JSON.parse(textDecoder.decode(bytes)) as unknown;
    } catch (error: unknown) {
      throw new MessageValidationError(
        `Resource message is not valid UTF-8 JSON: ${asErrorMessage(error)}`,
      );
    }
    if (!this.validate(value)) {
      throw new MessageValidationError("Invalid Resource message");
    }
    return value;
  },
  exercise: {
    defaultPayloadBytes: 32,
    maximumPayloadBytes: FIELDLINK_MAX_MESSAGE_BYTES - exerciseEnvelopeBytes,
    payloadPresets: [32, 39, 4096],
    create(payloadBytes: number): ResourceMessage {
      return {
        type: "resource",
        kind: "response",
        request_id: takeExerciseRequestId(),
        status: 200,
        body: "x".repeat(payloadBytes),
      };
    },
    key(message): string {
      return message.request_id;
    },
    isComplete({ sent, received, side }): boolean {
      if (sent.kind === "request") {
        return (
          side === "source" &&
          received.kind === "response" &&
          received.request_id === sent.request_id
        );
      }
      return (
        side === "destination" &&
        received.kind === "response" &&
        received.request_id === sent.request_id &&
        received.status === sent.status &&
        received.body === sent.body
      );
    },
  },
} satisfies MessageDefinition<ResourceMessage>;

const MAX_CACHED_RESOURCE_REQUESTS = 64;

/**
 * Executes Resource requests from one preflight-approved radio and returns the
 * result across FieldLink. Repeated request IDs replay the first response;
 * reusing an ID for different JSON returns a conflict without touching Atlas.
 */
export function attachResourceRequestHandler(
  node: Pick<FieldLinkNode, "onMessage" | "send">,
  executor: ResourceRequestExecutor,
  allowedSource: NodeId,
): () => void {
  const cached = new Map<
    string,
    {
      readonly encodedRequest: string;
      readonly response: Promise<ResourceResponse>;
    }
  >();

  return node.onMessage(async (received: ReceivedMessage) => {
    const message = received.message;
    if (
      received.source !== allowedSource ||
      message.type !== "resource" ||
      message.kind !== "request"
    ) {
      return;
    }

    const key = `${received.source}:${message.request_id}`;
    const encodedRequest = JSON.stringify(message);
    const previous = cached.get(key);
    let response: Promise<ResourceResponse>;
    if (previous !== undefined) {
      response =
        previous.encodedRequest === encodedRequest
          ? previous.response
          : Promise.resolve({
              type: "resource",
              kind: "response",
              request_id: message.request_id,
              status: 409,
              body: { error: "request_id was already used for different JSON" },
            });
    } else {
      if (cached.size >= MAX_CACHED_RESOURCE_REQUESTS) {
        const oldest = cached.keys().next().value;
        if (oldest !== undefined) {
          cached.delete(oldest);
        }
      }
      response = executor.execute(message).catch(() => ({
        type: "resource",
        kind: "response",
        request_id: message.request_id,
        status: 500,
        body: { error: "Atlas Resource request failed" },
      }));
      cached.set(key, { encodedRequest, response });
    }

    await node.send(await response, { destination: received.source });
  });
}

function isResponse(value: Record<string, unknown>): boolean {
  return (
    hasShape(value, ["type", "kind", "request_id", "status"], ["body"]) &&
    isRequestId(value.request_id) &&
    typeof value.status === "number" &&
    Number.isInteger(value.status) &&
    value.status >= 100 &&
    value.status <= 599 &&
    (!Object.hasOwn(value, "body") || isJsonValue(value.body))
  );
}

function isListQuery(value: unknown): value is ResourceListQuery {
  return (
    isPlainRecord(value) &&
    hasShape(value, ["limit"], ["cursor"]) &&
    typeof value.limit === "number" &&
    Number.isSafeInteger(value.limit) &&
    value.limit > 0 &&
    value.limit <= 1000 &&
    (!Object.hasOwn(value, "cursor") || isNonEmptyString(value.cursor))
  );
}

function isJsonObject(value: unknown): value is JsonObject {
  return isPlainRecord(value) && isJsonValue(value);
}

export function isJsonValue(value: unknown): value is JsonValue {
  const pending = [value];
  const seen = new WeakSet<object>();
  while (pending.length > 0) {
    const current = pending.pop();
    if (
      current === null ||
      typeof current === "string" ||
      typeof current === "boolean"
    ) {
      continue;
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current)) {
        return false;
      }
      continue;
    }
    if (typeof current !== "object" || seen.has(current)) {
      return false;
    }
    seen.add(current);
    if (Array.isArray(current)) {
      const keys = Object.keys(current);
      if (
        Object.getOwnPropertySymbols(current).length > 0 ||
        keys.length !== current.length
      ) {
        return false;
      }
      for (let index = 0; index < current.length; index += 1) {
        if (keys[index] !== String(index)) {
          return false;
        }
        pending.push(current[index]);
      }
      continue;
    }
    if (!isPlainRecord(current)) {
      return false;
    }
    for (const key of Reflect.ownKeys(current)) {
      if (typeof key !== "string") {
        return false;
      }
      const descriptor = Object.getOwnPropertyDescriptor(current, key);
      if (
        descriptor === undefined ||
        !descriptor.enumerable ||
        !("value" in descriptor)
      ) {
        return false;
      }
      pending.push(descriptor.value);
    }
  }
  return true;
}

function isResourceType(value: unknown): value is ResourceType {
  return value === "entity" || value === "object" || value === "task";
}

function isMutableResourceType(value: unknown): value is "entity" | "object" {
  return value === "entity" || value === "object";
}

function isRequestId(value: unknown): value is string {
  return isNonEmptyString(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value) as unknown;
  return prototype === Object.prototype || prototype === null;
}

function hasShape(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): boolean {
  const allowed = new Set([...required, ...optional]);
  const keys = Reflect.ownKeys(value);
  return (
    required.every((key) => Object.hasOwn(value, key)) &&
    keys.every((key) => typeof key === "string" && allowed.has(key))
  );
}

function takeExerciseRequestId(): string {
  const requestId = nextExerciseRequestId;
  nextExerciseRequestId = (nextExerciseRequestId + 1) >>> 0;
  return `exercise-${requestId.toString(16).padStart(8, "0")}`;
}

function asErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
