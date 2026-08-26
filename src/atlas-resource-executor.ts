import { access } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  isJsonValue,
  type JsonObject,
  type JsonValue,
  type ResourceRequest,
  type ResourceRequestExecutor,
  type ResourceResponse,
  type ResourceType,
} from "./messages/resource.js";

interface AtlasClientLike {
  handshake(options?: { readonly signal?: AbortSignal }): Promise<void>;
  readonly entities: AtlasMutableResourceClient;
  readonly objects: AtlasMutableResourceClient;
  readonly tasks: {
    get(
      id: string,
      options?: { readonly fresh?: boolean; readonly signal?: AbortSignal },
    ): Promise<unknown>;
  };
  readonly queries: {
    full(options: AtlasFullQueryOptions): Promise<unknown>;
  };
}

interface AtlasMutableResourceClient {
  get(
    id: string,
    options?: { readonly fresh?: boolean; readonly signal?: AbortSignal },
  ): Promise<unknown>;
  create(body: JsonObject): Promise<unknown>;
  update(id: string, body: JsonObject): Promise<unknown>;
  delete(id: string): Promise<void>;
}

interface AtlasFullQueryOptions {
  readonly entityLimit: number;
  readonly taskLimit: number;
  readonly objectLimit: number;
  readonly entityCursor?: string;
  readonly taskCursor?: string;
  readonly objectCursor?: string;
}

interface AtlasFullResponse {
  readonly entities: readonly unknown[];
  readonly tasks: readonly unknown[];
  readonly objects: readonly unknown[];
  readonly has_more_entities: boolean;
  readonly has_more_tasks: boolean;
  readonly has_more_objects: boolean;
  readonly next_entity_cursor?: string;
  readonly next_task_cursor?: string;
  readonly next_object_cursor?: string;
}

type AtlasClientConstructor = new (options: {
  readonly baseUrl: string;
  readonly apiKey: string;
  readonly sync: false;
}) => AtlasClientLike;

export class AtlasResourceExecutor implements ResourceRequestExecutor {
  readonly #client: AtlasClientLike;

  constructor(client: AtlasClientLike) {
    this.#client = client;
  }

  async execute(request: ResourceRequest): Promise<ResourceResponse> {
    try {
      return await this.#execute(request);
    } catch (error: unknown) {
      const apiError = atlasAPIError(error);
      return {
        type: "resource",
        kind: "response",
        request_id: request.request_id,
        status: apiError?.status ?? 502,
        body: {
          error:
            apiError?.message ??
            (error instanceof Error ? error.message : "Atlas request failed"),
          ...(apiError?.errorCode === undefined
            ? {}
            : { error_code: apiError.errorCode }),
        },
      };
    }
  }

  async #execute(request: ResourceRequest): Promise<ResourceResponse> {
    switch (request.operation) {
      case "create":
        return response(
          request.request_id,
          201,
          await this.#mutable(request.resource_type).create(request.body),
        );
      case "get":
        return response(
          request.request_id,
          200,
          await this.#read(request.resource_type, request.resource_id),
        );
      case "list":
        return response(
          request.request_id,
          200,
          await this.#list(
            request.resource_type,
            request.query.limit,
            request.query.cursor,
          ),
        );
      case "patch":
        return response(
          request.request_id,
          200,
          await this.#mutable(request.resource_type).update(
            request.resource_id,
            request.body,
          ),
        );
      case "delete":
        await this.#mutable(request.resource_type).delete(request.resource_id);
        return {
          type: "resource",
          kind: "response",
          request_id: request.request_id,
          status: 204,
        };
    }
  }

  #mutable(type: "entity" | "object"): AtlasMutableResourceClient {
    return type === "entity" ? this.#client.entities : this.#client.objects;
  }

  #read(type: ResourceType, id: string): Promise<unknown> {
    switch (type) {
      case "entity":
        return this.#client.entities.get(id, { fresh: true });
      case "object":
        return this.#client.objects.get(id, { fresh: true });
      case "task":
        return this.#client.tasks.get(id, { fresh: true });
    }
  }

  async #list(
    type: ResourceType,
    limit: number,
    cursor: string | undefined,
  ): Promise<JsonValue> {
    const options: AtlasFullQueryOptions = {
      entityLimit: type === "entity" ? limit : 1,
      taskLimit: type === "task" ? limit : 1,
      objectLimit: type === "object" ? limit : 1,
      ...(type === "entity" && cursor !== undefined
        ? { entityCursor: cursor }
        : {}),
      ...(type === "task" && cursor !== undefined
        ? { taskCursor: cursor }
        : {}),
      ...(type === "object" && cursor !== undefined
        ? { objectCursor: cursor }
        : {}),
    };
    const page = atlasFullResponse(await this.#client.queries.full(options));
    switch (type) {
      case "entity":
        return listBody(
          page.entities,
          page.has_more_entities,
          page.next_entity_cursor,
        );
      case "task":
        return listBody(page.tasks, page.has_more_tasks, page.next_task_cursor);
      case "object":
        return listBody(
          page.objects,
          page.has_more_objects,
          page.next_object_cursor,
        );
    }
  }
}

export async function createAtlasResourceExecutorFromEnvironment(
  signal?: AbortSignal,
): Promise<AtlasResourceExecutor> {
  loadDotEnv();
  const checkout = requiredEnvironment("ATLAS_MODERNIZATION_PATH");
  const baseUrl = requiredEnvironment("ATLAS_BASE_URL");
  const apiKey = requiredEnvironment("ATLAS_API_KEY");
  const sdkEntry = resolve(
    checkout,
    "atlas_sdk",
    "dist",
    "atlas_sdk",
    "src",
    "index.js",
  );
  try {
    await access(sdkEntry);
  } catch (error: unknown) {
    throw new Error(
      `Atlas SDK build not found at ${sdkEntry}. Run npm run build:sdk in ${checkout}.`,
      { cause: error },
    );
  }

  // TODO: Replace checkout loading with
  // `import { AtlasClient } from "@the-drunken-coder/atlas-sdk"` once published.
  const loaded: unknown = await import(pathToFileURL(sdkEntry).href);
  if (!isRecord(loaded) || typeof loaded.AtlasClient !== "function") {
    throw new Error(`Atlas SDK entry does not export AtlasClient: ${sdkEntry}`);
  }
  const AtlasClient = loaded.AtlasClient as AtlasClientConstructor;
  const client = new AtlasClient({ baseUrl, apiKey, sync: false });
  await client.handshake(signal === undefined ? undefined : { signal });
  return new AtlasResourceExecutor(client);
}

function response(
  requestId: string,
  status: number,
  value: unknown,
): ResourceResponse {
  if (!isJsonValue(value)) {
    throw new TypeError("Atlas SDK returned a non-JSON resource");
  }
  return {
    type: "resource",
    kind: "response",
    request_id: requestId,
    status,
    body: value,
  };
}

function listBody(
  items: readonly unknown[],
  hasMore: boolean,
  nextCursor: string | undefined,
): JsonValue {
  const value = {
    items,
    has_more: hasMore,
    ...(nextCursor === undefined ? {} : { next_cursor: nextCursor }),
  };
  if (!isJsonValue(value)) {
    throw new TypeError("Atlas SDK returned a non-JSON resource list");
  }
  return value;
}

function atlasFullResponse(value: unknown): AtlasFullResponse {
  if (!isAtlasFullResponse(value)) {
    throw new TypeError("Atlas SDK returned an invalid full-query response");
  }
  return value;
}

function isAtlasFullResponse(value: unknown): value is AtlasFullResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.entities) &&
    Array.isArray(value.tasks) &&
    Array.isArray(value.objects) &&
    typeof value.has_more_entities === "boolean" &&
    typeof value.has_more_tasks === "boolean" &&
    typeof value.has_more_objects === "boolean" &&
    optionalString(value.next_entity_cursor) &&
    optionalString(value.next_task_cursor) &&
    optionalString(value.next_object_cursor)
  );
}

function optionalString(value: unknown): boolean {
  return value === undefined || (typeof value === "string" && value.length > 0);
}

function atlasAPIError(error: unknown):
  | {
      readonly status: number;
      readonly message: string;
      readonly errorCode?: string;
    }
  | undefined {
  if (
    !(error instanceof Error) ||
    (error.name !== "AtlasAPIError" && error.name !== "ConflictError") ||
    !isRecord(error) ||
    typeof error.status !== "number" ||
    !Number.isInteger(error.status) ||
    error.status < 100 ||
    error.status > 599
  ) {
    return undefined;
  }
  return {
    status: error.status,
    message: error.message,
    ...(typeof error.errorCode === "string"
      ? { errorCode: error.errorCode }
      : {}),
  };
}

function loadDotEnv(): void {
  try {
    process.loadEnvFile(resolve(".env"));
  } catch (error: unknown) {
    if (!isRecord(error) || error.code !== "ENOENT") {
      throw error;
    }
  }
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required in .env or the adapter environment`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
