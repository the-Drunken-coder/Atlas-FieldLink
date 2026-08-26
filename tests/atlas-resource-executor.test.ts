import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import {
  AtlasResourceExecutor,
  createAtlasResourceExecutorFromEnvironment,
} from "../src/atlas-resource-executor.js";
import { FIELDLINK_MAX_MESSAGE_BYTES } from "../src/frame.js";

describe("Atlas Resource executor", () => {
  it("maps Resource CRUD operations onto the Atlas SDK", async () => {
    const client = atlasClient();
    client.entities.create.mockResolvedValue({ entity_id: "entity-1" });
    client.entities.get.mockResolvedValue({ entity_id: "entity-1" });
    client.entities.update.mockResolvedValue({
      entity_id: "entity-1",
      alias: "Updated",
    });
    const executor = new AtlasResourceExecutor(client);

    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "create",
        request_id: "create-1",
        resource_type: "entity",
        body: { entity_id: "entity-1", entity_type: "asset" },
      }),
    ).resolves.toMatchObject({ status: 201, body: { entity_id: "entity-1" } });
    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "get",
        request_id: "get-1",
        resource_type: "entity",
        resource_id: "entity-1",
      }),
    ).resolves.toMatchObject({ status: 200, body: { entity_id: "entity-1" } });
    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "patch",
        request_id: "patch-1",
        resource_type: "entity",
        resource_id: "entity-1",
        body: { alias: "Updated" },
      }),
    ).resolves.toMatchObject({ status: 200, body: { alias: "Updated" } });
    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "delete",
        request_id: "delete-1",
        resource_type: "entity",
        resource_id: "entity-1",
      }),
    ).resolves.toEqual({
      type: "resource",
      kind: "response",
      request_id: "delete-1",
      status: 204,
    });

    expect(client.entities.create).toHaveBeenCalledWith({
      entity_id: "entity-1",
      entity_type: "asset",
    });
    expect(client.entities.get).toHaveBeenCalledWith("entity-1", {
      fresh: true,
    });
    expect(client.entities.update).toHaveBeenCalledWith("entity-1", {
      alias: "Updated",
    });
    expect(client.entities.delete).toHaveBeenCalledWith("entity-1");
  });

  it("normalizes one requested Atlas resource list", async () => {
    const client = atlasClient();
    client.queries.full.mockResolvedValue({
      entities: [{ entity_id: "ignored" }],
      tasks: [{ task_id: "task-1" }],
      objects: [{ object_id: "ignored" }],
      has_more_entities: false,
      has_more_tasks: true,
      has_more_objects: false,
      next_task_cursor: "next-task",
      version: 42,
    });
    const executor = new AtlasResourceExecutor(client);

    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "list",
        request_id: "list-1",
        resource_type: "task",
        query: { limit: 25, cursor: "current-task" },
      }),
    ).resolves.toEqual({
      type: "resource",
      kind: "response",
      request_id: "list-1",
      status: 200,
      body: {
        items: [{ task_id: "task-1" }],
        has_more: true,
        next_cursor: "next-task",
      },
    });
    expect(client.queries.full).toHaveBeenCalledWith({
      entityLimit: 1,
      taskLimit: 25,
      objectLimit: 1,
      taskCursor: "current-task",
    });
  });

  it("returns an Atlas API error without throwing through the radio handler", async () => {
    const client = atlasClient();
    client.tasks.get.mockRejectedValue(
      Object.assign(new Error("Atlas request failed: 404 TASK_NOT_FOUND"), {
        name: "AtlasAPIError",
        status: 404,
        errorCode: "TASK_NOT_FOUND",
      }),
    );
    const executor = new AtlasResourceExecutor(client);

    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "get",
        request_id: "missing-1",
        resource_type: "task",
        resource_id: "missing",
      }),
    ).resolves.toEqual({
      type: "resource",
      kind: "response",
      request_id: "missing-1",
      status: 404,
      body: {
        error: "Atlas request failed: 404 TASK_NOT_FOUND",
        error_code: "TASK_NOT_FOUND",
      },
    });
  });

  it("returns a small error response when Atlas data exceeds FieldLink's bound", async () => {
    const client = atlasClient();
    client.tasks.get.mockResolvedValue("x".repeat(FIELDLINK_MAX_MESSAGE_BYTES));
    const executor = new AtlasResourceExecutor(client);

    const result = await executor.execute({
      type: "resource",
      kind: "request",
      operation: "get",
      request_id: "oversized-1",
      resource_type: "task",
      resource_id: "task-1",
    });
    expect(result).toMatchObject({
      type: "resource",
      kind: "response",
      request_id: "oversized-1",
      status: 502,
    });
    expect(JSON.stringify(result.body)).toContain("exceeds");
  });

  it("bounds oversized Atlas error details", async () => {
    const client = atlasClient();
    client.tasks.get.mockRejectedValue(
      new Error("x".repeat(FIELDLINK_MAX_MESSAGE_BYTES)),
    );
    const executor = new AtlasResourceExecutor(client);

    await expect(
      executor.execute({
        type: "resource",
        kind: "request",
        operation: "get",
        request_id: "error-1",
        resource_type: "task",
        resource_id: "task-1",
      }),
    ).resolves.toEqual({
      type: "resource",
      kind: "response",
      request_id: "error-1",
      status: 502,
      body: { error: "Atlas request failed" },
    });
  });

  it("loads the built SDK from the configured Atlas Modernization checkout", async () => {
    const checkout = await mkdtemp(join(tmpdir(), "fieldlink-atlas-sdk-"));
    const sdkDirectory = join(
      checkout,
      "atlas_sdk",
      "dist",
      "atlas_sdk",
      "src",
    );
    const previous = {
      checkout: process.env.ATLAS_MODERNIZATION_PATH,
      baseUrl: process.env.ATLAS_BASE_URL,
      apiKey: process.env.ATLAS_API_KEY,
    };
    try {
      await mkdir(sdkDirectory, { recursive: true });
      await writeFile(
        join(checkout, "package.json"),
        `${JSON.stringify({ type: "module" })}\n`,
      );
      await writeFile(
        join(sdkDirectory, "index.js"),
        `export class AtlasClient {
  constructor(options) {
    if (options.baseUrl !== "https://atlas.test" || options.apiKey !== "secret" || options.sync !== false) throw new Error("wrong options");
  }
  async handshake() {}
  entities = { get: async (id) => ({ entity_id: id }), create: async () => ({}), update: async () => ({}), delete: async () => {} };
  objects = { get: async (id) => ({ object_id: id }), create: async () => ({}), update: async () => ({}), delete: async () => {} };
  tasks = { get: async (id) => ({ task_id: id }) };
  queries = { full: async () => ({ entities: [], tasks: [], objects: [], has_more_entities: false, has_more_tasks: false, has_more_objects: false, version: 1 }) };
}\n`,
      );
      process.env.ATLAS_MODERNIZATION_PATH = checkout;
      process.env.ATLAS_BASE_URL = "https://atlas.test";
      process.env.ATLAS_API_KEY = "secret";

      const executor = await createAtlasResourceExecutorFromEnvironment();

      await expect(
        executor.execute({
          type: "resource",
          kind: "request",
          operation: "get",
          request_id: "get-1",
          resource_type: "entity",
          resource_id: "entity-1",
        }),
      ).resolves.toMatchObject({ body: { entity_id: "entity-1" } });
    } finally {
      restoreEnvironment("ATLAS_MODERNIZATION_PATH", previous.checkout);
      restoreEnvironment("ATLAS_BASE_URL", previous.baseUrl);
      restoreEnvironment("ATLAS_API_KEY", previous.apiKey);
      await rm(checkout, { recursive: true });
    }
  });
});

function atlasClient() {
  const mutable = () => ({
    get: vi.fn(),
    create: vi.fn(),
    update: vi.fn(),
    delete: vi.fn(() => Promise.resolve()),
  });
  return {
    handshake: vi.fn(() => Promise.resolve()),
    entities: mutable(),
    objects: mutable(),
    tasks: { get: vi.fn() },
    queries: { full: vi.fn() },
  };
}

function restoreEnvironment(name: string, value: string | undefined): void {
  if (value === undefined) {
    Reflect.deleteProperty(process.env, name);
  } else {
    process.env[name] = value;
  }
}
