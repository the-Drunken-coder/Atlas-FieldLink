import { mkdtemp, open, readFile, rm, type FileHandle } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import {
  AdapterEvidence,
  TestArtifacts,
  type TestManifest,
} from "../src/evidence.js";

const manifest: TestManifest = {
  command: "test",
  message: "test",
  startedAt: "2026-08-24T12:00:00.000Z",
  radios: { a: "/dev/cu.a", b: "/dev/cu.b" },
  channel: 1,
  input: { kind: "exercise", payloadSize: 64 },
  retryStrategy: "selective-window",
  timeoutMs: 1000,
  inboxDrainAccepted: true,
  execution: { adapterProcesses: 2, radiosPerAdapter: 1 },
};

describe("test evidence", () => {
  it("persists standalone adapter inbox evidence", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "fieldlink-evidence-"));
    try {
      const evidence = await AdapterEvidence.create(join(temporary, "adapter"));
      await evidence.record("inbox-message", { text: "preserved" });
      await evidence.close();

      expect(await readFile(evidence.paths.events, "utf8")).toContain(
        '"text":"preserved"',
      );
    } finally {
      await rm(temporary, { recursive: true });
    }
  });

  it("creates manifest, event stream, and running summary before radio work", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "fieldlink-evidence-"));
    try {
      const artifacts = await TestArtifacts.create(
        manifest,
        join(temporary, "run"),
      );
      expect(
        JSON.parse(await readFile(artifacts.paths.manifest, "utf8")),
      ).toEqual(manifest);
      expect(await readFile(artifacts.paths.events, "utf8")).toBe("");
      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toMatchObject({ status: "running" });
      await artifacts.record("fragment", {
        bytes: Uint8Array.of(1, 2, 3),
      });
      await artifacts.finish({ status: "interrupted", partial: true });
      expect(await readFile(artifacts.paths.events, "utf8")).toContain(
        '"base64":"AQID"',
      );
      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toEqual({ status: "interrupted", partial: true });
    } finally {
      await rm(temporary, { recursive: true });
    }
  });

  it("refuses to overwrite existing evidence", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "fieldlink-evidence-"));
    const directory = join(temporary, "run");
    try {
      const artifacts = await TestArtifacts.create(manifest, directory);
      await artifacts.finish({ status: "failed" });
      await expect(TestArtifacts.create(manifest, directory)).rejects.toThrow();
    } finally {
      await rm(temporary, { recursive: true });
    }
  });

  it("writes an honest summary after an event-stream failure", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "fieldlink-evidence-"));
    const probe = await open(join(temporary, "probe"), "w");
    const prototype = Object.getPrototypeOf(probe) as {
      appendFile: FileHandle["appendFile"];
    };
    await probe.close();
    const appendFile = vi
      .spyOn(prototype, "appendFile")
      .mockRejectedValueOnce(new Error("disk full"));
    try {
      const artifacts = await TestArtifacts.create(
        manifest,
        join(temporary, "run"),
      );
      await expect(artifacts.record("frame", {})).rejects.toThrow("disk full");
      await expect(
        artifacts.finish({ status: "failed", artifactError: "disk full" }),
      ).rejects.toThrow("Could not finish test artifacts");
      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toEqual({
        status: "failed",
        artifactError: "disk full",
        partial: true,
      });
    } finally {
      appendFile.mockRestore();
      await rm(temporary, { recursive: true });
    }
  });

  it("keeps the running summary when its atomic replacement fails", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "fieldlink-evidence-"));
    const probe = await open(join(temporary, "probe"), "w");
    const prototype = Object.getPrototypeOf(probe) as {
      writeFile: FileHandle["writeFile"];
    };
    await probe.close();
    const writeFile = vi
      .spyOn(prototype, "writeFile")
      .mockRejectedValueOnce(new Error("disk full"));
    try {
      const artifacts = await TestArtifacts.create(
        manifest,
        join(temporary, "run"),
      );
      await expect(artifacts.finish({ status: "failed" })).rejects.toThrow(
        "Could not finish test artifacts",
      );
      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toMatchObject({ status: "running" });
    } finally {
      writeFile.mockRestore();
      await rm(temporary, { recursive: true });
    }
  });

  it("downgrades a passing summary when event finalization fails", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "fieldlink-evidence-"));
    const probe = await open(join(temporary, "probe"), "w");
    const prototype = Object.getPrototypeOf(probe) as {
      sync: FileHandle["sync"];
    };
    await probe.close();
    const sync = vi
      .spyOn(prototype, "sync")
      .mockRejectedValueOnce(new Error("sync failed"));
    try {
      const artifacts = await TestArtifacts.create(
        manifest,
        join(temporary, "run"),
      );
      await expect(artifacts.finish({ status: "passed" })).rejects.toThrow(
        "Could not finish test artifacts",
      );
      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toEqual({
        status: "failed",
        partial: true,
        artifactError: "sync failed",
      });
    } finally {
      sync.mockRestore();
      await rm(temporary, { recursive: true });
    }
  });
});
