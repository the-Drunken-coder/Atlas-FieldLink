import { mkdtemp, open, readFile, rm, type FileHandle } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import { RunArtifacts } from "../src/report.js";

const manifest = {
  schemaVersion: 2,
  command: "ping",
  startedAt: "2026-08-21T12:00:00.000Z",
  radios: { a: "/dev/a", b: "/dev/b" },
  channel: 1,
  requestedCountPerPhase: 1,
  datagramBytes: 16,
  timeoutMs: 100,
  inboxDrainAccepted: true,
} as const;

describe("run artifacts", () => {
  it("writes the manifest first, streams JSONL events, and finishes a summary", async () => {
    const temporaryDirectory = await mkdtemp(
      join(tmpdir(), "fieldlink-report-"),
    );
    const runDirectory = join(temporaryDirectory, "run");
    try {
      const artifacts = await RunArtifacts.create(manifest, runDirectory);

      const writtenManifest = JSON.parse(
        await readFile(artifacts.paths.manifest, "utf8"),
      ) as { command: string };
      expect(writtenManifest.command).toBe("ping");

      await artifacts.record("sample", {
        sequence: 1,
        bytes: new Uint8Array([1, 2]),
      });
      await artifacts.record("interrupted", { signal: "SIGINT" });
      await artifacts.finish({ status: "interrupted" });

      const eventLines = (await readFile(artifacts.paths.events, "utf8"))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { type: string; data: unknown });
      expect(eventLines).toHaveLength(2);
      expect(eventLines[0]).toMatchObject({
        type: "sample",
        data: { sequence: 1, bytes: { hex: "0102" } },
      });
      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toEqual({ status: "interrupted" });
    } finally {
      await rm(temporaryDirectory, { recursive: true });
    }
  });

  it("reports a queued event-write failure before the summary is constructed", async () => {
    const temporaryDirectory = await mkdtemp(
      join(tmpdir(), "fieldlink-report-"),
    );
    const probe = await open(join(temporaryDirectory, "probe"), "w");
    const fileHandlePrototype = Object.getPrototypeOf(probe) as {
      appendFile: FileHandle["appendFile"];
    };
    await probe.close();
    const appendFile = vi.spyOn(fileHandlePrototype, "appendFile");
    try {
      const artifacts = await RunArtifacts.create(
        manifest,
        join(temporaryDirectory, "run"),
      );
      appendFile.mockRejectedValueOnce(new Error("disk full"));

      await expect(artifacts.record("sample", { sequence: 1 })).rejects.toThrow(
        "disk full",
      );
      await artifacts.record("cleanup", { complete: true });
      await expect(artifacts.flush()).rejects.toThrow("disk full");
      await expect(
        artifacts.finish({ status: "failed", artifactError: "disk full" }),
      ).rejects.toThrow("Could not finish run artifacts");

      expect(
        JSON.parse(await readFile(artifacts.paths.summary, "utf8")),
      ).toEqual({ status: "failed", artifactError: "disk full" });
      expect(await readFile(artifacts.paths.events, "utf8")).toContain(
        '"type":"cleanup"',
      );
    } finally {
      appendFile.mockRestore();
      await rm(temporaryDirectory, { recursive: true });
    }
  });
});
