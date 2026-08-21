import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { RunArtifacts } from "../src/report.js";

describe("run artifacts", () => {
  it("writes the manifest first, streams JSONL events, and finishes a summary", async () => {
    const temporaryDirectory = await mkdtemp(
      join(tmpdir(), "fieldlink-report-"),
    );
    const runDirectory = join(temporaryDirectory, "run");
    try {
      const artifacts = await RunArtifacts.create(
        {
          schemaVersion: 2,
          command: "ping",
          startedAt: "2026-08-21T12:00:00.000Z",
          radios: { a: "/dev/a", b: "/dev/b" },
          channel: 1,
          requestedCountPerPhase: 1,
          datagramBytes: 16,
          timeoutMs: 100,
          inboxDrainAccepted: true,
        },
        runDirectory,
      );

      const manifest = JSON.parse(
        await readFile(artifacts.paths.manifest, "utf8"),
      ) as { command: string };
      expect(manifest.command).toBe("ping");

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
});
