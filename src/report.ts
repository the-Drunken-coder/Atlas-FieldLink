import { mkdir, open, writeFile, type FileHandle } from "node:fs/promises";
import { resolve } from "node:path";

import type { HardwareCommand } from "./args.js";

export interface RunManifest {
  readonly schemaVersion: 2;
  readonly command: HardwareCommand["name"];
  readonly startedAt: string;
  readonly radios: { readonly a: string; readonly b: string };
  readonly channel: number;
  readonly requestedCountPerPhase: number;
  readonly datagramBytes: number;
  readonly timeoutMs: number;
  readonly inboxDrainAccepted: true;
}

export interface ArtifactPaths {
  readonly directory: string;
  readonly manifest: string;
  readonly events: string;
  readonly summary: string;
}

export class RunArtifacts {
  readonly paths: ArtifactPaths;
  readonly #events: FileHandle;
  #writeTail: Promise<void> = Promise.resolve();
  #writeError: Error | undefined;
  #closed = false;

  private constructor(paths: ArtifactPaths, events: FileHandle) {
    this.paths = paths;
    this.#events = events;
  }

  static async create(
    manifest: RunManifest,
    requestedDirectory: string | undefined,
  ): Promise<RunArtifacts> {
    const timestamp = manifest.startedAt.replaceAll(":", "-");
    const directory = resolve(
      requestedDirectory ?? `results/${timestamp}-${manifest.command}`,
    );
    const paths: ArtifactPaths = {
      directory,
      manifest: resolve(directory, "manifest.json"),
      events: resolve(directory, "events.jsonl"),
      summary: resolve(directory, "summary.json"),
    };
    await mkdir(directory, { recursive: true });
    await writeFile(paths.manifest, `${stringify(manifest, 2)}\n`, {
      encoding: "utf8",
      flag: "wx",
    });
    const events = await open(paths.events, "wx");
    return new RunArtifacts(paths, events);
  }

  record(type: string, data: unknown): Promise<void> {
    if (this.#closed) {
      return Promise.reject(new Error("Run artifacts are already closed"));
    }
    const line = `${stringify({ at: new Date().toISOString(), type, data })}\n`;
    const write = this.#writeTail.then(() =>
      this.#events.appendFile(line, "utf8"),
    );
    this.#writeTail = write.then(
      () => undefined,
      (error: unknown) => {
        this.#writeError ??= asError(error);
      },
    );
    return write;
  }

  async finish(summary: unknown): Promise<void> {
    if (this.#closed) {
      throw new Error("Run artifacts are already closed");
    }
    this.#closed = true;
    const errors: Error[] = [];
    try {
      await this.#writeTail;
      await this.#events.sync();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    if (this.#writeError !== undefined) {
      errors.push(this.#writeError);
    }
    try {
      await this.#events.close();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    try {
      await writeFile(this.paths.summary, `${stringify(summary, 2)}\n`, {
        encoding: "utf8",
        flag: "wx",
      });
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, "Could not finish run artifacts");
    }
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function stringify(value: unknown, spaces?: number): string {
  return JSON.stringify(
    value,
    (_key, nested: unknown) => {
      if (nested instanceof Uint8Array) {
        return { hex: Buffer.from(nested).toString("hex") };
      }
      if (nested instanceof Error) {
        return { name: nested.name, message: nested.message };
      }
      return nested;
    },
    spaces,
  );
}
