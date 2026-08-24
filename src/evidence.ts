import { mkdir, open, writeFile, type FileHandle } from "node:fs/promises";
import { resolve } from "node:path";

import type { TestCommand } from "./args.js";

export interface TestManifest {
  readonly command: "test";
  readonly message: string;
  readonly startedAt: string;
  readonly radios: { readonly a: string; readonly b: string };
  readonly channel: TestCommand["channel"];
  readonly payloadSize: number;
  readonly retryStrategy: TestCommand["retryStrategy"];
  readonly timeoutMs: number;
  readonly inboxDrainAccepted: true;
  readonly execution: {
    readonly adapterProcesses: 2;
    readonly radiosPerAdapter: 1;
  };
}

export interface ArtifactPaths {
  readonly directory: string;
  readonly manifest: string;
  readonly events: string;
  readonly summary: string;
}

export class TestArtifacts {
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
    manifest: TestManifest,
    requestedDirectory: string | undefined,
  ): Promise<TestArtifacts> {
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
    try {
      await writeFile(
        paths.summary,
        `${stringify(
          {
            command: "test",
            startedAt: manifest.startedAt,
            status: "running",
          },
          2,
        )}\n`,
        { encoding: "utf8", flag: "wx" },
      );
    } catch (error: unknown) {
      await events.close().catch(() => undefined);
      throw error;
    }
    return new TestArtifacts(paths, events);
  }

  record(type: string, data: unknown): Promise<void> {
    if (this.#closed) {
      return Promise.reject(new Error("Test artifacts are already closed"));
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

  async flush(): Promise<void> {
    let tail: Promise<void>;
    do {
      tail = this.#writeTail;
      await tail;
    } while (tail !== this.#writeTail);
    if (this.#writeError !== undefined) {
      throw this.#writeError;
    }
  }

  async finish(summary: unknown): Promise<void> {
    if (this.#closed) {
      throw new Error("Test artifacts are already closed");
    }
    this.#closed = true;
    const errors: Error[] = [];
    try {
      await this.flush();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    try {
      await this.#events.sync();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    try {
      await this.#events.close();
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    try {
      const finalSummary =
        errors.length === 0
          ? summary
          : failedFinalizationSummary(summary, errors);
      await writeFile(this.paths.summary, `${stringify(finalSummary, 2)}\n`, {
        encoding: "utf8",
        flag: "w",
      });
    } catch (error: unknown) {
      errors.push(asError(error));
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, "Could not finish test artifacts");
    }
  }
}

function failedFinalizationSummary(
  summary: unknown,
  errors: readonly Error[],
): Record<string, unknown> {
  const base = isRecord(summary) ? summary : { summary };
  const artifactError = [
    ...(typeof base.artifactError === "string" ? [base.artifactError] : []),
    ...errors.map((error) => error.message),
  ]
    .filter((message, index, messages) => messages.indexOf(message) === index)
    .join("; ");
  return {
    ...base,
    status: "failed",
    partial: true,
    artifactError,
  };
}

function stringify(value: unknown, spaces?: number): string {
  return JSON.stringify(
    value,
    (_key, nested: unknown) => {
      if (nested instanceof Uint8Array) {
        return { base64: Buffer.from(nested).toString("base64") };
      }
      if (nested instanceof Error) {
        return { name: nested.name, message: nested.message };
      }
      return nested;
    },
    spaces,
  );
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
