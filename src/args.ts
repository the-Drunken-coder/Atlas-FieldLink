import { parseArgs } from "node:util";

import {
  FIELDLINK_HEADER_BYTES,
  FIELDLINK_MAX_DATAGRAM_BYTES,
  FIELDLINK_PING_DATAGRAM_BYTES,
} from "./protocol.js";

const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_SAMPLE_COUNT = 10_000;

export interface ListCommand {
  readonly name: "list";
}

export interface HardwareCommand {
  readonly name: "ping" | "bench";
  readonly a: string;
  readonly b: string;
  readonly channel: number;
  readonly count: number;
  readonly payloadSize: number;
  readonly timeoutMs: number;
  readonly allowInboxDrain: true;
  readonly output?: string;
}

export type FieldLinkCommand = ListCommand | HardwareCommand;

export class UsageError extends Error {}

export function parseCommand(arguments_: readonly string[]): FieldLinkCommand {
  if (
    arguments_[0] === "radios" &&
    arguments_[1] === "list" &&
    arguments_.length === 2
  ) {
    return { name: "list" };
  }
  if (arguments_[0] === "ping" || arguments_[0] === "bench") {
    return parseHardwareCommand(arguments_[0], arguments_.slice(1));
  }
  throw new UsageError("Expected 'radios list', 'ping', or 'bench'");
}

function parseHardwareCommand(
  name: HardwareCommand["name"],
  arguments_: readonly string[],
): HardwareCommand {
  const config = {
    args: [...arguments_],
    allowPositionals: false,
    strict: true,
    options: {
      a: { type: "string" },
      b: { type: "string" },
      channel: { type: "string" },
      count: { type: "string" },
      "payload-size": { type: "string" },
      "timeout-ms": { type: "string", default: String(DEFAULT_TIMEOUT_MS) },
      "allow-inbox-drain": { type: "boolean", default: false },
      output: { type: "string" },
    },
  } as const;
  const parse = () => parseArgs(config);

  let parsed: ReturnType<typeof parse>;
  try {
    parsed = parse();
  } catch (error: unknown) {
    throw new UsageError(
      error instanceof Error ? error.message : String(error),
    );
  }

  const a = required(parsed.values.a, "--a");
  const b = required(parsed.values.b, "--b");
  if (!parsed.values["allow-inbox-drain"]) {
    throw new UsageError(
      "--allow-inbox-drain is required because Companion inbox messages are consumed during a run",
    );
  }
  if (a === b) {
    throw new UsageError("--a and --b must name different serial ports");
  }
  const channel = integer(
    required(parsed.values.channel, "--channel"),
    "--channel",
    0,
    7,
  );
  const count = integer(
    required(parsed.values.count, "--count"),
    "--count",
    1,
    MAX_SAMPLE_COUNT,
  );
  const timeoutMs = integer(
    required(parsed.values["timeout-ms"], "--timeout-ms"),
    "--timeout-ms",
    1,
    3_600_000,
  );

  let payloadSize = FIELDLINK_PING_DATAGRAM_BYTES;
  if (name === "bench") {
    payloadSize = integer(
      required(parsed.values["payload-size"], "--payload-size"),
      "--payload-size",
      FIELDLINK_HEADER_BYTES,
      FIELDLINK_MAX_DATAGRAM_BYTES,
    );
  } else if (parsed.values["payload-size"] !== undefined) {
    throw new UsageError("--payload-size is only valid for bench");
  }

  return {
    name,
    a,
    b,
    channel,
    count,
    payloadSize,
    timeoutMs,
    allowInboxDrain: true,
    ...(parsed.values.output === undefined
      ? {}
      : { output: parsed.values.output }),
  };
}

function required(value: string | undefined, option: string): string {
  if (value === undefined || value.length === 0) {
    throw new UsageError(`${option} is required`);
  }
  return value;
}

function integer(
  value: string,
  option: string,
  minimum: number,
  maximum: number,
): number {
  if (!/^\d+$/.test(value)) {
    throw new UsageError(`${option} must be an integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new UsageError(`${option} must be between ${minimum} and ${maximum}`);
  }
  return parsed;
}
