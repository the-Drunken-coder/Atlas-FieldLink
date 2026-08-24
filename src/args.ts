import { parseArgs } from "node:util";

import { FIELDLINK_MAX_MESSAGE_BYTES } from "./frame.js";
import {
  definitionForName,
  messageRegistry,
  type MessageName,
} from "./messages/index.js";
import type { RetryStrategyName } from "./retry-strategies/index.js";

const DEFAULT_TIMEOUT_MS = 30 * 60 * 1000;

export interface ListRadiosCommand {
  readonly name: "list-radios";
  readonly json: boolean;
}

export interface ListMessagesCommand {
  readonly name: "list-messages";
  readonly json: boolean;
}

export interface AdapterCommand {
  readonly name: "adapter";
  readonly radio: string;
  readonly channel: number;
  readonly allowInboxDrain: true;
}

export type ChannelSelection = number | "auto";

export interface TestCommand {
  readonly name: "test";
  readonly message: MessageName;
  readonly a: string;
  readonly b: string;
  readonly channel: ChannelSelection;
  readonly payloadSize: number;
  readonly retryStrategy: RetryStrategyName;
  readonly timeoutMs: number;
  readonly allowInboxDrain: true;
  readonly output?: string;
}

export type FieldLinkCommand =
  AdapterCommand | ListMessagesCommand | ListRadiosCommand | TestCommand;

export class UsageError extends Error {}

export function parseCommand(arguments_: readonly string[]): FieldLinkCommand {
  if (arguments_[0] === "radios" && arguments_[1] === "list") {
    return parseListCommand("list-radios", arguments_.slice(2));
  }
  if (arguments_[0] === "messages" && arguments_[1] === "list") {
    return parseListCommand("list-messages", arguments_.slice(2));
  }
  if (arguments_[0] === "adapter") {
    return parseAdapterCommand(arguments_.slice(1));
  }
  if (arguments_[0] === "test") {
    return parseTestCommand(arguments_.slice(1));
  }
  throw new UsageError(
    "Expected 'radios list', 'messages list', 'adapter', or 'test'",
  );
}

function parseListCommand(
  name: ListMessagesCommand["name"] | ListRadiosCommand["name"],
  arguments_: readonly string[],
): ListMessagesCommand | ListRadiosCommand {
  const parsed = parseOptions(arguments_, {
    json: { type: "boolean", default: false },
  });
  return { name, json: parsed.json };
}

function parseAdapterCommand(arguments_: readonly string[]): AdapterCommand {
  const parsed = parseOptions(arguments_, {
    radio: { type: "string" },
    channel: { type: "string" },
    "allow-inbox-drain": { type: "boolean", default: false },
  });
  if (!parsed["allow-inbox-drain"]) {
    throw inboxDrainError();
  }
  return {
    name: "adapter",
    radio: required(parsed.radio, "--radio"),
    channel: parseChannel(parsed.channel),
    allowInboxDrain: true,
  };
}

function parseTestCommand(arguments_: readonly string[]): TestCommand {
  const parsed = parseOptions(arguments_, {
    a: { type: "string" },
    b: { type: "string" },
    channel: { type: "string" },
    message: { type: "string", default: "test" },
    "payload-size": { type: "string" },
    "retry-strategy": { type: "string", default: "selective-window" },
    "timeout-ms": { type: "string", default: String(DEFAULT_TIMEOUT_MS) },
    output: { type: "string" },
    "allow-inbox-drain": { type: "boolean", default: false },
  });
  const a = required(parsed.a, "--a");
  const b = required(parsed.b, "--b");
  const message = required(parsed.message, "--message");
  const definition = definitionForName(message);
  if (definition === undefined) {
    throw new UsageError(
      `--message must be one of: ${messageRegistry.map((item) => item.name).join(", ")}`,
    );
  }
  if (a === b) {
    throw new UsageError("--a and --b must name different serial ports");
  }
  if (!parsed["allow-inbox-drain"]) {
    throw inboxDrainError();
  }
  const retryStrategy = required(parsed["retry-strategy"], "--retry-strategy");
  if (retryStrategy !== "selective-window") {
    throw new UsageError("--retry-strategy must be 'selective-window'");
  }
  return {
    name: "test",
    message: definition.name,
    a,
    b,
    channel: parseTestChannel(parsed.channel),
    payloadSize: integer(
      parsed["payload-size"] ?? String(definition.exercise.defaultPayloadBytes),
      "--payload-size",
      0,
      Math.min(
        FIELDLINK_MAX_MESSAGE_BYTES,
        definition.exercise.maximumPayloadBytes,
      ),
    ),
    retryStrategy,
    timeoutMs: integer(
      required(parsed["timeout-ms"], "--timeout-ms"),
      "--timeout-ms",
      1,
      24 * 60 * 60 * 1000,
    ),
    allowInboxDrain: true,
    ...(parsed.output === undefined ? {} : { output: parsed.output }),
  };
}

function parseOptions<
  Options extends Record<
    string,
    { readonly type: "string" | "boolean"; readonly default?: string | boolean }
  >,
>(arguments_: readonly string[], options: Options) {
  try {
    return parseArgs({
      args: [...arguments_],
      allowPositionals: false,
      strict: true,
      options,
    }).values;
  } catch (error: unknown) {
    throw new UsageError(
      error instanceof Error ? error.message : String(error),
    );
  }
}

function parseChannel(value: string | undefined): number {
  return integer(required(value, "--channel"), "--channel", 0, 255);
}

function parseTestChannel(value: string | undefined): ChannelSelection {
  return value === undefined || value === "auto" ? "auto" : parseChannel(value);
}

function inboxDrainError(): UsageError {
  return new UsageError(
    "--allow-inbox-drain is required because the complete Companion inbox is consumed while FieldLink runs",
  );
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
