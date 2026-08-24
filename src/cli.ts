#!/usr/bin/env node

import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

import {
  AdapterProcessNode,
  runAdapterProcess,
  type StartAdapterProcessOptions,
} from "./adapter-process.js";
import { parseCommand, UsageError, type TestCommand } from "./args.js";
import { TestArtifacts } from "./evidence.js";
import {
  COMPLETE_MESSAGE_BODY_BYTES,
  FIELDLINK_MAX_MESSAGE_BYTES,
  TRANSFER_FRAGMENT_BYTES,
} from "./frame.js";
import {
  definitionForName,
  messageRegistry,
  type MessageDefinition,
  type SupportedMessage,
} from "./messages/index.js";
import type {
  FieldLinkEvent,
  NodeId,
  ReceivedMessage,
  SendResult,
} from "./node.js";
import {
  listRadioPorts,
  MeshCoreTransport,
  safeChannelConfiguration,
  selectMatchingChannel,
  type RadioPort,
  type SafeChannelConfiguration,
  type SafeRadioIdentity,
} from "./radio.js";
import { retryStrategies } from "./retry-strategies/index.js";

const HELP = `Usage:
  fieldlink radios list [--json]
  fieldlink messages list [--json]
  fieldlink adapter --radio <port> --channel <index> --allow-inbox-drain
  fieldlink test --a <port> --b <port> [--channel auto|<index>] [--message <name>] [--payload-size <bytes>] [--retry-strategy selective-window] [--timeout-ms <ms>] [--output <directory>] --allow-inbox-drain

Defaults:
  --channel auto
  --message test
  --payload-size 64
  --retry-strategy selective-window
  --timeout-ms 1800000
`;

export async function main(arguments_: readonly string[]): Promise<number> {
  if (
    arguments_.length === 0 ||
    arguments_.includes("--help") ||
    arguments_.includes("-h")
  ) {
    process.stdout.write(HELP);
    return 0;
  }
  const command = parseCommand(arguments_);
  if (command.name === "adapter") {
    return runAdapterProcess(command);
  }
  if (command.name === "list-radios") {
    printPorts(await listRadioPorts(), command.json);
    return 0;
  }
  if (command.name === "list-messages") {
    printMessageCatalog(command.json);
    return 0;
  }
  return runHardwareTest(command);
}

async function runHardwareTest(command: TestCommand): Promise<number> {
  const startedAt = new Date().toISOString();
  const artifacts = await TestArtifacts.create(
    {
      command: "test",
      message: command.message,
      startedAt,
      radios: { a: command.a, b: command.b },
      channel: command.channel,
      payloadSize: command.payloadSize,
      retryStrategy: command.retryStrategy,
      timeoutMs: command.timeoutMs,
      inboxDrainAccepted: command.allowInboxDrain,
      execution: { adapterProcesses: 2, radiosPerAdapter: 1 },
    },
    command.output,
  );
  const controller = new AbortController();
  let artifactError: Error | undefined;
  const record = (type: string, data: unknown): void => {
    void artifacts.record(type, data).catch((error: unknown) => {
      const failure = asError(error);
      artifactError ??= failure;
      if (!controller.signal.aborted) {
        controller.abort(
          new Error(`Could not preserve test evidence: ${failure.message}`, {
            cause: failure,
          }),
        );
      }
    });
  };
  let interruptedBy: "SIGINT" | "SIGTERM" | undefined;
  let finalizing = false;
  const interrupt = (signal: "SIGINT" | "SIGTERM"): void => {
    if (finalizing) {
      process.stderr.write(
        `\n${signal}: artifact finalization already in progress\n`,
      );
      return;
    }
    interruptedBy ??= signal;
    record("interrupted", { signal });
    controller.abort(new Error(`Test interrupted by ${signal}`));
  };
  const onSigint = (): void => {
    interrupt("SIGINT");
  };
  const onSigterm = (): void => {
    interrupt("SIGTERM");
  };
  process.on("SIGINT", onSigint);
  process.on("SIGTERM", onSigterm);
  const timeout = setTimeout(() => {
    controller.abort(
      new Error(`Test exceeded the ${command.timeoutMs} ms overall timeout`),
    );
  }, command.timeoutMs);

  process.stderr.write(
    [
      "WARNING: use dedicated test radios only.",
      "FieldLink will drain both complete MeshCore Companion inboxes and preserve every item in events.jsonl.",
      `Preparing A=${command.a} and B=${command.b}`,
    ].join("\n") + "\n",
  );

  let a: AdapterProcessNode | undefined;
  let b: AdapterProcessNode | undefined;
  let sendResult: SendResult | undefined;
  let completion: ExerciseCompletion | undefined;
  let runError: Error | undefined;
  let durationMs: number | undefined;
  let selectedChannel: number | undefined;
  const cleanupErrors: string[] = [];
  try {
    selectedChannel = await resolveTestChannel(
      command,
      controller.signal,
      record,
    );
    [a, b] = await startAdapterPair(
      command,
      selectedChannel,
      controller.signal,
      record,
    );
    verifyPreflight(a, b);
    await Promise.all([a.activate(), b.activate()]);
    record("ready", {
      a: adapterEvidence(a, command.a),
      b: adapterEvidence(b, command.b),
      channel: a.channel,
    });
    const definition = definitionForName(command.message);
    if (definition === undefined) {
      throw new Error(`Message ${command.message} disappeared from registry`);
    }
    const sent = definition.exercise.create(command.payloadSize);
    const completionPromise = waitForExerciseCompletion(
      a,
      b,
      definition,
      sent,
      controller.signal,
    );
    const start = performance.now();
    try {
      [sendResult, completion] = await Promise.all([
        a.send(sent, {
          destination: b.nodeId,
          retryStrategy: command.retryStrategy,
          signal: controller.signal,
        }),
        completionPromise,
      ]);
    } finally {
      durationMs = performance.now() - start;
    }
    record("test-passed", {
      message: command.message,
      sendResult,
      completion: {
        side: completion.side,
        source: completion.received.source,
        delivery: completion.received.delivery,
        snrDb: completion.received.snrDb,
        logicalId: completion.received.logicalId,
      },
      durationMs,
      integrity: "matched",
    });
  } catch (error: unknown) {
    runError = asError(error);
    if (!controller.signal.aborted) {
      controller.abort(runError);
    }
    record("test-failed", { message: runError.message });
  } finally {
    clearTimeout(timeout);
    const adapters = [a, b].filter(
      (adapter): adapter is AdapterProcessNode => adapter !== undefined,
    );
    const closed = await Promise.allSettled(
      adapters.map((adapter) => adapter.close()),
    );
    for (const [index, result] of closed.entries()) {
      if (result.status === "fulfilled") {
        continue;
      }
      const adapter = adapters[index];
      const label = adapter === a ? "A" : "B";
      const message = `${label}: ${asError(result.reason).message}`;
      cleanupErrors.push(message);
      record("cleanup-error", { radio: label, message });
    }
  }

  try {
    await artifacts.flush();
  } catch (error: unknown) {
    artifactError ??= asError(error);
  }
  finalizing = true;
  const interrupted = interruptedBy !== undefined;
  const failed =
    runError !== undefined ||
    artifactError !== undefined ||
    cleanupErrors.length > 0 ||
    sendResult === undefined ||
    completion === undefined;
  const summary = {
    command: "test",
    message: command.message,
    startedAt,
    finishedAt: new Date().toISOString(),
    status: interrupted ? "interrupted" : failed ? "failed" : "passed",
    interrupted,
    ...(interruptedBy === undefined ? {} : { interruptedBy }),
    payloadSize: command.payloadSize,
    retryStrategy: command.retryStrategy,
    channelSelection: command.channel,
    ...(selectedChannel === undefined
      ? {}
      : {
          selectedChannel: a?.channel ??
            b?.channel ?? { index: selectedChannel },
        }),
    ...(a === undefined ? {} : { radioA: adapterEvidence(a, command.a) }),
    ...(b === undefined ? {} : { radioB: adapterEvidence(b, command.b) }),
    ...(sendResult === undefined ? {} : { request: sendResult }),
    ...(completion === undefined
      ? {}
      : {
          completion: {
            side: completion.side,
            source: completion.received.source,
            delivery: completion.received.delivery,
            snrDb: completion.received.snrDb,
            logicalId: completion.received.logicalId,
          },
          integrity: "matched",
        }),
    ...(durationMs === undefined ? {} : { elapsedMs: durationMs }),
    ...(runError === undefined ? {} : { error: runError.message }),
    ...(artifactError === undefined
      ? {}
      : { artifactError: artifactError.message }),
    cleanupErrors,
  };
  let finishError: Error | undefined;
  try {
    await artifacts.finish(summary);
  } catch (error: unknown) {
    finishError = asError(error);
  } finally {
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigterm);
  }
  if (finishError !== undefined) {
    process.stderr.write(`fieldlink: ${finishError.message}\n`);
    process.stderr.write(`Partial artifacts: ${artifacts.paths.directory}\n`);
    return interrupted ? 130 : 1;
  }
  if (!failed && durationMs !== undefined && sendResult !== undefined) {
    process.stdout.write(
      [
        `Test passed: ${command.message}, ${command.payloadSize} payload bytes`,
        `Request delivery: ${sendResult.delivery}`,
        `MeshCore channel: ${selectedChannel}`,
        `Fragments: ${sendResult.fragments}`,
        `Retransmissions: ${sendResult.retransmissions}`,
        `Elapsed: ${durationMs.toFixed(2)} ms`,
        `Artifacts: ${artifacts.paths.directory}`,
      ].join("\n") + "\n",
    );
  } else {
    process.stderr.write(`fieldlink: ${runError?.message ?? "test failed"}\n`);
    process.stderr.write(`Artifacts: ${artifacts.paths.directory}\n`);
  }
  return interrupted ? 130 : failed ? 1 : 0;
}

async function startAdapterPair(
  command: TestCommand,
  channel: number,
  signal: AbortSignal,
  record: (type: string, data: unknown) => void,
): Promise<readonly [AdapterProcessNode, AdapterProcessNode]> {
  const options = (
    label: "A" | "B",
    path: string,
  ): StartAdapterProcessOptions => {
    const stderr = linePrefixer(`[adapter ${label}] `, (line) => {
      process.stderr.write(line);
    });
    return {
      path,
      channel,
      allowInboxDrain: true,
      signal,
      onInboxMessage: (message) => {
        record("inbox-message", { radio: label, message });
      },
      onListenerError: (error) => {
        record("listener-error", { radio: label, message: error.message });
      },
      onStderr: (message) => {
        record("adapter-stderr", { radio: label, message });
        stderr.write(message);
      },
      onStderrEnd: () => {
        stderr.end();
      },
    };
  };
  const settled = await Promise.allSettled([
    AdapterProcessNode.start(options("A", command.a)),
    AdapterProcessNode.start(options("B", command.b)),
  ]);
  const [resultA, resultB] = settled;
  if (resultA.status === "fulfilled" && resultB.status === "fulfilled") {
    for (const [label, node] of [
      ["A", resultA.value],
      ["B", resultB.value],
    ] as const) {
      node.onEvent((event) => {
        record("node-event", { radio: label, event });
      });
      node.onMessage((message) => {
        record("message", { radio: label, message });
      });
    }
    return [resultA.value, resultB.value];
  }
  const started = settled.flatMap((result) =>
    result.status === "fulfilled" ? [result.value] : [],
  );
  const closed = await Promise.allSettled(started.map((node) => node.close()));
  const errors = settled.flatMap((result) =>
    result.status === "rejected" ? [asError(result.reason)] : [],
  );
  errors.push(
    ...closed.flatMap((result) =>
      result.status === "rejected" ? [asError(result.reason)] : [],
    ),
  );
  throw new AggregateError(errors, "Could not start both adapter processes");
}

async function resolveTestChannel(
  command: TestCommand,
  signal: AbortSignal,
  record: (type: string, data: unknown) => void,
): Promise<number> {
  if (command.channel !== "auto") {
    record("channel-selected", {
      mode: "manual",
      channel: { index: command.channel },
    });
    return command.channel;
  }

  record("channel-scan-started", { scope: "all-available" });
  const scans = await Promise.allSettled([
    inspectChannels(command.a, signal),
    inspectChannels(command.b, signal),
  ]);
  const errors = scans.flatMap((result) =>
    result.status === "rejected" ? [asError(result.reason)] : [],
  );
  const onlyScanError = errors[0];
  if (errors.length === 1 && onlyScanError !== undefined) {
    throw onlyScanError;
  }
  if (errors.length > 0) {
    throw new Error(
      `Could not inspect both radios' channels: ${errors.map((error) => error.message).join("; ")}`,
      {
        cause: new AggregateError(errors),
      },
    );
  }
  const [resultA, resultB] = scans;
  if (resultA.status !== "fulfilled" || resultB.status !== "fulfilled") {
    throw new Error("Channel inspection did not complete");
  }
  const a = resultA.value;
  const b = resultB.value;
  record("channel-scan", { radio: "A", channels: a });
  record("channel-scan", { radio: "B", channels: b });
  const channel = selectMatchingChannel(a, b);
  if (channel === undefined) {
    throw new Error(
      "No configured MeshCore channel matches by slot, name, and key on both radios",
    );
  }
  record("channel-selected", { mode: "automatic", channel });
  process.stderr.write(
    `Using shared MeshCore channel ${channel.index}: ${channel.name || "unnamed"}\n`,
  );
  return channel.index;
}

async function inspectChannels(
  path: string,
  signal: AbortSignal,
): Promise<readonly SafeChannelConfiguration[]> {
  throwIfAborted(signal);
  const transport = new MeshCoreTransport(path, { channel: 0 });
  const errors: Error[] = [];
  let channels: readonly SafeChannelConfiguration[] | undefined;
  try {
    await transport.open();
    channels = (await transport.getChannels()).map(safeChannelConfiguration);
  } catch (error: unknown) {
    errors.push(asError(error));
  }
  try {
    await transport.close();
  } catch (error: unknown) {
    errors.push(asError(error));
  }
  const onlyInspectionError = errors[0];
  if (errors.length === 1 && onlyInspectionError !== undefined) {
    throw new Error(
      `Could not inspect channels on ${path}: ${onlyInspectionError.message}`,
      {
        cause: onlyInspectionError,
      },
    );
  }
  if (errors.length > 0 || channels === undefined) {
    throw new Error(
      `Could not inspect channels on ${path}: ${errors.map((error) => error.message).join("; ")}`,
      {
        cause: new AggregateError(errors),
      },
    );
  }
  throwIfAborted(signal);
  return channels;
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new Error("Test aborted");
  }
}

function verifyPreflight(a: AdapterProcessNode, b: AdapterProcessNode): void {
  if (a.nodeId === b.nodeId) {
    throw new Error(
      "A and B report the same Node ID; select two distinct physical radios",
    );
  }
  verifyMatchingRadioSettings(a.identity, b.identity);
  verifyMatchingChannels(a.channel, b.channel);
}

function verifyMatchingRadioSettings(
  a: SafeRadioIdentity,
  b: SafeRadioIdentity,
): void {
  if (
    a.radio.frequency !== b.radio.frequency ||
    a.radio.bandwidth !== b.radio.bandwidth ||
    a.radio.spreadingFactor !== b.radio.spreadingFactor ||
    a.radio.codingRate !== b.radio.codingRate
  ) {
    throw new Error(
      "A and B use different LoRa frequency, bandwidth, spreading factor, or coding rate settings",
    );
  }
}

function verifyMatchingChannels(
  a: SafeChannelConfiguration,
  b: SafeChannelConfiguration,
): void {
  if (!a.configured || !b.configured) {
    throw new Error(`Channel ${a.index} is not configured on both radios`);
  }
  if (
    a.index !== b.index ||
    a.name !== b.name ||
    a.keyFingerprint !== b.keyFingerprint
  ) {
    throw new Error(
      `Channel ${a.index} differs between radios; configure the same channel before testing`,
    );
  }
}

export interface ExerciseCompletion {
  readonly side: "source" | "destination";
  readonly received: ReceivedMessage;
}

export interface ExerciseNode {
  readonly nodeId: NodeId;
  onMessage(
    listener: (message: ReceivedMessage) => void | Promise<void>,
  ): () => void;
  onEvent(
    listener: (event: FieldLinkEvent) => void | Promise<void>,
  ): () => void;
}

export function waitForExerciseCompletion(
  sourceNode: ExerciseNode,
  destinationNode: ExerciseNode,
  definition: MessageDefinition<SupportedMessage>,
  sent: SupportedMessage,
  signal: AbortSignal,
): Promise<ExerciseCompletion> {
  return new Promise<ExerciseCompletion>((resolve, reject) => {
    const subscriptions: (() => void)[] = [];
    const completedTransfers = new Set<string>();
    const failedTransfers = new Map<string, Error>();
    const echoTransfers = new Set<string>();
    let matched: ExerciseCompletion | undefined;
    let settled = false;

    const transferKey = (sender: NodeId, logicalId: string): string =>
      `${sender}:${logicalId}`;
    const expectedSender = (side: ExerciseCompletion["side"]): NodeId =>
      side === "source" ? destinationNode.nodeId : sourceNode.nodeId;
    const finish = (candidate: ExerciseCompletion): void => {
      if (settled) {
        return;
      }
      if (candidate.received.delivery === "transfer") {
        const key = transferKey(
          expectedSender(candidate.side),
          candidate.received.logicalId,
        );
        const failure = failedTransfers.get(key);
        if (failure !== undefined) {
          settled = true;
          cleanup();
          reject(failure);
          return;
        }
        if (!completedTransfers.has(key)) {
          matched = candidate;
          return;
        }
      }
      settled = true;
      cleanup();
      resolve(candidate);
    };
    const listenForMessage = (
      node: ExerciseNode,
      side: ExerciseCompletion["side"],
      expectedSource: NodeId,
    ): void => {
      subscriptions.push(
        node.onMessage((received) => {
          if (
            received.source !== expectedSource ||
            !definition.validate(received.message)
          ) {
            return;
          }
          let complete: boolean;
          try {
            complete = definition.exercise.isComplete({
              sent,
              received: received.message,
              side,
            });
          } catch (error: unknown) {
            settled = true;
            cleanup();
            reject(asError(error));
            return;
          }
          if (complete) {
            finish({ side, received });
          }
        }),
      );
    };
    const listenForTransfer = (node: ExerciseNode): void => {
      subscriptions.push(
        node.onEvent((event: FieldLinkEvent) => {
          if (
            node === destinationNode &&
            event.type === "transfer-started" &&
            typeof event.logicalId === "string" &&
            event.destination === sourceNode.nodeId
          ) {
            echoTransfers.add(transferKey(node.nodeId, event.logicalId));
            return;
          }
          if (
            (event.type !== "transfer-completed" &&
              event.type !== "transfer-failed") ||
            typeof event.logicalId !== "string"
          ) {
            return;
          }
          const key = transferKey(node.nodeId, event.logicalId);
          if (event.type === "transfer-completed") {
            completedTransfers.add(key);
          } else {
            const message =
              typeof event.error === "string" ? event.error : "unknown error";
            const failure = new Error(`Echo transfer failed: ${message}`);
            failedTransfers.set(key, failure);
            if (matched === undefined && echoTransfers.has(key)) {
              settled = true;
              cleanup();
              reject(failure);
              return;
            }
          }
          if (
            matched !== undefined &&
            key ===
              transferKey(
                expectedSender(matched.side),
                matched.received.logicalId,
              )
          ) {
            finish(matched);
          }
        }),
      );
    };
    const abort = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(
        signal.reason instanceof Error
          ? signal.reason
          : new Error("Test aborted"),
      );
    };
    const cleanup = (): void => {
      for (const unsubscribe of subscriptions.splice(0)) {
        unsubscribe();
      }
      signal.removeEventListener("abort", abort);
    };

    listenForMessage(sourceNode, "source", destinationNode.nodeId);
    listenForMessage(destinationNode, "destination", sourceNode.nodeId);
    listenForTransfer(sourceNode);
    listenForTransfer(destinationNode);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) {
      abort();
    }
  });
}

export interface MessageCatalog {
  readonly messages: readonly {
    readonly id: number;
    readonly name: string;
    readonly defaultPriority: string;
    readonly exercise: {
      readonly defaultPayloadBytes: number;
      readonly maximumPayloadBytes: number;
      readonly presets: readonly {
        readonly payloadBytes: number;
        readonly encodedBytes: number;
        readonly delivery: "complete" | "transfer";
        readonly fragments: number;
      }[];
    };
  }[];
  readonly retryStrategies: readonly {
    readonly id: number;
    readonly name: string;
  }[];
  readonly delivery: {
    readonly maximumEncodedMessageBytes: number;
    readonly maximumCompleteMessageBytes: number;
    readonly transferFragmentBytes: number;
  };
}

export function buildMessageCatalog(): MessageCatalog {
  return {
    messages: messageRegistry.map((definition) => ({
      id: definition.id,
      name: definition.name,
      defaultPriority: definition.defaultPriority,
      exercise: {
        defaultPayloadBytes: definition.exercise.defaultPayloadBytes,
        maximumPayloadBytes: definition.exercise.maximumPayloadBytes,
        presets: definition.exercise.payloadPresets.map((payloadBytes) => {
          const encodedBytes = definition.encode(
            definition.exercise.create(payloadBytes),
          ).length;
          return {
            payloadBytes,
            encodedBytes,
            delivery:
              encodedBytes <= COMPLETE_MESSAGE_BODY_BYTES
                ? "complete"
                : "transfer",
            fragments:
              encodedBytes <= COMPLETE_MESSAGE_BODY_BYTES
                ? 1
                : Math.ceil(encodedBytes / TRANSFER_FRAGMENT_BYTES),
          };
        }),
      },
    })),
    retryStrategies: retryStrategies.map(({ id, name }) => ({ id, name })),
    delivery: {
      maximumEncodedMessageBytes: FIELDLINK_MAX_MESSAGE_BYTES,
      maximumCompleteMessageBytes: COMPLETE_MESSAGE_BODY_BYTES,
      transferFragmentBytes: TRANSFER_FRAGMENT_BYTES,
    },
  };
}

function printMessageCatalog(json: boolean): void {
  const catalog = buildMessageCatalog();
  if (json) {
    process.stdout.write(`${JSON.stringify(catalog)}\n`);
    return;
  }
  process.stdout.write("ID\tNAME\tPRIORITY\tDEFAULT PAYLOAD\n");
  for (const message of catalog.messages) {
    process.stdout.write(
      `${message.id}\t${message.name}\t${message.defaultPriority}\t${message.exercise.defaultPayloadBytes}\n`,
    );
  }
}

function adapterEvidence(node: AdapterProcessNode, path: string) {
  return {
    processId: node.processId,
    path,
    nodeId: node.nodeId,
    identity: node.identity,
    channel: node.channel,
    delivery: node.delivery,
    supportedMessages: node.supportedMessages,
    retryStrategies: node.retryStrategies,
  };
}

function linePrefixer(prefix: string, output: (line: string) => void) {
  let pending = "";
  return {
    write(message: string): void {
      pending += message;
      let newline = pending.indexOf("\n");
      while (newline !== -1) {
        output(`${prefix}${pending.slice(0, newline + 1)}`);
        pending = pending.slice(newline + 1);
        newline = pending.indexOf("\n");
      }
    },
    end(): void {
      if (pending.length > 0) {
        output(`${prefix}${pending}\n`);
        pending = "";
      }
    },
  };
}

function printPorts(ports: readonly RadioPort[], json: boolean): void {
  if (json) {
    process.stdout.write(`${JSON.stringify(ports)}\n`);
    return;
  }
  if (ports.length === 0) {
    process.stdout.write("No USB serial radio candidates found.\n");
    return;
  }
  process.stdout.write(
    "CANDIDATE PATH\tMANUFACTURER\tSERIAL\tUSB VID:PID\tSTATUS\n",
  );
  for (const port of ports) {
    const usbId =
      port.vendorId === undefined && port.productId === undefined
        ? "-"
        : `${port.vendorId ?? "?"}:${port.productId ?? "?"}`;
    process.stdout.write(
      `${port.path}\t${port.manufacturer ?? "-"}\t${port.serialNumber ?? "-"}\t${usbId}\tunverified until MeshCore preflight\n`,
    );
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (error: unknown) {
    if (error instanceof UsageError) {
      process.stderr.write(`fieldlink: ${error.message}\n\n${HELP}`);
      process.exitCode = 2;
    } else {
      process.stderr.write(`fieldlink: ${asError(error).message}\n`);
      process.exitCode = 1;
    }
  }
}
