#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { join } from "node:path";
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

const ADAPTER_REQUEST_TIMEOUT_MARGIN_MS = 30_000;

const HELP = `Usage:
  fieldlink radios list [--json]
  fieldlink messages list [--json]
  fieldlink adapter --radio <port> --channel <index> --output <directory> --allow-inbox-drain
  fieldlink test --a <port> --b <port> [--channel auto|<index>] [--message <name>] [--payload-size <bytes> | --resource-request <json-file>] [--retry-strategy selective-window] [--timeout-ms <ms>] [--output <directory>] --allow-inbox-drain

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
  const definition = definitionForName(command.message);
  if (definition === undefined) {
    throw new Error(`Message ${command.message} disappeared from registry`);
  }
  const sent = await testInputMessage(command, definition);
  const startedAt = new Date().toISOString();
  const controller = new AbortController();
  let artifactSink: TestArtifacts | undefined = undefined;
  let artifactError: Error | undefined;
  const preserveEvidence = async (
    type: string,
    data: unknown,
  ): Promise<void> => {
    const sink = artifactSink;
    if (sink === undefined) {
      return;
    }
    try {
      await sink.record(type, data);
    } catch (error: unknown) {
      const failure = asError(error);
      artifactError ??= failure;
      const persistenceError = new Error(
        `Could not preserve test evidence: ${failure.message}`,
        { cause: failure },
      );
      if (!controller.signal.aborted) {
        controller.abort(persistenceError);
      }
      throw persistenceError;
    }
  };
  const record = (type: string, data: unknown): void => {
    void preserveEvidence(type, data).catch(() => undefined);
  };
  let interruptedBy: "SIGINT" | "SIGTERM" | undefined;
  let interruptionRecorded = false;
  let finalizing = false;
  const recordInterruption = (): void => {
    if (
      interruptionRecorded ||
      interruptedBy === undefined ||
      artifactSink === undefined
    ) {
      return;
    }
    interruptionRecorded = true;
    record("interrupted", { signal: interruptedBy });
  };
  const interrupt = (signal: "SIGINT" | "SIGTERM"): void => {
    if (finalizing) {
      process.stderr.write(
        `\n${signal}: artifact finalization already in progress\n`,
      );
      return;
    }
    interruptedBy ??= signal;
    recordInterruption();
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

  const artifacts = await TestArtifacts.create(
    {
      command: "test",
      message: command.message,
      startedAt,
      radios: { a: command.a, b: command.b },
      channel: command.channel,
      input: testEvidenceInput(command, sent),
      retryStrategy: command.retryStrategy,
      timeoutMs: command.timeoutMs,
      inboxDrainAccepted: command.allowInboxDrain,
      execution: { adapterProcesses: 2, radiosPerAdapter: 1 },
    },
    command.output,
  ).catch((error: unknown) => {
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigterm);
    throw error;
  });
  artifactSink = artifacts;
  recordInterruption();
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
  const diagnosticErrors: {
    readonly radio: "A" | "B";
    readonly type: "listener-error" | "protocol-error" | "transport-error";
    readonly message: string;
    readonly logicalId?: string;
  }[] = [];
  try {
    throwIfAborted(controller.signal);
    selectedChannel = await resolveTestChannel(
      command,
      controller.signal,
      record,
    );
    [a, b] = await startAdapterPair(
      command,
      selectedChannel,
      artifacts.paths.directory,
      controller.signal,
      record,
      preserveEvidence,
      (error) => {
        diagnosticErrors.push(error);
      },
    );
    verifyPreflight(a, b);
    await Promise.all([
      a.activate(controller.signal),
      b.activate(controller.signal),
    ]);
    record("ready", {
      a: adapterEvidence(a, command.a),
      b: adapterEvidence(b, command.b),
      channel: a.channel,
    });
    if (command.input.kind === "resource-request") {
      await b.enableResourceGateway(a.nodeId, controller.signal);
      record("resource-gateway-ready", {
        radio: "B",
        allowedSource: a.nodeId,
      });
    }
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
    record("exercise-complete", {
      message: command.message,
      sendResult,
      response: completion.response,
      completion: {
        side: completion.side,
        source: completion.received.source,
        delivery: completion.received.delivery,
        snrDb: completion.received.snrDb,
        logicalId: completion.received.logicalId,
      },
      durationMs,
      correlation: "matched",
      ...(completion.response.delivery === "transfer"
        ? { responseDigest: "verified" }
        : {}),
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

  finalizing = true;
  try {
    await artifacts.flush();
  } catch (error: unknown) {
    artifactError ??= asError(error);
  }
  const interrupted = interruptedBy !== undefined;
  const failed =
    runError !== undefined ||
    artifactError !== undefined ||
    cleanupErrors.length > 0 ||
    diagnosticErrors.length > 0 ||
    sendResult === undefined ||
    completion === undefined;
  const condition = interrupted
    ? "interrupted"
    : failed
      ? "failed"
      : (sendResult?.retransmissions ?? 0) > 0 ||
          (sendResult?.receiptRequestRetries ?? 0) > 0 ||
          (completion?.response.retransmissions ?? 0) > 0 ||
          (completion?.response.receiptRequestRetries ?? 0) > 0
        ? "recovered"
        : "clean";
  const summary = {
    command: "test",
    message: command.message,
    startedAt,
    finishedAt: new Date().toISOString(),
    status: interrupted ? "interrupted" : failed ? "failed" : "passed",
    condition,
    interrupted,
    ...(interrupted || failed ? { partial: true } : {}),
    ...(interruptedBy === undefined ? {} : { interruptedBy }),
    input: testEvidenceInput(command, sent),
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
          response: completion.response,
          verification: {
            correlation: "matched",
            ...(completion.response.delivery === "transfer"
              ? { responseDigest: "verified" }
              : {}),
            ...(completion.received.message.type === "resource" &&
            completion.received.message.kind === "response"
              ? { atlasStatus: completion.received.message.status }
              : {}),
          },
          ...(completion.received.message.type === "resource" &&
          completion.received.message.kind === "response"
            ? { resourceResponse: completion.received.message }
            : {}),
        }),
    ...(durationMs === undefined ? {} : { elapsedMs: durationMs }),
    ...(runError === undefined ? {} : { error: runError.message }),
    ...(artifactError === undefined
      ? {}
      : { artifactError: artifactError.message }),
    diagnosticErrors,
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
  if (
    !interrupted &&
    !failed &&
    durationMs !== undefined &&
    sendResult !== undefined
  ) {
    process.stdout.write(
      [
        `Test passed: ${command.message}, ${testInputDescription(command)}`,
        `Request delivery: ${sendResult.delivery}`,
        `MeshCore channel: ${selectedChannel}`,
        `Request fragments: ${sendResult.fragments}`,
        `Request retransmissions: ${sendResult.retransmissions}`,
        `Request receipt-request retries: ${sendResult.receiptRequestRetries}`,
        `Response fragments: ${completion?.response.fragments ?? "?"}`,
        `Response retransmissions: ${completion?.response.retransmissions ?? "?"}`,
        `Response receipt-request retries: ${completion?.response.receiptRequestRetries ?? "?"}`,
        `Condition: ${condition}`,
        `Elapsed: ${durationMs.toFixed(2)} ms`,
        `Artifacts: ${artifacts.paths.directory}`,
      ].join("\n") + "\n",
    );
  } else {
    process.stderr.write(
      `fieldlink: ${runError?.message ?? diagnosticErrors[0]?.message ?? "test failed"}\n`,
    );
    process.stderr.write(`Artifacts: ${artifacts.paths.directory}\n`);
  }
  return interrupted ? 130 : failed ? 1 : 0;
}

async function testInputMessage(
  command: TestCommand,
  definition: MessageDefinition<SupportedMessage>,
): Promise<SupportedMessage> {
  if (command.input.kind === "exercise") {
    return definition.exercise.create(command.input.payloadSize);
  }
  const message = definition.decode(await readFile(command.input.path));
  if (message.type !== "resource" || message.kind !== "request") {
    throw new Error(
      "--resource-request must contain one valid Resource request JSON object",
    );
  }
  return message;
}

function testInputDescription(command: TestCommand): string {
  return command.input.kind === "exercise"
    ? `${command.input.payloadSize} payload bytes`
    : `request JSON from ${command.input.path}`;
}

function testEvidenceInput(command: TestCommand, sent: SupportedMessage) {
  if (command.input.kind === "exercise") {
    return command.input;
  }
  if (sent.type !== "resource" || sent.kind !== "request") {
    throw new Error("Resource request evidence received an invalid message");
  }
  return { kind: "resource-request" as const, request: sent };
}

async function startAdapterPair(
  command: TestCommand,
  channel: number,
  artifactDirectory: string,
  signal: AbortSignal,
  record: (type: string, data: unknown) => void,
  preserveEvidence: (type: string, data: unknown) => Promise<void>,
  onDiagnosticError: (error: {
    readonly radio: "A" | "B";
    readonly type: "listener-error" | "protocol-error" | "transport-error";
    readonly message: string;
    readonly logicalId?: string;
  }) => void,
): Promise<readonly [AdapterProcessNode, AdapterProcessNode]> {
  const startupController = new AbortController();
  const startupSignal = AbortSignal.any([signal, startupController.signal]);
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
      evidenceDirectory: join(
        artifactDirectory,
        "adapters",
        label.toLowerCase(),
      ),
      allowInboxDrain: true,
      signal: startupSignal,
      requestTimeoutMs: command.timeoutMs + ADAPTER_REQUEST_TIMEOUT_MARGIN_MS,
      onInboxMessage: (message) =>
        preserveEvidence("inbox-message", { radio: label, message }),
      onListenerError: (error) => {
        record("listener-error", { radio: label, message: error.message });
        onDiagnosticError({
          radio: label,
          type: "listener-error",
          message: error.message,
        });
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
  const start = (label: "A" | "B", path: string): Promise<AdapterProcessNode> =>
    AdapterProcessNode.start(options(label, path)).catch((error: unknown) => {
      startupController.abort(asError(error));
      throw error;
    });
  const settled = await Promise.allSettled([
    start("A", command.a),
    start("B", command.b),
  ]);
  const [resultA, resultB] = settled;
  if (resultA.status === "fulfilled" && resultB.status === "fulfilled") {
    for (const [label, node] of [
      ["A", resultA.value],
      ["B", resultB.value],
    ] as const) {
      node.onEvent((event) => {
        record("node-event", { radio: label, event });
        if (
          (event.type === "protocol-error" ||
            event.type === "transport-error") &&
          typeof event.message === "string"
        ) {
          onDiagnosticError({
            radio: label,
            type: event.type,
            message: event.message,
            ...(typeof event.logicalId === "string"
              ? { logicalId: event.logicalId }
              : {}),
          });
        }
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
  readonly response: ExerciseResponseEvidence;
}

export interface ExerciseResponseEvidence {
  readonly logicalId: string;
  readonly delivery: "complete" | "transfer";
  readonly encodedBytes: number;
  readonly fragments: number;
  readonly retransmissions: number;
  readonly receiptRequests: number;
  readonly receiptRequestRetries: number;
  readonly receipts: number;
  readonly retryStrategy?: string;
  readonly durationMs?: number;
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
    const startedTransfers = new Map<
      string,
      {
        readonly at: string;
        readonly encodedBytes: number;
        readonly fragmentCount: number;
        readonly retryStrategy?: string;
      }
    >();
    const completedTransfers = new Map<
      string,
      {
        readonly at: string;
        readonly retransmissions: number;
        readonly receipts: number;
      }
    >();
    const receiptRequests = new Map<string, number>();
    const receiptRequestWindows = new Map<string, Set<string>>();
    const receiptRequestRetries = new Map<string, number>();
    const failedTransfers = new Map<string, Error>();
    const echoTransfers = new Set<string>();
    const expectedHandlerLogicalIds = new Set<string>();
    const exerciseKey = definition.exercise.key(sent);
    let matched: Omit<ExerciseCompletion, "response"> | undefined;
    let settled = false;

    const transferKey = (sender: NodeId, logicalId: string): string =>
      `${sender}:${logicalId}`;
    const expectedSender = (side: ExerciseCompletion["side"]): NodeId =>
      side === "source" ? destinationNode.nodeId : sourceNode.nodeId;
    const finish = (candidate: Omit<ExerciseCompletion, "response">): void => {
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
        const completed = completedTransfers.get(key);
        if (completed === undefined) {
          matched = candidate;
          return;
        }
        const started = startedTransfers.get(key);
        const encodedBytes =
          started?.encodedBytes ??
          definition.encode(candidate.received.message).length;
        settled = true;
        cleanup();
        resolve({
          ...candidate,
          response: {
            logicalId: candidate.received.logicalId,
            delivery: "transfer",
            encodedBytes,
            fragments:
              started?.fragmentCount ??
              Math.ceil(encodedBytes / TRANSFER_FRAGMENT_BYTES),
            retransmissions: completed.retransmissions,
            receiptRequests: receiptRequests.get(key) ?? 0,
            receiptRequestRetries: receiptRequestRetries.get(key) ?? 0,
            receipts: completed.receipts,
            ...(started?.retryStrategy === undefined
              ? {}
              : { retryStrategy: started.retryStrategy }),
            ...(started === undefined
              ? {}
              : {
                  durationMs: Math.max(
                    0,
                    Date.parse(completed.at) - Date.parse(started.at),
                  ),
                }),
          },
        });
        return;
      }
      settled = true;
      cleanup();
      resolve({
        ...candidate,
        response: {
          logicalId: candidate.received.logicalId,
          delivery: "complete",
          encodedBytes: definition.encode(candidate.received.message).length,
          fragments: 1,
          retransmissions: 0,
          receiptRequests: 0,
          receiptRequestRetries: 0,
          receipts: 0,
        },
      });
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
            if (
              side === "destination" &&
              definition.exercise.key(received.message) === exerciseKey
            ) {
              expectedHandlerLogicalIds.add(received.logicalId);
            }
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
            event.type === "transfer-started" &&
            typeof event.logicalId === "string" &&
            typeof event.encodedBytes === "number" &&
            typeof event.fragmentCount === "number"
          ) {
            startedTransfers.set(transferKey(node.nodeId, event.logicalId), {
              at: event.at,
              encodedBytes: event.encodedBytes,
              fragmentCount: event.fragmentCount,
              ...(typeof event.retryStrategy === "string"
                ? { retryStrategy: event.retryStrategy }
                : {}),
            });
          }
          if (
            event.type === "receipt-request-sent" &&
            typeof event.logicalId === "string" &&
            typeof event.windowStart === "number" &&
            typeof event.windowCount === "number"
          ) {
            const key = transferKey(node.nodeId, event.logicalId);
            receiptRequests.set(key, (receiptRequests.get(key) ?? 0) + 1);
            const window = `${event.windowStart}:${event.windowCount}`;
            const windows = receiptRequestWindows.get(key) ?? new Set<string>();
            if (windows.has(window)) {
              receiptRequestRetries.set(
                key,
                (receiptRequestRetries.get(key) ?? 0) + 1,
              );
            } else {
              windows.add(window);
              receiptRequestWindows.set(key, windows);
            }
          }
          if (
            node === destinationNode &&
            event.type === "protocol-error" &&
            typeof event.logicalId === "string" &&
            expectedHandlerLogicalIds.has(event.logicalId) &&
            typeof event.message === "string"
          ) {
            const eventMessage = event.message;
            const handlerPrefix = [
              "Message handler failed:",
              "Message listener failed:",
            ].find((prefix) => eventMessage.startsWith(prefix));
            if (handlerPrefix !== undefined) {
              settled = true;
              cleanup();
              reject(
                new Error(
                  `${handlerPrefix === "Message handler failed:" ? "Echo" : "Resource"} handler failed:${eventMessage.slice(handlerPrefix.length)}`,
                ),
              );
              return;
            }
          }
          if (
            node === destinationNode &&
            event.type === "transfer-started" &&
            typeof event.logicalId === "string" &&
            event.destination === sourceNode.nodeId &&
            event.exerciseKey === exerciseKey
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
            completedTransfers.set(key, {
              at: event.at,
              retransmissions:
                typeof event.retransmissions === "number"
                  ? event.retransmissions
                  : 0,
              receipts: typeof event.receipts === "number" ? event.receipts : 0,
            });
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
