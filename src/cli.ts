#!/usr/bin/env node

import {
  AdapterProcessRadio,
  runAdapterProcess,
  type StartAdapterProcessOptions,
} from "./adapter-process.js";
import { parseCommand, UsageError, type HardwareCommand } from "./args.js";
import { FIELDLINK_HEADER_BYTES } from "./protocol.js";
import {
  FIELDLINK_DATA_TYPE,
  listRadioPorts,
  type ChannelConfiguration,
  type InboxMessage,
  type RadioIdentity,
  type RadioPort,
} from "./radio.js";
import { RunArtifacts } from "./report.js";
import { createRunSignals } from "./run-signals.js";
import {
  runDirectionalBench,
  runRoundTrips,
  type DirectionalBenchResult,
  type DirectionalSample,
  type RoundTripResult,
  type RoundTripSample,
} from "./runner.js";

const HELP = `Usage:
  npm run fieldlink -- radios list
  npm run fieldlink -- adapter --radio <port> --channel <0-7> --allow-inbox-drain
  npm run fieldlink -- ping --a <port> --b <port> --channel <0-7> --count <n> --allow-inbox-drain
  npm run fieldlink -- bench --a <port> --b <port> --channel <0-7> --count <n> --payload-size <12-163> --allow-inbox-drain

Options:
  --radio <port>      Serial radio owned by one adapter process
  --allow-inbox-drain  Required acknowledgement that both Companion inboxes are consumed
  --timeout-ms <ms>   Full send-and-delivery deadline per sample. Default: 30000
  --output <path>     Artifact directory. Default: results/<timestamp>-<command>/
  --help              Show this help
`;

type HardwareResult = RoundTripResult | DirectionalBenchResult;

async function main(arguments_: readonly string[]): Promise<number> {
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
  if (command.name === "list") {
    printPorts(await listRadioPorts());
    return 0;
  }
  return runHardwareCommand(command);
}

async function runHardwareCommand(command: HardwareCommand): Promise<number> {
  const startedAt = new Date().toISOString();
  const artifacts = await RunArtifacts.create(
    {
      schemaVersion: 3,
      command: command.name,
      startedAt,
      radios: { a: command.a, b: command.b },
      channel: command.channel,
      requestedCountPerPhase: command.count,
      datagramBytes: command.payloadSize,
      timeoutMs: command.timeoutMs,
      inboxDrainAccepted: command.allowInboxDrain,
      execution: {
        adapterProcesses: 2,
        radiosPerAdapter: 1,
      },
    },
    command.output,
  );
  let artifactError: Error | undefined;
  const record = (type: string, data: unknown): void => {
    void artifacts.record(type, data).catch((error: unknown) => {
      artifactError ??= asError(error);
    });
  };
  const signals = createRunSignals(record, (message) => {
    process.stderr.write(message);
  });
  const onSigint = (): void => {
    signals.handle("SIGINT");
  };
  const onSigterm = (): void => {
    signals.handle("SIGTERM");
  };
  process.on("SIGINT", onSigint);
  process.on("SIGTERM", onSigterm);

  process.stderr.write(
    [
      "WARNING: use dedicated test radios only.",
      "FieldLink must drain both shared MeshCore Companion inboxes; every consumed message is preserved in events.jsonl.",
      `Starting one adapter process for A=${command.a} and one for B=${command.b}`,
    ].join("\n") + "\n",
  );

  let radioA: AdapterProcessRadio | undefined;
  let radioB: AdapterProcessRadio | undefined;
  let identityA: RadioIdentity | undefined;
  let identityB: RadioIdentity | undefined;
  let channel: ChannelConfiguration | undefined;
  let result: HardwareResult | undefined;
  let runError: Error | undefined;
  const cleanupErrors: string[] = [];

  try {
    [radioA, radioB] = await startAdapterPair(command, record);
    identityA = radioA.identity;
    identityB = radioB.identity;
    const channelA = radioA.channelConfiguration;
    const channelB = radioB.channelConfiguration;
    verifyDistinctMatchingRadios(identityA, identityB);
    verifyMatchingChannels(channelA, channelB);
    channel = channelA;
    record("radio-identity", {
      a: safeIdentity(identityA),
      b: safeIdentity(identityB),
    });
    record("adapter-process", {
      a: { processId: radioA.processId, radio: command.a },
      b: { processId: radioB.processId, radio: command.b },
    });
    record("channel", { index: channel.index, name: channel.name });

    process.stderr.write(
      `Testing channel ${channel.index} (${channel.name || "unnamed"}) with ${command.payloadSize}-byte datagrams\n`,
    );
    const common = {
      a: radioA,
      b: radioB,
      channel: command.channel,
      count: command.count,
      datagramBytes: command.payloadSize,
      timeoutMs: command.timeoutMs,
      signal: signals.signal,
      onAnomaly: (anomaly: unknown) => {
        record("anomaly", anomaly);
      },
      onError: (message: string) => {
        record("runner-error", { message });
      },
    };
    if (command.name === "ping") {
      result = await runRoundTrips({
        ...common,
        onSample: pingSampleRecorder(record),
      });
    } else {
      result = await runDirectionalBench({
        ...common,
        onSample: benchSampleRecorder(command, record),
      });
    }
  } catch (error: unknown) {
    runError = asError(error);
    record("run-error", runError);
  } finally {
    const adapters = [radioA, radioB].filter(
      (adapter): adapter is AdapterProcessRadio => adapter !== undefined,
    );
    const closed = await Promise.allSettled(
      adapters.map((adapter) => adapter.close()),
    );
    for (const [index, closeResult] of closed.entries()) {
      if (closeResult.status === "rejected") {
        const adapter = adapters[index];
        const label = adapter === radioA ? "A" : "B";
        const message = `${label}: ${asError(closeResult.reason).message}`;
        cleanupErrors.push(message);
        record("cleanup-error", { radio: label, message });
      }
    }
  }

  try {
    await artifacts.flush();
  } catch (error: unknown) {
    artifactError ??= asError(error);
  }
  signals.beginFinalization();

  const interrupted = signals.signal.aborted;
  const failed =
    runError !== undefined ||
    artifactError !== undefined ||
    cleanupErrors.length > 0 ||
    result === undefined ||
    resultFailed(result);
  const summary = {
    schemaVersion: 3,
    command: command.name,
    startedAt,
    finishedAt: new Date().toISOString(),
    status: interrupted ? "interrupted" : failed ? "failed" : "passed",
    interrupted,
    ...(identityA === undefined ? {} : { radioA: safeIdentity(identityA) }),
    ...(identityB === undefined ? {} : { radioB: safeIdentity(identityB) }),
    ...(channel === undefined
      ? {}
      : { channel: { index: channel.index, name: channel.name } }),
    ...(radioA === undefined || radioB === undefined
      ? {}
      : {
          execution: {
            adapterProcesses: 2,
            radiosPerAdapter: 1,
            processIds: { a: radioA.processId, b: radioB.processId },
          },
        }),
    protocol: {
      meshCoreDataType: FIELDLINK_DATA_TYPE,
      meshCoreDelivery: "flood",
      datagramBytes: command.payloadSize,
      fieldLinkHeaderBytes: FIELDLINK_HEADER_BYTES,
      applicationBodyBytes: command.payloadSize - FIELDLINK_HEADER_BYTES,
    },
    ...(result === undefined ? {} : { result }),
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

  if (result !== undefined) {
    printSummary(result, artifacts.paths.directory);
  } else {
    process.stderr.write(
      `fieldlink: ${runError?.message ?? "run did not produce a result"}\n`,
    );
    process.stderr.write(`Artifacts: ${artifacts.paths.directory}\n`);
  }
  if (interrupted) {
    return 130;
  }
  return failed ? 1 : 0;
}

async function startAdapterPair(
  command: HardwareCommand,
  record: (type: string, data: unknown) => void,
): Promise<readonly [AdapterProcessRadio, AdapterProcessRadio]> {
  const options = (
    label: "A" | "B",
    path: string,
  ): StartAdapterProcessOptions => ({
    path,
    channel: command.channel,
    allowInboxDrain: command.allowInboxDrain,
    onInboxMessage: (message: InboxMessage) => {
      record("inbox-message", { radio: label, message });
    },
    onListenerError: (error) => {
      record("listener-error", { radio: label, error });
    },
    onStderr: (message) => {
      record("adapter-stderr", { radio: label, message });
      process.stderr.write(`[adapter ${label}] ${message}`);
    },
  });
  const [a, b] = await Promise.allSettled([
    AdapterProcessRadio.start(options("A", command.a)),
    AdapterProcessRadio.start(options("B", command.b)),
  ]);
  if (a.status === "fulfilled" && b.status === "fulfilled") {
    return [a.value, b.value];
  }
  const started = [a, b].flatMap((result) =>
    result.status === "fulfilled" ? [result.value] : [],
  );
  const stopped = await Promise.allSettled(
    started.map((adapter) => adapter.close()),
  );
  const errors = [a, b].flatMap((result) =>
    result.status === "rejected" ? [asError(result.reason)] : [],
  );
  errors.push(
    ...stopped.flatMap((result) =>
      result.status === "rejected" ? [asError(result.reason)] : [],
    ),
  );
  throw new AggregateError(
    errors,
    `Could not start both adapter processes: ${errors.map((error) => error.message).join("; ")}`,
  );
}

function verifyDistinctMatchingRadios(
  identityA: RadioIdentity,
  identityB: RadioIdentity,
): void {
  if (Buffer.from(identityA.publicKey).equals(identityB.publicKey)) {
    throw new Error(
      "A and B report the same MeshCore public key; select two distinct physical radios",
    );
  }
  const a = identityA.radio;
  const b = identityB.radio;
  if (
    a.frequency !== b.frequency ||
    a.bandwidth !== b.bandwidth ||
    a.spreadingFactor !== b.spreadingFactor ||
    a.codingRate !== b.codingRate
  ) {
    throw new Error(
      "A and B use different LoRa frequency, bandwidth, spreading factor, or coding rate settings",
    );
  }
}

function verifyMatchingChannels(
  channelA: ChannelConfiguration,
  channelB: ChannelConfiguration,
): void {
  if (
    channelA.secret.every((byte) => byte === 0) ||
    channelB.secret.every((byte) => byte === 0)
  ) {
    throw new Error(
      `Channel ${channelA.index} is not configured on both radios`,
    );
  }
  if (
    channelA.name !== channelB.name ||
    !Buffer.from(channelA.secret).equals(channelB.secret)
  ) {
    throw new Error(
      `Channel ${channelA.index} differs between radios; configure the same channel before testing`,
    );
  }
}

function pingSampleRecorder(
  record: (type: string, data: unknown) => void,
): (sample: RoundTripSample) => void {
  return (sample) => {
    record("sample", sample);
    if (sample.status === "ok") {
      process.stdout.write(
        `seq=${sample.sequence} rtt=${formatNumber(sample.rttMs)}ms forward_snr=${formatNumber(sample.forwardSnrDb)}dB return_snr=${formatNumber(sample.returnSnrDb)}dB\n`,
      );
    } else {
      process.stdout.write(
        `seq=${sample.sequence} status=${sample.status}${sample.error === undefined ? "" : ` error=${sample.error}`}\n`,
      );
    }
  };
}

function benchSampleRecorder(
  command: HardwareCommand,
  record: (type: string, data: unknown) => void,
): (sample: DirectionalSample) => void {
  return (sample) => {
    record("sample", sample);
    const interval = Math.max(1, Math.floor(command.count / 10));
    if (sample.sequence % interval === 0 || sample.sequence === command.count) {
      process.stderr.write(
        `Progress ${sample.direction} ${sample.sequence}/${command.count}\n`,
      );
    }
  };
}

function resultFailed(result: HardwareResult): boolean {
  if (result.kind === "round-trip") {
    return result.summary.failed > 0 || result.summary.anomalyTotal > 0;
  }
  return result.phases.some(
    (phase) => phase.failed > 0 || phase.anomalyTotal > 0,
  );
}

function printSummary(result: HardwareResult, outputDirectory: string): void {
  if (result.kind === "round-trip") {
    const { summary } = result;
    process.stdout.write(
      [
        `Completed: ${summary.completed}/${summary.attempted} (${summary.successPercent}%)`,
        `Duration: ${summary.durationMs} ms`,
        `Application goodput: ${summary.applicationGoodputBitsPerSecond} bit/s`,
        `Mesh datagram bitrate: ${summary.meshDatagramBitsPerSecond} bit/s`,
        `RTT p50/p95/p99: ${formatNumber(summary.rttMs?.p50)}/${formatNumber(summary.rttMs?.p95)}/${formatNumber(summary.rttMs?.p99)} ms`,
        `Anomalies: ${summary.anomalyTotal}`,
        `Artifacts: ${outputDirectory}`,
      ].join("\n") + "\n",
    );
    return;
  }
  for (const phase of result.phases) {
    process.stdout.write(
      `${phase.direction}: ${phase.delivered}/${phase.attempted} (${phase.successPercent}%), app_goodput=${phase.applicationGoodputBitsPerSecond} bit/s, mesh_bitrate=${phase.meshDatagramBitsPerSecond} bit/s, one_way_p95=${formatNumber(phase.oneWayLatencyMs?.p95)} ms, anomalies=${phase.anomalyTotal}\n`,
    );
  }
  process.stdout.write(`Artifacts: ${outputDirectory}\n`);
}

function safeIdentity(
  identity: RadioIdentity,
): Omit<RadioIdentity, "publicKey"> {
  return {
    fingerprint: identity.fingerprint,
    name: identity.name,
    model: identity.model,
    firmwareVersion: identity.firmwareVersion,
    firmwareBuildDate: identity.firmwareBuildDate,
    firmwareProtocolCode: identity.firmwareProtocolCode,
    clientProtocolVersion: identity.clientProtocolVersion,
    radio: identity.radio,
  };
}

function printPorts(ports: readonly RadioPort[]): void {
  if (ports.length === 0) {
    process.stdout.write("No serial ports found.\n");
    return;
  }
  process.stdout.write("PATH\tMANUFACTURER\tSERIAL\tUSB VID:PID\n");
  for (const port of ports) {
    const usbId =
      port.vendorId === undefined && port.productId === undefined
        ? "-"
        : `${port.vendorId ?? "?"}:${port.productId ?? "?"}`;
    process.stdout.write(
      `${port.path}\t${port.manufacturer ?? "-"}\t${port.serialNumber ?? "-"}\t${usbId}\n`,
    );
  }
}

function formatNumber(value: number | undefined): string {
  return value === undefined ? "n/a" : value.toFixed(2);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

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
