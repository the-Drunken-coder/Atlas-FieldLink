#!/usr/bin/env node

import { parseCommand, UsageError, type HardwareCommand } from "./args.js";
import { FIELDLINK_HEADER_BYTES } from "./protocol.js";
import {
  FIELDLINK_DATA_TYPE,
  MeshCoreRadio,
  listRadioPorts,
  type ChannelConfiguration,
  type RadioPort,
} from "./radio.js";
import { createReport, writeReport } from "./report.js";
import {
  runRoundTrips,
  type RoundTripResult,
  type RoundTripSample,
} from "./runner.js";

const HELP = `Usage:
  npm run fieldlink -- radios list
  npm run fieldlink -- ping --a <port> --b <port> --channel <0-7> --count <n>
  npm run fieldlink -- bench --a <port> --b <port> --channel <0-7> --count <n> --payload-size <12-163>

Options:
  --timeout-ms <ms>  Per-round-trip timeout. Default: 30000
  --output <path>    JSON result path. Default: results/<timestamp>-<command>.json
  --help             Show this help
`;

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
  if (command.name === "list") {
    printPorts(await listRadioPorts());
    return 0;
  }
  return runHardwareCommand(command);
}

async function runHardwareCommand(command: HardwareCommand): Promise<number> {
  const radioA = new MeshCoreRadio(command.a);
  const radioB = new MeshCoreRadio(command.b);
  const startedAt = new Date().toISOString();
  process.stderr.write(`Opening A=${command.a} and B=${command.b}\n`);

  let channel: ChannelConfiguration;
  let result: RoundTripResult;
  try {
    await Promise.all([radioA.open(), radioB.open()]);
    const [channelA, channelB] = await Promise.all([
      radioA.getChannel(command.channel),
      radioB.getChannel(command.channel),
    ]);
    verifyMatchingChannels(channelA, channelB);
    channel = channelA;

    process.stderr.write(
      `Testing channel ${channel.index} (${channel.name || "unnamed"}) with ${command.payloadSize}-byte datagrams\n`,
    );
    result = await runRoundTrips({
      a: radioA,
      b: radioB,
      channel: command.channel,
      count: command.count,
      datagramBytes: command.payloadSize,
      timeoutMs: command.timeoutMs,
      onSample: samplePrinter(command),
    });
  } finally {
    await Promise.allSettled([radioA.close(), radioB.close()]);
  }

  const report = createReport(
    command,
    channel,
    startedAt,
    new Date().toISOString(),
    result,
    FIELDLINK_DATA_TYPE,
    FIELDLINK_HEADER_BYTES,
  );
  const outputPath = await writeReport(report, command.output);
  printSummary(result, outputPath);
  return result.summary.failed === 0 ? 0 : 1;
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

function samplePrinter(
  command: HardwareCommand,
): (sample: RoundTripSample) => void {
  if (command.name === "ping") {
    return (sample) => {
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

  return (sample) => {
    const interval = Math.max(1, Math.floor(command.count / 10));
    if (sample.sequence % interval === 0 || sample.sequence === command.count) {
      process.stderr.write(`Progress ${sample.sequence}/${command.count}\n`);
    }
  };
}

function printSummary(result: RoundTripResult, outputPath: string): void {
  const { summary } = result;
  process.stdout.write(
    [
      `Completed: ${summary.completed}/${summary.attempted} (${summary.successPercent}%)`,
      `Duration: ${summary.durationMs} ms`,
      `Verified goodput: ${summary.verifiedGoodputBitsPerSecond} bit/s`,
      `RTT p50/p95: ${formatNumber(summary.rttMs?.p50)}/${formatNumber(summary.rttMs?.p95)} ms`,
      `Result: ${outputPath}`,
    ].join("\n") + "\n",
  );
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

try {
  process.exitCode = await main(process.argv.slice(2));
} catch (error: unknown) {
  if (error instanceof UsageError) {
    process.stderr.write(`fieldlink: ${error.message}\n\n${HELP}`);
    process.exitCode = 2;
  } else {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`fieldlink: ${message}\n`);
    process.exitCode = 1;
  }
}
