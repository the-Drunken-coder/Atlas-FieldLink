import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import type { HardwareCommand } from "./args.js";
import type { ChannelConfiguration } from "./radio.js";
import type { RoundTripResult } from "./runner.js";

export interface HardwareRunReport {
  readonly schemaVersion: 1;
  readonly command: HardwareCommand["name"];
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly radios: {
    readonly a: string;
    readonly b: string;
  };
  readonly channel: {
    readonly index: number;
    readonly name: string;
  };
  readonly requestedCount: number;
  readonly timeoutMs: number;
  readonly protocol: {
    readonly meshCoreDataType: number;
    readonly meshCoreDelivery: "flood";
    readonly datagramBytes: number;
    readonly fieldLinkHeaderBytes: number;
    readonly bodyBytes: number;
  };
  readonly result: RoundTripResult;
}

export function createReport(
  command: HardwareCommand,
  channel: ChannelConfiguration,
  startedAt: string,
  finishedAt: string,
  result: RoundTripResult,
  dataType: number,
  headerBytes: number,
): HardwareRunReport {
  return {
    schemaVersion: 1,
    command: command.name,
    startedAt,
    finishedAt,
    radios: {
      a: command.a,
      b: command.b,
    },
    channel: {
      index: channel.index,
      name: channel.name,
    },
    requestedCount: command.count,
    timeoutMs: command.timeoutMs,
    protocol: {
      meshCoreDataType: dataType,
      meshCoreDelivery: "flood",
      datagramBytes: result.datagramBytes,
      fieldLinkHeaderBytes: headerBytes,
      bodyBytes: result.bodyBytes,
    },
    result,
  };
}

export async function writeReport(
  report: HardwareRunReport,
  requestedPath: string | undefined,
): Promise<string> {
  const outputPath = resolve(requestedPath ?? defaultReportPath(report));
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  return outputPath;
}

function defaultReportPath(report: HardwareRunReport): string {
  const timestamp = report.startedAt.replaceAll(":", "-").replace(".000Z", "Z");
  return `results/${timestamp}-${report.command}.json`;
}
