import { randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";

import {
  DatagramKind,
  FIELDLINK_HEADER_BYTES,
  decodeDatagram,
  encodeDatagram,
  verifyDatagram,
} from "./protocol.js";
import {
  FIELDLINK_DATA_TYPE,
  type ChannelDatagram,
  type DatagramRadio,
} from "./radio.js";

export type SampleStatus = "ok" | "timeout" | "send-error" | "corrupt";

export interface RoundTripSample {
  readonly sequence: number;
  readonly status: SampleStatus;
  readonly rttMs?: number;
  readonly forwardSnrDb?: number;
  readonly returnSnrDb?: number;
  readonly error?: string;
}

export interface Distribution {
  readonly min: number;
  readonly mean: number;
  readonly p50: number;
  readonly p95: number;
  readonly max: number;
}

export interface RoundTripSummary {
  readonly attempted: number;
  readonly completed: number;
  readonly failed: number;
  readonly successPercent: number;
  readonly durationMs: number;
  readonly verifiedBytes: number;
  readonly verifiedGoodputBitsPerSecond: number;
  readonly requestsReceivedByB: number;
  readonly responsesSentByB: number;
  readonly responsesReceivedByA: number;
  readonly corruptDatagrams: number;
  readonly rttMs: Distribution | null;
  readonly forwardSnrDb: Distribution | null;
  readonly returnSnrDb: Distribution | null;
}

export interface RoundTripResult {
  readonly runId: number;
  readonly datagramBytes: number;
  readonly bodyBytes: number;
  readonly samples: readonly RoundTripSample[];
  readonly summary: RoundTripSummary;
}

export interface RoundTripOptions {
  readonly a: DatagramRadio;
  readonly b: DatagramRadio;
  readonly channel: number;
  readonly count: number;
  readonly datagramBytes: number;
  readonly timeoutMs: number;
  readonly runId?: number;
  readonly onSample?: (sample: RoundTripSample) => void;
}

type PendingOutcome =
  | { readonly status: "ok"; readonly returnSnrDb: number }
  | { readonly status: "corrupt"; readonly leg: "forward" | "return" }
  | { readonly status: "send-error"; readonly error: string }
  | { readonly status: "timeout" };

interface PendingExchange {
  forwardSnrDb: number | undefined;
  settled: boolean;
  readonly resolve: (outcome: PendingOutcome) => void;
}

export async function runRoundTrips(
  options: RoundTripOptions,
): Promise<RoundTripResult> {
  const runId = options.runId ?? randomBytes(4).readUInt32LE(0);
  const samples: RoundTripSample[] = [];
  const pending = new Map<number, PendingExchange>();
  const seenRequests = new Set<number>();
  let requestsReceivedByB = 0;
  let responsesSentByB = 0;
  let responsesReceivedByA = 0;
  let corruptDatagrams = 0;

  const unsubscribeB = options.b.onDatagram(async (datagram) => {
    if (!isExpectedTransport(datagram, options.channel)) {
      return;
    }
    const decoded = decodeDatagram(datagram.bytes);
    if (
      decoded === null ||
      decoded.runId !== runId ||
      decoded.kind !== DatagramKind.request
    ) {
      return;
    }

    const exchange = pending.get(decoded.sequence);
    if (exchange === undefined || seenRequests.has(decoded.sequence)) {
      return;
    }
    seenRequests.add(decoded.sequence);

    if (
      !verifyDatagram(
        decoded,
        DatagramKind.request,
        runId,
        decoded.sequence,
        options.datagramBytes,
      )
    ) {
      if (settle(exchange, { status: "corrupt", leg: "forward" })) {
        corruptDatagrams += 1;
      }
      return;
    }

    requestsReceivedByB += 1;
    exchange.forwardSnrDb = datagram.snrDb;
    try {
      await options.b.send(
        options.channel,
        encodeDatagram(
          DatagramKind.response,
          runId,
          decoded.sequence,
          options.datagramBytes,
        ),
      );
      responsesSentByB += 1;
    } catch (error: unknown) {
      settle(exchange, { status: "send-error", error: errorMessage(error) });
    }
  });

  const unsubscribeA = options.a.onDatagram((datagram) => {
    if (!isExpectedTransport(datagram, options.channel)) {
      return;
    }
    const decoded = decodeDatagram(datagram.bytes);
    if (
      decoded === null ||
      decoded.runId !== runId ||
      decoded.kind !== DatagramKind.response
    ) {
      return;
    }

    const exchange = pending.get(decoded.sequence);
    if (exchange === undefined) {
      return;
    }
    if (
      !verifyDatagram(
        decoded,
        DatagramKind.response,
        runId,
        decoded.sequence,
        options.datagramBytes,
      )
    ) {
      if (settle(exchange, { status: "corrupt", leg: "return" })) {
        corruptDatagrams += 1;
      }
      return;
    }

    if (settle(exchange, { status: "ok", returnSnrDb: datagram.snrDb })) {
      responsesReceivedByA += 1;
    }
  });

  const runStarted = performance.now();
  try {
    for (let sequence = 1; sequence <= options.count; sequence += 1) {
      const exchangeStarted = performance.now();
      let resolveOutcome: (outcome: PendingOutcome) => void = () => undefined;
      const outcomePromise = new Promise<PendingOutcome>((resolve) => {
        resolveOutcome = resolve;
      });
      const exchange: PendingExchange = {
        forwardSnrDb: undefined,
        settled: false,
        resolve: resolveOutcome,
      };
      pending.set(sequence, exchange);

      let outcome: PendingOutcome;
      try {
        await options.a.send(
          options.channel,
          encodeDatagram(
            DatagramKind.request,
            runId,
            sequence,
            options.datagramBytes,
          ),
        );
        outcome = await waitForOutcome(outcomePromise, options.timeoutMs);
      } catch (error: unknown) {
        outcome = { status: "send-error", error: errorMessage(error) };
      } finally {
        pending.delete(sequence);
      }

      const sample = buildSample(
        sequence,
        outcome,
        exchange.forwardSnrDb,
        exchangeStarted,
      );
      samples.push(sample);
      options.onSample?.(sample);
    }
  } finally {
    unsubscribeA();
    unsubscribeB();
  }

  const durationMs = performance.now() - runStarted;
  const completed = samples.filter((sample) => sample.status === "ok").length;
  const verifiedBytes = completed * options.datagramBytes * 2;

  return {
    runId,
    datagramBytes: options.datagramBytes,
    bodyBytes: options.datagramBytes - FIELDLINK_HEADER_BYTES,
    samples,
    summary: {
      attempted: options.count,
      completed,
      failed: options.count - completed,
      successPercent: percent(completed, options.count),
      durationMs: round(durationMs),
      verifiedBytes,
      verifiedGoodputBitsPerSecond:
        durationMs === 0 ? 0 : round((verifiedBytes * 8 * 1000) / durationMs),
      requestsReceivedByB,
      responsesSentByB,
      responsesReceivedByA,
      corruptDatagrams,
      rttMs: distribution(samples.flatMap((sample) => sample.rttMs ?? [])),
      forwardSnrDb: distribution(
        samples.flatMap((sample) => sample.forwardSnrDb ?? []),
      ),
      returnSnrDb: distribution(
        samples.flatMap((sample) => sample.returnSnrDb ?? []),
      ),
    },
  };
}

function settle(exchange: PendingExchange, outcome: PendingOutcome): boolean {
  if (exchange.settled) {
    return false;
  }
  exchange.settled = true;
  exchange.resolve(outcome);
  return true;
}

function buildSample(
  sequence: number,
  outcome: PendingOutcome,
  forwardSnrDb: number | undefined,
  exchangeStarted: number,
): RoundTripSample {
  if (outcome.status === "ok") {
    return {
      sequence,
      status: "ok",
      rttMs: round(performance.now() - exchangeStarted),
      ...(forwardSnrDb === undefined ? {} : { forwardSnrDb }),
      returnSnrDb: outcome.returnSnrDb,
    };
  }
  if (outcome.status === "corrupt") {
    return {
      sequence,
      status: "corrupt",
      ...(forwardSnrDb === undefined ? {} : { forwardSnrDb }),
      error: `${outcome.leg} datagram failed byte verification`,
    };
  }
  if (outcome.status === "send-error") {
    return {
      sequence,
      status: "send-error",
      ...(forwardSnrDb === undefined ? {} : { forwardSnrDb }),
      error: outcome.error,
    };
  }
  return { sequence, status: "timeout" };
}

function waitForOutcome(
  promise: Promise<PendingOutcome>,
  timeoutMs: number,
): Promise<PendingOutcome> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      resolve({ status: "timeout" });
    }, timeoutMs);

    void promise.then((outcome) => {
      clearTimeout(timer);
      resolve(outcome);
    });
  });
}

function isExpectedTransport(
  datagram: ChannelDatagram,
  channel: number,
): boolean {
  return (
    datagram.channel === channel && datagram.dataType === FIELDLINK_DATA_TYPE
  );
}

function distribution(values: readonly number[]): Distribution | null {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    min: round(sorted[0] ?? 0),
    mean: round(total / sorted.length),
    p50: round(percentile(sorted, 0.5)),
    p95: round(percentile(sorted, 0.95)),
    max: round(sorted.at(-1) ?? 0),
  };
}

function percentile(
  sorted: readonly number[],
  percentileValue: number,
): number {
  const index = Math.max(0, Math.ceil(percentileValue * sorted.length) - 1);
  return sorted[index] ?? 0;
}

function percent(numerator: number, denominator: number): number {
  return denominator === 0 ? 0 : round((numerator / denominator) * 100);
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
