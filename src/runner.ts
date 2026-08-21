import { randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";

import {
  DatagramKind,
  FIELDLINK_HEADER_BYTES,
  decodeDatagram,
  encodeDatagram,
  verifyDatagram,
  type DatagramKind as DatagramKindValue,
} from "./protocol.js";
import {
  FIELDLINK_DATA_TYPE,
  type ChannelDatagram,
  type DatagramRadio,
} from "./radio.js";

export type SampleStatus =
  "ok" | "timeout" | "send-error" | "corrupt" | "aborted";

export type Direction = "A-to-B" | "B-to-A";

export interface RoundTripSample {
  readonly sequence: number;
  readonly status: SampleStatus;
  readonly rttMs?: number;
  readonly forwardSnrDb?: number;
  readonly returnSnrDb?: number;
  readonly error?: string;
}

export interface DirectionalSample {
  readonly direction: Direction;
  readonly sequence: number;
  readonly status: SampleStatus;
  readonly oneWayLatencyMs?: number;
  readonly snrDb?: number;
  readonly error?: string;
}

export interface Distribution {
  readonly min: number;
  readonly mean: number;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
  readonly max: number;
}

export interface AnomalyCounts {
  readonly duplicateRequests: number;
  readonly duplicateResponses: number;
  readonly malformedDatagrams: number;
  readonly unexpectedRunIds: number;
  readonly unexpectedKinds: number;
  readonly unexpectedSequences: number;
  readonly payloadMismatches: number;
}

export type AnomalyKind = keyof AnomalyCounts;

export interface AnomalyEvent {
  readonly kind: AnomalyKind;
  readonly receiver: "A" | "B";
  readonly sequence?: number;
  readonly detail: string;
}

export interface RoundTripSummary {
  readonly requested: number;
  readonly attempted: number;
  readonly completed: number;
  readonly failed: number;
  readonly successPercent: number;
  readonly durationMs: number;
  readonly applicationBytes: number;
  readonly meshDatagramBytes: number;
  readonly applicationGoodputBitsPerSecond: number;
  readonly meshDatagramBitsPerSecond: number;
  readonly requestsReceivedByB: number;
  readonly responsesSentByB: number;
  readonly responsesReceivedByA: number;
  readonly anomalies: AnomalyCounts;
  readonly anomalyTotal: number;
  readonly interrupted: boolean;
  readonly rttMs: Distribution | null;
  readonly forwardSnrDb: Distribution | null;
  readonly returnSnrDb: Distribution | null;
}

export interface RoundTripResult {
  readonly kind: "round-trip";
  readonly runId: number;
  readonly datagramBytes: number;
  readonly bodyBytes: number;
  readonly summary: RoundTripSummary;
}

export interface DirectionalSummary {
  readonly direction: Direction;
  readonly requested: number;
  readonly attempted: number;
  readonly delivered: number;
  readonly failed: number;
  readonly successPercent: number;
  readonly durationMs: number;
  readonly applicationBytes: number;
  readonly meshDatagramBytes: number;
  readonly applicationGoodputBitsPerSecond: number;
  readonly meshDatagramBitsPerSecond: number;
  readonly anomalies: AnomalyCounts;
  readonly anomalyTotal: number;
  readonly interrupted: boolean;
  readonly oneWayLatencyMs: Distribution | null;
  readonly snrDb: Distribution | null;
}

export interface DirectionalBenchResult {
  readonly kind: "directional";
  readonly runId: number;
  readonly datagramBytes: number;
  readonly bodyBytes: number;
  readonly phases: readonly [DirectionalSummary, DirectionalSummary];
  readonly anomalyTotal: number;
  readonly interrupted: boolean;
}

interface CommonRunOptions {
  readonly a: DatagramRadio;
  readonly b: DatagramRadio;
  readonly channel: number;
  readonly count: number;
  readonly datagramBytes: number;
  readonly timeoutMs: number;
  readonly runId?: number;
  readonly signal?: AbortSignal;
  readonly onAnomaly?: (anomaly: AnomalyEvent) => void;
}

export interface RoundTripOptions extends CommonRunOptions {
  readonly onSample?: (sample: RoundTripSample) => void;
}

export interface DirectionalBenchOptions extends CommonRunOptions {
  readonly onSample?: (sample: DirectionalSample) => void;
}

type PendingOutcome =
  | {
      readonly status: "ok";
      readonly receivedAt: number;
      readonly snrDb: number;
    }
  | { readonly status: "corrupt"; readonly leg: "forward" | "return" }
  | { readonly status: "send-error"; readonly error: string }
  | { readonly status: "timeout" }
  | { readonly status: "aborted" };

interface PendingExchange {
  forwardSnrDb: number | undefined;
  settled: boolean;
  readonly resolve: (outcome: PendingOutcome) => void;
}

type MutableAnomalies = { -readonly [Key in keyof AnomalyCounts]: number };

export async function runRoundTrips(
  options: RoundTripOptions,
): Promise<RoundTripResult> {
  const runId = options.runId ?? randomBytes(4).readUInt32LE(0);
  const pending = new Map<number, PendingExchange>();
  const seenRequests = new Set<number>();
  const seenResponses = new Set<number>();
  const anomalies = emptyAnomalies();
  const rttValues: number[] = [];
  const forwardSnrValues: number[] = [];
  const returnSnrValues: number[] = [];
  let attempted = 0;
  let completed = 0;
  let requestsReceivedByB = 0;
  let responsesSentByB = 0;
  let responsesReceivedByA = 0;

  const recordAnomaly = anomalyRecorder(anomalies, options.onAnomaly);
  const unsubscribeB = options.b.onDatagram(async (datagram) => {
    if (!isExpectedTransport(datagram, options.channel)) {
      return;
    }
    const inspected = inspectDatagram({
      datagram,
      receiver: "B",
      expectedKind: DatagramKind.request,
      runId,
      datagramBytes: options.datagramBytes,
      pending,
      seen: seenRequests,
      duplicateKind: "duplicateRequests",
      recordAnomaly,
      corruptLeg: "forward",
    });
    if (inspected === null) {
      return;
    }
    const { decoded, exchange } = inspected;
    seenRequests.add(decoded.sequence);
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
    const inspected = inspectDatagram({
      datagram,
      receiver: "A",
      expectedKind: DatagramKind.response,
      runId,
      datagramBytes: options.datagramBytes,
      pending,
      seen: seenResponses,
      duplicateKind: "duplicateResponses",
      recordAnomaly,
      corruptLeg: "return",
    });
    if (inspected === null) {
      return;
    }
    seenResponses.add(inspected.decoded.sequence);
    if (
      settle(inspected.exchange, {
        status: "ok",
        receivedAt: performance.now(),
        snrDb: datagram.snrDb,
      })
    ) {
      responsesReceivedByA += 1;
    }
  });

  const runStarted = performance.now();
  try {
    for (let sequence = 1; sequence <= options.count; sequence += 1) {
      if (options.signal?.aborted === true) {
        break;
      }
      attempted += 1;
      const exchangeStarted = performance.now();
      const exchange = createPendingExchange();
      pending.set(sequence, exchange);
      const sendPromise = options.a.send(
        options.channel,
        encodeDatagram(
          DatagramKind.request,
          runId,
          sequence,
          options.datagramBytes,
        ),
      );
      const operation = sendPromise.then(
        () => exchange.promise,
        (error: unknown): PendingOutcome => ({
          status: "send-error",
          error: errorMessage(error),
        }),
      );
      const outcome = await waitForOutcome(
        operation,
        options.timeoutMs,
        options.signal,
      );
      pending.delete(sequence);
      const sample = buildRoundTripSample(
        sequence,
        outcome,
        exchange.forwardSnrDb,
        exchangeStarted,
      );
      if (sample.status === "ok") {
        completed += 1;
        if (sample.rttMs !== undefined) {
          rttValues.push(sample.rttMs);
        }
        if (sample.forwardSnrDb !== undefined) {
          forwardSnrValues.push(sample.forwardSnrDb);
        }
        if (sample.returnSnrDb !== undefined) {
          returnSnrValues.push(sample.returnSnrDb);
        }
      } else if (sample.forwardSnrDb !== undefined) {
        forwardSnrValues.push(sample.forwardSnrDb);
      }
      options.onSample?.(sample);
      if (outcome.status === "timeout" || outcome.status === "send-error") {
        await settleLateOperations([sendPromise], [options.a, options.b]);
      }
      if (outcome.status === "aborted") {
        break;
      }
    }
  } finally {
    unsubscribeA();
    unsubscribeB();
  }

  const durationMs = performance.now() - runStarted;
  const bodyBytes = options.datagramBytes - FIELDLINK_HEADER_BYTES;
  const applicationBytes = completed * bodyBytes * 2;
  const meshDatagramBytes = completed * options.datagramBytes * 2;
  return {
    kind: "round-trip",
    runId,
    datagramBytes: options.datagramBytes,
    bodyBytes,
    summary: {
      requested: options.count,
      attempted,
      completed,
      failed: attempted - completed,
      successPercent: percent(completed, attempted),
      durationMs: round(durationMs),
      applicationBytes,
      meshDatagramBytes,
      applicationGoodputBitsPerSecond: bitrate(applicationBytes, durationMs),
      meshDatagramBitsPerSecond: bitrate(meshDatagramBytes, durationMs),
      requestsReceivedByB,
      responsesSentByB,
      responsesReceivedByA,
      anomalies,
      anomalyTotal: countAnomalies(anomalies),
      interrupted: options.signal?.aborted === true,
      rttMs: distribution(rttValues),
      forwardSnrDb: distribution(forwardSnrValues),
      returnSnrDb: distribution(returnSnrValues),
    },
  };
}

export async function runDirectionalBench(
  options: DirectionalBenchOptions,
): Promise<DirectionalBenchResult> {
  const runId = options.runId ?? randomBytes(4).readUInt32LE(0);
  const aToB = await runDirectionalPhase({
    ...options,
    runId,
    sender: options.a,
    receiver: options.b,
    receiverName: "B",
    direction: "A-to-B",
    kind: DatagramKind.aToB,
  });
  const bToA = await runDirectionalPhase({
    ...options,
    runId,
    sender: options.b,
    receiver: options.a,
    receiverName: "A",
    direction: "B-to-A",
    kind: DatagramKind.bToA,
  });
  return {
    kind: "directional",
    runId,
    datagramBytes: options.datagramBytes,
    bodyBytes: options.datagramBytes - FIELDLINK_HEADER_BYTES,
    phases: [aToB, bToA],
    anomalyTotal: aToB.anomalyTotal + bToA.anomalyTotal,
    interrupted: aToB.interrupted || bToA.interrupted,
  };
}

interface DirectionalPhaseOptions extends DirectionalBenchOptions {
  readonly runId: number;
  readonly sender: DatagramRadio;
  readonly receiver: DatagramRadio;
  readonly receiverName: "A" | "B";
  readonly direction: Direction;
  readonly kind: DatagramKindValue;
}

async function runDirectionalPhase(
  options: DirectionalPhaseOptions,
): Promise<DirectionalSummary> {
  const pending = new Map<number, PendingExchange>();
  const seen = new Set<number>();
  const anomalies = emptyAnomalies();
  const latencyValues: number[] = [];
  const snrValues: number[] = [];
  let attempted = 0;
  let delivered = 0;
  const recordAnomaly = anomalyRecorder(anomalies, options.onAnomaly);
  const unsubscribe = options.receiver.onDatagram((datagram) => {
    if (!isExpectedTransport(datagram, options.channel)) {
      return;
    }
    const inspected = inspectDatagram({
      datagram,
      receiver: options.receiverName,
      expectedKind: options.kind,
      runId: options.runId,
      datagramBytes: options.datagramBytes,
      pending,
      seen,
      duplicateKind: "duplicateRequests",
      recordAnomaly,
      corruptLeg: "forward",
    });
    if (inspected === null) {
      return;
    }
    seen.add(inspected.decoded.sequence);
    settle(inspected.exchange, {
      status: "ok",
      receivedAt: performance.now(),
      snrDb: datagram.snrDb,
    });
  });

  const phaseStarted = performance.now();
  try {
    for (let sequence = 1; sequence <= options.count; sequence += 1) {
      if (options.signal?.aborted === true) {
        break;
      }
      attempted += 1;
      const sampleStarted = performance.now();
      const exchange = createPendingExchange();
      pending.set(sequence, exchange);
      const sendPromise = options.sender.send(
        options.channel,
        encodeDatagram(
          options.kind,
          options.runId,
          sequence,
          options.datagramBytes,
        ),
      );
      const operation = sendPromise.then(
        () => exchange.promise,
        (error: unknown): PendingOutcome => ({
          status: "send-error",
          error: errorMessage(error),
        }),
      );
      const outcome = await waitForOutcome(
        operation,
        options.timeoutMs,
        options.signal,
      );
      pending.delete(sequence);
      const sample = buildDirectionalSample(
        options.direction,
        sequence,
        outcome,
        sampleStarted,
      );
      if (sample.status === "ok") {
        delivered += 1;
        if (sample.oneWayLatencyMs !== undefined) {
          latencyValues.push(sample.oneWayLatencyMs);
        }
        if (sample.snrDb !== undefined) {
          snrValues.push(sample.snrDb);
        }
      }
      options.onSample?.(sample);
      if (outcome.status === "timeout" || outcome.status === "send-error") {
        await settleLateOperations(
          [sendPromise],
          [options.sender, options.receiver],
        );
      }
      if (outcome.status === "aborted") {
        break;
      }
    }
  } finally {
    unsubscribe();
  }

  const durationMs = performance.now() - phaseStarted;
  const bodyBytes = options.datagramBytes - FIELDLINK_HEADER_BYTES;
  const applicationBytes = delivered * bodyBytes;
  const meshDatagramBytes = delivered * options.datagramBytes;
  return {
    direction: options.direction,
    requested: options.count,
    attempted,
    delivered,
    failed: attempted - delivered,
    successPercent: percent(delivered, attempted),
    durationMs: round(durationMs),
    applicationBytes,
    meshDatagramBytes,
    applicationGoodputBitsPerSecond: bitrate(applicationBytes, durationMs),
    meshDatagramBitsPerSecond: bitrate(meshDatagramBytes, durationMs),
    anomalies,
    anomalyTotal: countAnomalies(anomalies),
    interrupted: options.signal?.aborted === true,
    oneWayLatencyMs: distribution(latencyValues),
    snrDb: distribution(snrValues),
  };
}

interface InspectionOptions {
  readonly datagram: ChannelDatagram;
  readonly receiver: "A" | "B";
  readonly expectedKind: DatagramKindValue;
  readonly runId: number;
  readonly datagramBytes: number;
  readonly pending: ReadonlyMap<number, PendingExchange>;
  readonly seen: ReadonlySet<number>;
  readonly duplicateKind: "duplicateRequests" | "duplicateResponses";
  readonly recordAnomaly: (event: AnomalyEvent) => void;
  readonly corruptLeg: "forward" | "return";
}

function inspectDatagram(options: InspectionOptions): {
  readonly decoded: NonNullable<ReturnType<typeof decodeDatagram>>;
  readonly exchange: PendingExchange;
} | null {
  const decoded = decodeDatagram(options.datagram.bytes);
  if (decoded === null) {
    options.recordAnomaly({
      kind: "malformedDatagrams",
      receiver: options.receiver,
      detail: "FieldLink magic, version, kind, or header length is invalid",
    });
    const onlyPending = onlyValue(options.pending);
    if (onlyPending !== undefined) {
      settle(onlyPending, { status: "corrupt", leg: options.corruptLeg });
    }
    return null;
  }
  if (decoded.runId !== options.runId) {
    options.recordAnomaly({
      kind: "unexpectedRunIds",
      receiver: options.receiver,
      sequence: decoded.sequence,
      detail: `received run ${decoded.runId}; expected ${options.runId}`,
    });
    return null;
  }
  if (decoded.kind !== options.expectedKind) {
    options.recordAnomaly({
      kind: "unexpectedKinds",
      receiver: options.receiver,
      sequence: decoded.sequence,
      detail: `received kind ${decoded.kind}; expected ${options.expectedKind}`,
    });
    return null;
  }
  if (options.seen.has(decoded.sequence)) {
    options.recordAnomaly({
      kind: options.duplicateKind,
      receiver: options.receiver,
      sequence: decoded.sequence,
      detail: "received a sequence already accepted in this phase",
    });
    return null;
  }
  const exchange = options.pending.get(decoded.sequence);
  if (exchange === undefined) {
    options.recordAnomaly({
      kind: "unexpectedSequences",
      receiver: options.receiver,
      sequence: decoded.sequence,
      detail: "no exchange is awaiting this sequence",
    });
    return null;
  }
  if (
    !verifyDatagram(
      decoded,
      options.expectedKind,
      options.runId,
      decoded.sequence,
      options.datagramBytes,
    )
  ) {
    options.recordAnomaly({
      kind: "payloadMismatches",
      receiver: options.receiver,
      sequence: decoded.sequence,
      detail: "datagram length or deterministic body pattern did not match",
    });
    settle(exchange, { status: "corrupt", leg: options.corruptLeg });
    return null;
  }
  return { decoded, exchange };
}

function createPendingExchange(): PendingExchange & {
  readonly promise: Promise<PendingOutcome>;
} {
  let resolveOutcome: (outcome: PendingOutcome) => void = () => undefined;
  const promise = new Promise<PendingOutcome>((resolve) => {
    resolveOutcome = resolve;
  });
  return {
    forwardSnrDb: undefined,
    settled: false,
    resolve: resolveOutcome,
    promise,
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

function buildRoundTripSample(
  sequence: number,
  outcome: PendingOutcome,
  forwardSnrDb: number | undefined,
  exchangeStarted: number,
): RoundTripSample {
  if (outcome.status === "ok") {
    return {
      sequence,
      status: "ok",
      rttMs: round(outcome.receivedAt - exchangeStarted),
      ...(forwardSnrDb === undefined ? {} : { forwardSnrDb }),
      returnSnrDb: outcome.snrDb,
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
  return {
    sequence,
    status: outcome.status,
    ...(forwardSnrDb === undefined ? {} : { forwardSnrDb }),
  };
}

function buildDirectionalSample(
  direction: Direction,
  sequence: number,
  outcome: PendingOutcome,
  started: number,
): DirectionalSample {
  if (outcome.status === "ok") {
    return {
      direction,
      sequence,
      status: "ok",
      oneWayLatencyMs: round(outcome.receivedAt - started),
      snrDb: outcome.snrDb,
    };
  }
  if (outcome.status === "corrupt") {
    return {
      direction,
      sequence,
      status: "corrupt",
      error: "datagram failed byte verification",
    };
  }
  if (outcome.status === "send-error") {
    return { direction, sequence, status: "send-error", error: outcome.error };
  }
  return { direction, sequence, status: outcome.status };
}

function waitForOutcome(
  promise: Promise<PendingOutcome>,
  timeoutMs: number,
  signal: AbortSignal | undefined,
): Promise<PendingOutcome> {
  return new Promise((resolve) => {
    let finished = false;
    const finish = (outcome: PendingOutcome): void => {
      if (finished) {
        return;
      }
      finished = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      resolve(outcome);
    };
    const abort = (): void => {
      finish({ status: "aborted" });
    };
    const timer = setTimeout(() => {
      finish({ status: "timeout" });
    }, timeoutMs);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted === true) {
      abort();
      return;
    }
    void promise.then(finish);
  });
}

async function settleLateOperations(
  operations: readonly Promise<unknown>[],
  radios: readonly DatagramRadio[],
): Promise<void> {
  await Promise.allSettled(operations);
  await Promise.all(radios.map((radio) => radio.waitUntilIdle()));
}

function anomalyRecorder(
  counts: MutableAnomalies,
  listener: ((anomaly: AnomalyEvent) => void) | undefined,
): (anomaly: AnomalyEvent) => void {
  return (anomaly) => {
    counts[anomaly.kind] += 1;
    listener?.(anomaly);
  };
}

function emptyAnomalies(): MutableAnomalies {
  return {
    duplicateRequests: 0,
    duplicateResponses: 0,
    malformedDatagrams: 0,
    unexpectedRunIds: 0,
    unexpectedKinds: 0,
    unexpectedSequences: 0,
    payloadMismatches: 0,
  };
}

function countAnomalies(anomalies: AnomalyCounts): number {
  return (
    anomalies.duplicateRequests +
    anomalies.duplicateResponses +
    anomalies.malformedDatagrams +
    anomalies.unexpectedRunIds +
    anomalies.unexpectedKinds +
    anomalies.unexpectedSequences +
    anomalies.payloadMismatches
  );
}

function onlyValue<Value>(map: ReadonlyMap<number, Value>): Value | undefined {
  if (map.size !== 1) {
    return undefined;
  }
  return map.values().next().value;
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
    p99: round(percentile(sorted, 0.99)),
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

function bitrate(bytes: number, durationMs: number): number {
  return durationMs === 0 ? 0 : round((bytes * 8 * 1000) / durationMs);
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
