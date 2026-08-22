import { performance } from "node:perf_hooks";
import { describe, expect, it } from "vitest";

import {
  DatagramKind,
  decodeDatagram,
  encodeDatagram,
} from "../src/protocol.js";
import type {
  ChannelDatagram,
  DatagramListener,
  DatagramRadio,
} from "../src/radio.js";
import { FIELDLINK_DATA_TYPE } from "../src/radio.js";
import {
  runDirectionalBench,
  runRoundTrips,
  type DirectionalSample,
  type RoundTripSample,
} from "../src/runner.js";

class LoopbackRadio implements DatagramRadio {
  readonly #listeners = new Set<DatagramListener>();
  readonly sendStartedAt: number[] = [];
  peer: LoopbackRadio | undefined;
  snrDb = -4;
  delayMs = 0;
  duplicate = false;
  dropKind: number | undefined;
  transform: ((bytes: Uint8Array) => Uint8Array) | undefined;
  idleError: Error | undefined;

  async send(channel: number, bytes: Uint8Array): Promise<void> {
    this.sendStartedAt.push(performance.now());
    if (this.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    }
    const decoded = decodeDatagram(bytes);
    if (decoded?.kind === this.dropKind) {
      return;
    }
    const delivered =
      this.transform?.(Uint8Array.from(bytes)) ?? Uint8Array.from(bytes);
    await this.#deliver(channel, delivered);
    if (this.duplicate) {
      await this.#deliver(channel, Uint8Array.from(delivered));
    }
  }

  onDatagram(listener: DatagramListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  waitUntilIdle(): Promise<void> {
    if (this.idleError !== undefined) {
      return Promise.reject(this.idleError);
    }
    return Promise.resolve();
  }

  async #deliver(channel: number, bytes: Uint8Array): Promise<void> {
    const datagram: ChannelDatagram = {
      channel,
      dataType: FIELDLINK_DATA_TYPE,
      snrDb: this.snrDb,
      pathLength: 0xff,
      bytes,
    };
    const listeners = this.peer === undefined ? [] : this.peer.#listeners;
    for (const listener of listeners) {
      await listener(datagram);
    }
  }
}

function radioPair(): readonly [LoopbackRadio, LoopbackRadio] {
  const a = new LoopbackRadio();
  const b = new LoopbackRadio();
  a.peer = b;
  b.peer = a;
  return [a, b];
}

describe("two-radio round trips", () => {
  it("verifies bytes and reports application and mesh metrics", async () => {
    const [a, b] = radioPair();
    a.snrDb = -7;
    b.snrDb = -5;
    const samples: RoundTripSample[] = [];

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 3,
      datagramBytes: 64,
      timeoutMs: 50,
      runId: 123,
      onSample: (sample) => samples.push(sample),
    });

    expect(result.summary).toMatchObject({
      attempted: 3,
      completed: 3,
      failed: 0,
      successPercent: 100,
      applicationBytes: 312,
      meshDatagramBytes: 384,
      requestsReceivedByB: 3,
      responsesSentByB: 3,
      responsesReceivedByA: 3,
      anomalyTotal: 0,
    });
    expect(result.summary.forwardSnrDb?.mean).toBe(-7);
    expect(result.summary.returnSnrDb?.mean).toBe(-5);
    expect(result.summary.rttMs?.p99).toBeTypeOf("number");
    expect(samples.every((sample) => sample.status === "ok")).toBe(true);
  });

  it("starts the deadline before send and waits for a late send before continuing", async () => {
    const [a, b] = radioPair();
    a.delayMs = 25;
    const samples: RoundTripSample[] = [];

    await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 2,
      datagramBytes: 16,
      timeoutMs: 5,
      runId: 456,
      onSample: (sample) => samples.push(sample),
    });

    expect(samples.map((sample) => sample.status)).toEqual([
      "timeout",
      "timeout",
    ]);
    const [firstSend, secondSend] = a.sendStartedAt;
    expect(firstSend).toBeDefined();
    expect(secondSend).toBeDefined();
    if (firstSend === undefined || secondSend === undefined) {
      throw new Error("expected two send timestamps");
    }
    expect(secondSend - firstSend).toBeGreaterThanOrEqual(20);
  });

  it("preserves forward SNR when the response is lost", async () => {
    const [a, b] = radioPair();
    a.snrDb = -9;
    b.dropKind = DatagramKind.response;
    const samples: RoundTripSample[] = [];

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 1,
      datagramBytes: 16,
      timeoutMs: 5,
      runId: 789,
      onSample: (sample) => samples.push(sample),
    });

    expect(samples[0]).toMatchObject({ status: "timeout", forwardSnrDb: -9 });
    expect(result.summary.forwardSnrDb?.mean).toBe(-9);
  });

  it("preserves partial results when a radio cannot settle", async () => {
    const [a, b] = radioPair();
    a.dropKind = DatagramKind.request;
    a.idleError = new Error("radio queue stayed busy");
    const samples: RoundTripSample[] = [];

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 3,
      datagramBytes: 16,
      timeoutMs: 5,
      runId: 790,
      onSample: (sample) => samples.push(sample),
    });

    expect(result.summary.attempted).toBe(1);
    expect(result.summary.operationErrors).toEqual(["radio queue stayed busy"]);
    expect(samples).toHaveLength(1);
  });

  it("counts duplicate requests and responses as run-failing anomalies", async () => {
    const [a, b] = radioPair();
    a.duplicate = true;
    b.duplicate = true;

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 1,
      datagramBytes: 32,
      timeoutMs: 50,
      runId: 100,
    });

    expect(result.summary.anomalies.duplicateRequests).toBe(1);
    expect(result.summary.anomalies.duplicateResponses).toBe(1);
    expect(result.summary.anomalyTotal).toBe(2);
  });

  it.each([
    ["malformedDatagrams", (bytes: Uint8Array) => bytes.slice(0, 4)],
    [
      "malformedDatagrams",
      (bytes: Uint8Array) => {
        bytes[0] = 0;
        return bytes;
      },
    ],
    [
      "malformedDatagrams",
      (bytes: Uint8Array) => {
        bytes[2] = 0xff;
        return bytes;
      },
    ],
    [
      "payloadMismatches",
      (bytes: Uint8Array) => {
        bytes[bytes.length - 1] = (bytes.at(-1) ?? 0) ^ 0xff;
        return bytes;
      },
    ],
    [
      "unexpectedRunIds",
      () => encodeDatagram(DatagramKind.request, 999, 1, 32),
    ],
    [
      "unexpectedKinds",
      () => encodeDatagram(DatagramKind.response, 101, 1, 32),
    ],
    [
      "unexpectedSequences",
      () => encodeDatagram(DatagramKind.request, 101, 99, 32),
    ],
  ] as const)("classifies %s explicitly", async (counter, transform) => {
    const [a, b] = radioPair();
    a.transform = transform;

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 1,
      datagramBytes: 32,
      timeoutMs: 5,
      runId: 101,
    });

    expect(result.summary.anomalies[counter]).toBe(1);
  });
});

describe("directional benchmark", () => {
  it("runs A-to-B and B-to-A as independent phases", async () => {
    const [a, b] = radioPair();
    a.snrDb = -6;
    b.snrDb = -3;

    const result = await runDirectionalBench({
      a,
      b,
      channel: 1,
      count: 2,
      datagramBytes: 64,
      timeoutMs: 50,
      runId: 202,
    });

    expect(result.phases[0]).toMatchObject({
      direction: "A-to-B",
      attempted: 2,
      delivered: 2,
      applicationBytes: 104,
      meshDatagramBytes: 128,
    });
    expect(result.phases[0].snrDb?.mean).toBe(-6);
    expect(result.phases[1]).toMatchObject({
      direction: "B-to-A",
      attempted: 2,
      delivered: 2,
      applicationBytes: 104,
      meshDatagramBytes: 128,
    });
    expect(result.phases[1].snrDb?.mean).toBe(-3);
  });

  it("reports zero application goodput for a header-only datagram", async () => {
    const [a, b] = radioPair();

    const result = await runDirectionalBench({
      a,
      b,
      channel: 1,
      count: 1,
      datagramBytes: 12,
      timeoutMs: 50,
      runId: 203,
    });

    expect(result.phases[0].applicationBytes).toBe(0);
    expect(result.phases[0].applicationGoodputBitsPerSecond).toBe(0);
    expect(result.phases[0].meshDatagramBitsPerSecond).toBeGreaterThan(0);
  });

  it("reports an active sample as aborted and stops both phases", async () => {
    const [a, b] = radioPair();
    a.delayMs = 25;
    const controller = new AbortController();
    const samples: DirectionalSample[] = [];
    setTimeout(() => {
      controller.abort();
    }, 5);

    const result = await runDirectionalBench({
      a,
      b,
      channel: 1,
      count: 5,
      datagramBytes: 32,
      timeoutMs: 50,
      runId: 204,
      signal: controller.signal,
      onSample: (sample) => samples.push(sample),
    });

    expect(samples).toEqual([
      { direction: "A-to-B", sequence: 1, status: "aborted" },
    ]);
    expect(result.interrupted).toBe(true);
    expect(result.phases[0].attempted).toBe(1);
    expect(result.phases[1].attempted).toBe(0);
  });
});
