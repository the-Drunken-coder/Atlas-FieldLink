import { describe, expect, it } from "vitest";

import { decodeDatagram } from "../src/protocol.js";
import type {
  ChannelDatagram,
  DatagramListener,
  DatagramRadio,
} from "../src/radio.js";
import { FIELDLINK_DATA_TYPE } from "../src/radio.js";
import { runRoundTrips } from "../src/runner.js";

class LoopbackRadio implements DatagramRadio {
  readonly #listeners = new Set<DatagramListener>();
  peer: LoopbackRadio | undefined;
  snrDb = -4;
  dropSequence: number | undefined;
  corruptResponses = false;

  async send(channel: number, bytes: Uint8Array): Promise<void> {
    const decoded = decodeDatagram(bytes);
    if (decoded?.sequence === this.dropSequence) {
      return;
    }

    const delivered = Uint8Array.from(bytes);
    if (this.corruptResponses && decoded?.kind === 2) {
      const lastIndex = delivered.length - 1;
      delivered[lastIndex] = (delivered[lastIndex] ?? 0) ^ 0xff;
    }
    const datagram: ChannelDatagram = {
      channel,
      dataType: FIELDLINK_DATA_TYPE,
      snrDb: this.snrDb,
      pathLength: 0xff,
      bytes: delivered,
    };
    const peerListeners = this.peer === undefined ? [] : this.peer.#listeners;
    for (const listener of peerListeners) {
      await listener(datagram);
    }
  }

  onDatagram(listener: DatagramListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
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
  it("verifies request and response bytes and reports RTT and SNR", async () => {
    const [a, b] = radioPair();
    a.snrDb = -7;
    b.snrDb = -5;

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 3,
      datagramBytes: 64,
      timeoutMs: 50,
      runId: 123,
    });

    expect(result.summary).toMatchObject({
      attempted: 3,
      completed: 3,
      failed: 0,
      successPercent: 100,
      verifiedBytes: 384,
      requestsReceivedByB: 3,
      responsesSentByB: 3,
      responsesReceivedByA: 3,
      corruptDatagrams: 0,
    });
    expect(result.summary.forwardSnrDb?.mean).toBe(-7);
    expect(result.summary.returnSnrDb?.mean).toBe(-5);
    expect(result.samples.every((sample) => sample.status === "ok")).toBe(true);
  });

  it("counts a missing request as a timeout instead of inventing latency", async () => {
    const [a, b] = radioPair();
    a.dropSequence = 2;

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 2,
      datagramBytes: 16,
      timeoutMs: 5,
      runId: 456,
    });

    expect(result.summary.completed).toBe(1);
    expect(result.samples[1]).toEqual({ sequence: 2, status: "timeout" });
  });

  it("reports payload corruption separately from loss", async () => {
    const [a, b] = radioPair();
    b.corruptResponses = true;

    const result = await runRoundTrips({
      a,
      b,
      channel: 1,
      count: 1,
      datagramBytes: 32,
      timeoutMs: 50,
      runId: 789,
    });

    expect(result.summary.corruptDatagrams).toBe(1);
    expect(result.samples[0]?.status).toBe("corrupt");
  });
});
