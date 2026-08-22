import { describe, expect, it } from "vitest";
import { Constants } from "@liamcottle/meshcore.js";

import {
  FIELDLINK_PING_DATAGRAM_BYTES,
  encodeDatagram,
  DatagramKind,
} from "../src/protocol.js";
import {
  FIELDLINK_DATA_TYPE,
  MeshCoreRadio,
  type InboxMessage,
} from "../src/radio.js";

type Listener = (...arguments_: readonly unknown[]) => void;

class FakeConnection {
  readonly listeners = new Map<string | number, Set<Listener>>();
  readonly messages: InboxMessage[] = [];
  closeCalls = 0;
  hangConnect = false;
  connectGate: Promise<void> | undefined;
  hangGetChannel = false;
  emitWaitingOnFirstSync = false;
  syncCalls = 0;
  firmwareCode = 12;

  async connect(): Promise<void> {
    if (this.hangConnect) {
      await new Promise<void>(() => undefined);
    }
    await this.connectGate;
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  on(eventName: string | number, listener: Listener): this {
    const listeners = this.listeners.get(eventName) ?? new Set<Listener>();
    listeners.add(listener);
    this.listeners.set(eventName, listeners);
    return this;
  }

  off(eventName: string | number, listener: Listener): this {
    this.listeners.get(eventName)?.delete(listener);
    return this;
  }

  emit(eventName: string | number, ...arguments_: readonly unknown[]): void {
    for (const listener of this.listeners.get(eventName) ?? []) {
      listener(...arguments_);
    }
  }

  async getChannel(channelIdx: number) {
    if (this.hangGetChannel) {
      await new Promise<void>(() => undefined);
    }
    return { channelIdx, name: "test", secret: new Uint8Array(32).fill(1) };
  }

  getSelfInfo() {
    return Promise.resolve({
      publicKey: new Uint8Array(32).fill(7),
      name: "Field radio",
      radioFreq: 910.525,
      radioBw: 250,
      radioSf: 10,
      radioCr: 5,
      txPower: 20,
      maxTxPower: 22,
    });
  }

  deviceQuery(appTargetVersion: number) {
    void appTargetVersion;
    return Promise.resolve({
      firmwareVer: this.firmwareCode,
      firmware_build_date: "2026-08-01",
      manufacturerModel: "RAK4631\0v1.12.0\0",
    });
  }

  async sendChannelData(): Promise<void> {}

  syncNextMessage(): Promise<InboxMessage | null> {
    this.syncCalls += 1;
    if (this.emitWaitingOnFirstSync && this.syncCalls === 1) {
      this.emit(Constants.PushCodes.MsgWaiting);
      return Promise.resolve(null);
    }
    return Promise.resolve(this.messages.shift() ?? null);
  }
}

describe("MeshCore radio adapter", () => {
  it("closes the transport when opening times out", async () => {
    const connection = new FakeConnection();
    connection.hangConnect = true;
    const radio = new MeshCoreRadio("/dev/test", { connection });

    await expect(radio.open(5)).rejects.toThrow("Timed out opening");
    expect(connection.closeCalls).toBe(1);
  });

  it("cannot reopen after close wins a connection race", async () => {
    const connection = new FakeConnection();
    let releaseConnect = (): void => undefined;
    connection.connectGate = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });
    const radio = new MeshCoreRadio("/dev/test", { connection });

    const opening = radio.open(100);
    await radio.close();
    releaseConnect();

    await expect(opening).rejects.toThrow("cancelled by close");
    await expect(radio.getChannel(1)).rejects.toThrow("is not open");
    expect(connection.closeCalls).toBe(2);
  });

  it("records every consumed inbox item and re-drains a push race", async () => {
    const connection = new FakeConnection();
    connection.emitWaitingOnFirstSync = true;
    connection.messages.push(
      {
        channelMessage: {
          channelIdx: 1,
          pathLen: 0xff,
          txtType: 0,
          senderTimestamp: 10,
          text: "keep me",
        },
      },
      {
        channelData: {
          snr: -8,
          channelIdx: 1,
          pathLen: 0xff,
          dataType: FIELDLINK_DATA_TYPE,
          dataLen: FIELDLINK_PING_DATAGRAM_BYTES,
          data: encodeDatagram(
            DatagramKind.request,
            1,
            1,
            FIELDLINK_PING_DATAGRAM_BYTES,
          ),
        },
      },
    );
    const inbox: InboxMessage[] = [];
    let datagrams = 0;
    const radio = new MeshCoreRadio("/dev/test", {
      connection,
      onInboxMessage: (message) => {
        inbox.push(message);
      },
    });
    radio.onDatagram(() => {
      datagrams += 1;
    });

    await radio.open();
    await radio.waitUntilIdle();
    await radio.close();

    expect(inbox).toHaveLength(2);
    expect(inbox[0]).toHaveProperty("channelMessage.text", "keep me");
    expect(datagrams).toBe(1);
    expect(connection.syncCalls).toBeGreaterThanOrEqual(4);
  });

  it("records firmware and radio identity using a safe fingerprint", async () => {
    const connection = new FakeConnection();
    const radio = new MeshCoreRadio("/dev/test", { connection });
    await radio.open();

    const identity = await radio.getIdentity();
    await radio.close();

    expect(identity.fingerprint).toMatch(/^[0-9a-f]{16}$/);
    expect(identity).toMatchObject({
      model: "RAK4631",
      firmwareVersion: "v1.12.0",
      firmwareProtocolCode: 12,
      radio: {
        frequency: 910.525,
        bandwidth: 250,
        spreadingFactor: 10,
        codingRate: 5,
      },
    });
  });

  it("rejects firmware that predates binary channel datagrams", async () => {
    const connection = new FakeConnection();
    connection.firmwareCode = 11;
    const radio = new MeshCoreRadio("/dev/test", { connection });
    await radio.open();

    await expect(radio.getIdentity()).rejects.toThrow("require 12 or newer");
    await radio.close();
  });

  it("makes any timed-out Companion command fatal and still closes", async () => {
    const connection = new FakeConnection();
    connection.hangGetChannel = true;
    const radio = new MeshCoreRadio("/dev/test", {
      connection,
      commandTimeoutMs: 5,
    });
    await radio.open();

    await expect(radio.getChannel(1)).rejects.toThrow(
      "Timed out reading channel",
    );
    await expect(radio.getIdentity()).rejects.toThrow("is unavailable");
    await expect(radio.close()).rejects.toThrow("Could not cleanly close");
    expect(connection.closeCalls).toBe(1);
  });

  it("becomes unavailable after an unexpected disconnect", async () => {
    const connection = new FakeConnection();
    const radio = new MeshCoreRadio("/dev/test", { connection });
    await radio.open();

    connection.emit("disconnected");

    await expect(radio.getChannel(1)).rejects.toThrow("disconnected");
    await expect(radio.close()).rejects.toThrow("Could not cleanly close");
  });

  it("becomes unavailable after a serial error", async () => {
    const connection = new FakeConnection();
    const radio = new MeshCoreRadio("/dev/test", { connection });
    await radio.open();

    connection.emit("error", new Error("serial failure"));

    await expect(radio.getChannel(1)).rejects.toThrow("serial failure");
    await expect(radio.close()).rejects.toThrow("Could not cleanly close");
  });

  it("isolates a failing datagram listener", async () => {
    const connection = new FakeConnection();
    connection.messages.push({
      channelData: {
        snr: -8,
        channelIdx: 1,
        pathLen: 0xff,
        dataType: FIELDLINK_DATA_TYPE,
        dataLen: FIELDLINK_PING_DATAGRAM_BYTES,
        data: encodeDatagram(
          DatagramKind.request,
          1,
          1,
          FIELDLINK_PING_DATAGRAM_BYTES,
        ),
      },
    });
    const errors: string[] = [];
    let secondListenerCalls = 0;
    const radio = new MeshCoreRadio("/dev/test", {
      connection,
      onListenerError: (error) => {
        errors.push(error.message);
      },
    });
    radio.onDatagram(() => {
      throw new Error("listener failed");
    });
    radio.onDatagram(() => {
      secondListenerCalls += 1;
    });

    await radio.open();
    await radio.close();

    expect(errors).toEqual(["listener failed"]);
    expect(secondListenerCalls).toBe(1);
  });
});
