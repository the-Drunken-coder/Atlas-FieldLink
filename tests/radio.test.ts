import { describe, expect, it, vi } from "vitest";
import { Constants } from "@liamcottle/meshcore.js";

import { MESHCORE_DATAGRAM_BYTES } from "../src/frame.js";
import {
  FIELDLINK_DATA_TYPE,
  MeshCoreTransport,
  radioPortCandidates,
  safeChannelConfiguration,
  safeRadioIdentity,
  selectMatchingChannel,
  type CompanionConnection,
  type InboxMessage,
} from "../src/radio.js";

describe("radio discovery", () => {
  it("returns only current macOS USB callout candidates", () => {
    const ports = [
      { path: "/dev/tty.Bluetooth-Incoming-Port" },
      { path: "/dev/tty.debug-console" },
      {
        path: "/dev/tty.usbmodem-debug-console",
        vendorId: "239a",
      },
      { path: "/dev/tty.usbmodem-audio", productId: "0001" },
      { path: "/dev/tty.soundcoreSpaceOne" },
      {
        path: "/dev/tty.usbserial-4",
        manufacturer: "Silicon Labs",
        serialNumber: "0001",
        vendorId: "10c4",
        productId: "ea60",
      },
    ];

    expect(
      radioPortCandidates(ports, "darwin", [
        "cu.Bluetooth-Incoming-Port",
        "cu.debug-console",
        "cu.soundcoreSpaceOne",
        "cu.usbmodem-audio",
        "cu.usbmodem-debug-console",
        "cu.usbserial-0001",
        "cu.usbserial-4",
        "tty.usbserial-4",
      ]),
    ).toEqual([
      { path: "/dev/cu.usbserial-0001" },
      {
        path: "/dev/cu.usbserial-4",
        manufacturer: "Silicon Labs",
        serialNumber: "0001",
        vendorId: "10c4",
        productId: "ea60",
      },
    ]);
  });

  it("uses callout paths when the macOS device directory cannot be read", () => {
    expect(
      radioPortCandidates(
        [
          { path: "/dev/tty.Bluetooth-Incoming-Port" },
          { path: "/dev/tty.usbserial-4", vendorId: "10c4" },
        ],
        "darwin",
      ),
    ).toEqual([{ path: "/dev/cu.usbserial-4", vendorId: "10c4" }]);
  });

  it("filters generic serial devices on other platforms", () => {
    expect(
      radioPortCandidates(
        [
          { path: "/dev/ttyS0" },
          { path: "/dev/ttyUSB0" },
          { path: "/dev/serial-radio", vendorId: "239a" },
        ],
        "linux",
      ),
    ).toEqual([
      { path: "/dev/serial-radio", vendorId: "239a" },
      { path: "/dev/ttyUSB0" },
    ]);
  });

  it("chooses the lowest configured channel that matches exactly", () => {
    const channel = (
      index: number,
      name: string,
      keyFingerprint: string,
      configured = true,
    ) => ({ index, name, keyFingerprint, configured });
    const a = [
      channel(4, "field", "bbbb"),
      channel(1, "field", "aaaa"),
      channel(0, "empty", "zero", false),
    ];
    const b = [
      channel(1, "field", "aaaa"),
      channel(4, "other", "bbbb"),
      channel(0, "empty", "zero", false),
    ];

    expect(selectMatchingChannel(a, b)).toEqual(channel(1, "field", "aaaa"));
    expect(
      selectMatchingChannel(a, [channel(1, "field", "different")]),
    ).toBeUndefined();
  });
});

class FakeConnection implements CompanionConnection {
  readonly sent: unknown[][] = [];
  readonly messages: (InboxMessage | null)[] = [];
  readonly requestedChannels: number[] = [];
  getChannelsCalls = 0;
  channelCount = 40;
  channelIndexOffset = 0;
  connected = false;
  closed = false;
  queueLen = 0;
  firmwareCode = 12;
  sendError: Error | undefined;
  readonly #listeners = new Map<
    string | number,
    Set<(...arguments_: readonly unknown[]) => void>
  >();

  on(
    eventName: string | number,
    listener: (...arguments_: readonly unknown[]) => void,
  ): this {
    const listeners = this.#listeners.get(eventName) ?? new Set();
    listeners.add(listener);
    this.#listeners.set(eventName, listeners);
    return this;
  }

  off(
    eventName: string | number,
    listener: (...arguments_: readonly unknown[]) => void,
  ): this {
    this.#listeners.get(eventName)?.delete(listener);
    return this;
  }

  emit(eventName: string | number, ...arguments_: readonly unknown[]): void {
    for (const listener of this.#listeners.get(eventName) ?? []) {
      listener(...arguments_);
    }
  }

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closed = true;
    return Promise.resolve();
  }

  getChannel(channelIndex: number): Promise<{
    channelIdx: number;
    name: string;
    secret: Uint8Array;
  }> {
    this.requestedChannels.push(channelIndex);
    return Promise.resolve({
      channelIdx: channelIndex,
      name: "fieldlink",
      secret: Uint8Array.of(1, 2, 3),
    });
  }

  getChannels(): Promise<
    readonly {
      channelIdx: number;
      name: string;
      secret: Uint8Array;
    }[]
  > {
    this.getChannelsCalls += 1;
    return Promise.resolve(
      Array.from({ length: this.channelCount }, (_value, index) => ({
        channelIdx: index + this.channelIndexOffset,
        name: "fieldlink",
        secret: Uint8Array.of(1, 2, 3),
      })),
    );
  }

  getSelfInfo(): Promise<{
    publicKey: Uint8Array;
    name: string;
    radioFreq: number;
    radioBw: number;
    radioSf: number;
    radioCr: number;
    txPower: number;
    maxTxPower: number;
  }> {
    return Promise.resolve({
      publicKey: Uint8Array.from({ length: 32 }, (_value, index) => index),
      name: "radio",
      radioFreq: 915_000_000,
      radioBw: 250_000,
      radioSf: 10,
      radioCr: 5,
      txPower: 10,
      maxTxPower: 22,
    });
  }

  deviceQuery(): Promise<{
    firmwareVer: number;
    firmware_build_date: string;
    manufacturerModel: string;
  }> {
    return Promise.resolve({
      firmwareVer: this.firmwareCode,
      firmware_build_date: "2026-01-01",
      manufacturerModel: "RAK4631\0v1.0\0",
    });
  }

  getStatsCore(): Promise<{
    data: { batteryMilliVolts: number; uptimeSecs: number; queueLen: number };
  }> {
    return Promise.resolve({
      data: { batteryMilliVolts: 4000, uptimeSecs: 1, queueLen: this.queueLen },
    });
  }

  sendChannelData(...arguments_: unknown[]): Promise<void> {
    this.sent.push(arguments_);
    return this.sendError === undefined
      ? Promise.resolve()
      : Promise.reject(this.sendError);
  }

  syncNextMessage(): Promise<InboxMessage | null> {
    return Promise.resolve(this.messages.shift() ?? null);
  }
}

describe("MeshCore transport", () => {
  it("uses MeshCore's all-channel read without transmitting", async () => {
    const connection = new FakeConnection();
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();
    const channels = await transport.getChannels();

    expect(channels.map((channel) => channel.index)).toEqual(
      Array.from({ length: 40 }, (_value, index) => index),
    );
    expect(connection.getChannelsCalls).toBe(1);
    expect(connection.requestedChannels).toHaveLength(0);
    expect(connection.sent).toHaveLength(0);
    await transport.close();
  });

  it("accepts the channel count reported by MeshCore", async () => {
    const connection = new FakeConnection();
    connection.channelCount = 4;
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();

    await expect(transport.getChannels()).resolves.toHaveLength(4);
    await transport.close();
  });

  it("rejects channel slots returned out of order or with gaps", async () => {
    const connection = new FakeConnection();
    connection.channelIndexOffset = 1;
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();

    await expect(transport.getChannels()).rejects.toThrow("out of order");
    await transport.close();
  });

  it("uses 0xFFFF flood channel data, enforces 163 bytes, and paces from Core Stats", async () => {
    const connection = new FakeConnection();
    connection.queueLen = 3;
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();
    await transport.send(new Uint8Array(MESHCORE_DATAGRAM_BYTES));
    expect(connection.sent[0]).toEqual([
      2,
      0xff,
      new Uint8Array(),
      FIELDLINK_DATA_TYPE,
      new Uint8Array(MESHCORE_DATAGRAM_BYTES),
    ]);
    expect(FIELDLINK_DATA_TYPE).toBe(0xffff);
    expect(await transport.getQueueLength()).toBe(3);
    await expect(
      transport.send(new Uint8Array(MESHCORE_DATAGRAM_BYTES + 1)),
    ).rejects.toThrow("cannot exceed 163");
    await transport.close();
  });

  it("drains every inbox item but forwards only the selected FieldLink channel data", async () => {
    const connection = new FakeConnection();
    const inbox: InboxMessage[] = [
      {
        channelMessage: {
          channelIdx: 2,
          pathLen: 1,
          txtType: 0,
          senderTimestamp: 1,
          text: "hello",
        },
      },
      {
        channelData: {
          channelIdx: 1,
          dataType: FIELDLINK_DATA_TYPE,
          snr: -2,
          pathLen: 1,
          dataLen: 1,
          data: Uint8Array.of(1),
        },
      },
      {
        channelData: {
          channelIdx: 2,
          dataType: 7,
          snr: -3,
          pathLen: 2,
          dataLen: 1,
          data: Uint8Array.of(2),
        },
      },
      {
        channelData: {
          channelIdx: 2,
          dataType: FIELDLINK_DATA_TYPE,
          snr: -4,
          pathLen: 3,
          dataLen: 1,
          data: Uint8Array.of(3),
        },
      },
    ];
    connection.messages.push(...inbox, null);
    const consumed: InboxMessage[] = [];
    const delivered: Uint8Array[] = [];
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
      onInboxMessage: (message) => {
        consumed.push(message);
      },
    });
    await transport.open();
    transport.onDatagram((datagram) => {
      delivered.push(datagram.bytes);
    });
    await transport.startInbox();
    expect(consumed).toEqual(inbox);
    expect(delivered).toEqual([Uint8Array.of(3)]);
    await transport.close();
  });

  it("quarantines startup datagrams until delivery is explicitly enabled", async () => {
    const connection = new FakeConnection();
    const datagram = (byte: number): InboxMessage => ({
      channelData: {
        channelIdx: 2,
        dataType: FIELDLINK_DATA_TYPE,
        snr: -4,
        pathLen: 3,
        dataLen: 1,
        data: Uint8Array.of(byte),
      },
    });
    const startup = datagram(1);
    const live = datagram(2);
    connection.messages.push(startup, null);
    const consumed: InboxMessage[] = [];
    const delivered: Uint8Array[] = [];
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
      onInboxMessage: (message) => {
        consumed.push(message);
      },
    });
    await transport.open();
    transport.onDatagram((message) => {
      delivered.push(message.bytes);
    });

    await transport.startInbox({ deliverDatagrams: false });
    expect(consumed).toEqual([startup]);
    expect(delivered).toEqual([]);
    await transport.enableDatagramDelivery();
    connection.messages.push(live, null);
    await transport.flushInbox();

    expect(consumed).toEqual([startup, live]);
    expect(delivered).toEqual([Uint8Array.of(2)]);
    await transport.close();
  });

  it("forces another drain pass when an explicit flush joins an active drain", async () => {
    const connection = new FakeConnection();
    let releaseFirst: ((message: InboxMessage | null) => void) | undefined;
    const syncNextMessage = vi
      .spyOn(connection, "syncNextMessage")
      .mockImplementationOnce(
        () =>
          new Promise<InboxMessage | null>((resolve) => {
            releaseFirst = resolve;
          }),
      )
      .mockResolvedValue(null);
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();

    const first = transport.flushInbox();
    const second = transport.flushInbox();
    await vi.waitFor(() => {
      expect(releaseFirst).toBeDefined();
    });
    if (releaseFirst === undefined) {
      throw new Error("The first inbox read did not start");
    }
    releaseFirst(null);
    await Promise.all([first, second]);

    expect(syncNextMessage).toHaveBeenCalledTimes(2);
    await transport.close();
  });

  it("reads safe identity and channel fingerprints without configuration writes", async () => {
    const connection = new FakeConnection();
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();
    const identity = await transport.getIdentity();
    const channel = await transport.getChannel();
    const safeIdentity = safeRadioIdentity(identity);
    const safeChannel = safeChannelConfiguration(channel);
    expect(safeIdentity).not.toHaveProperty("publicKey");
    expect(safeIdentity.nodeId).toMatch(/^[0-9a-f]{16}$/);
    expect(safeChannel).toMatchObject({ configured: true, name: "fieldlink" });
    expect(safeChannel).not.toHaveProperty("secret");
    expect(connection.sent).toHaveLength(0);
    await transport.close();
  });

  it("surfaces MeshCore send failures", async () => {
    const connection = new FakeConnection();
    connection.sendError = new Error("queue rejected");
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
    });
    await transport.open();
    await expect(transport.send(Uint8Array.of(1))).rejects.toThrow(
      "queue rejected",
    );
    await transport.close();
  });

  it("fails preflight on unsupported firmware and reports disconnects", async () => {
    const connection = new FakeConnection();
    connection.firmwareCode = 11;
    const onFatalError = vi.fn();
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
      onFatalError,
    });
    await transport.open();
    await expect(transport.getIdentity()).rejects.toThrow(
      "require 12 or newer",
    );
    connection.emit("disconnected");
    await vi.waitFor(() => {
      expect(onFatalError).toHaveBeenCalledWith(
        expect.objectContaining({ message: "/dev/cu.test disconnected" }),
      );
    });
    await expect(transport.getQueueLength()).rejects.toThrow("unavailable");
    await expect(transport.close()).rejects.toThrow("Could not cleanly close");
  });

  it("stops draining when inbox evidence cannot be preserved", async () => {
    const connection = new FakeConnection();
    connection.messages.push(
      {
        channelMessage: {
          channelIdx: 2,
          pathLen: 1,
          txtType: 0,
          senderTimestamp: 1,
          text: "one",
        },
      },
      {
        channelMessage: {
          channelIdx: 2,
          pathLen: 1,
          txtType: 0,
          senderTimestamp: 2,
          text: "two",
        },
      },
      null,
    );
    const onInbox = vi.fn().mockRejectedValueOnce(new Error("disk full"));
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
      onInboxMessage: onInbox,
    });
    await transport.open();
    await expect(transport.startInbox()).rejects.toThrow("disk full");
    expect(onInbox).toHaveBeenCalledTimes(1);
    expect(connection.messages).toHaveLength(2);
    await transport.close();
  });

  it("disables background drains and reports fatal evidence failures", async () => {
    const connection = new FakeConnection();
    connection.messages.push(null);
    const onInbox = vi.fn().mockRejectedValueOnce(new Error("disk full"));
    const onFatalError = vi.fn();
    const transport = new MeshCoreTransport("/dev/cu.test", {
      channel: 2,
      connection,
      onInboxMessage: onInbox,
      onFatalError,
    });
    await transport.open();
    await transport.startInbox();
    connection.messages.push(
      {
        channelMessage: {
          channelIdx: 2,
          pathLen: 1,
          txtType: 0,
          senderTimestamp: 1,
          text: "one",
        },
      },
      {
        channelMessage: {
          channelIdx: 2,
          pathLen: 1,
          txtType: 0,
          senderTimestamp: 2,
          text: "two",
        },
      },
      null,
    );

    connection.emit(Constants.PushCodes.MsgWaiting);
    await vi.waitFor(() => {
      expect(onFatalError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: "disk full",
        }),
      );
    });
    connection.emit(Constants.PushCodes.MsgWaiting);
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(onInbox).toHaveBeenCalledTimes(1);
    expect(connection.messages).toHaveLength(2);
    await expect(transport.waitUntilIdle()).rejects.toThrow("disk full");
    await expect(transport.close()).rejects.toThrow("Could not cleanly close");
  });
});
