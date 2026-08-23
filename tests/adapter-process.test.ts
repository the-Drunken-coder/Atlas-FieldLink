import { PassThrough } from "node:stream";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";

import {
  AdapterProcessRadio,
  adapterNodeArguments,
  serveAdapter,
} from "../src/adapter-process.js";
import {
  FIELDLINK_DATA_TYPE,
  type ChannelDatagram,
  type DatagramListener,
  type RadioIdentity,
} from "../src/radio.js";

const BYTES_MARKER = "$fieldlinkBytes";

class FakeManagedRadio {
  readonly listeners = new Set<DatagramListener>();
  readonly sent: { readonly channel: number; readonly bytes: Uint8Array }[] =
    [];
  openCalls = 0;
  closeCalls = 0;
  idleCalls = 0;

  open(): Promise<void> {
    this.openCalls += 1;
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  getIdentity(): Promise<RadioIdentity> {
    return Promise.resolve(identity());
  }

  getChannel(index: number) {
    return Promise.resolve({
      index,
      name: "fieldlink-test",
      secret: new Uint8Array(32).fill(2),
    });
  }

  async send(channel: number, bytes: Uint8Array): Promise<void> {
    this.sent.push({ channel, bytes: Uint8Array.from(bytes) });
    const datagram: ChannelDatagram = {
      channel,
      dataType: FIELDLINK_DATA_TYPE,
      snrDb: -6,
      pathLength: 0xff,
      bytes: Uint8Array.from(bytes),
    };
    for (const listener of this.listeners) {
      await listener(datagram);
    }
  }

  onDatagram(listener: DatagramListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  waitUntilIdle(): Promise<void> {
    this.idleCalls += 1;
    return Promise.resolve();
  }
}

describe("single-radio adapter process", () => {
  it("opens one radio and serves send, idle, and close over NDJSON", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const messages = new TestMessageReader(output);
    const radio = new FakeManagedRadio();
    const running = serveAdapter({
      path: "/dev/radio-a",
      channel: 2,
      input,
      output,
      processId: 321,
      createRadio: () => radio,
    });

    expect(await messages.next("ready")).toMatchObject({
      type: "ready",
      processId: 321,
      channel: { index: 2, name: "fieldlink-test" },
    });

    input.write(
      wireLine({
        id: 1,
        type: "send",
        bytes: new Uint8Array([1, 2, 3]),
      }),
    );
    expect(await messages.next("datagram")).toMatchObject({
      type: "datagram",
      datagram: { channel: 2, bytes: new Uint8Array([1, 2, 3]) },
    });
    expect(await messages.next("send response")).toEqual({
      type: "response",
      id: 1,
      ok: true,
    });

    input.write(wireLine({ id: 2, type: "idle" }));
    expect(await messages.next("idle response")).toEqual({
      type: "response",
      id: 2,
      ok: true,
    });
    input.write(wireLine({ id: 3, type: "close" }));
    expect(await messages.next("close response")).toEqual({
      type: "response",
      id: 3,
      ok: true,
    });
    await withTimeout(running, "adapter server did not stop");

    expect(radio.openCalls).toBe(1);
    expect(radio.closeCalls).toBe(1);
    expect(radio.idleCalls).toBe(1);
    expect(radio.sent).toEqual([
      { channel: 2, bytes: new Uint8Array([1, 2, 3]) },
    ]);
  });

  it("proxies traffic through a separate child process", async () => {
    const inbox: unknown[] = [];
    const adapter = await AdapterProcessRadio.start({
      path: "/dev/fixture",
      channel: 1,
      allowInboxDrain: true,
      program: {
        executable: process.execPath,
        arguments: ["-e", ADAPTER_FIXTURE],
      },
      onInboxMessage: (message) => {
        inbox.push(message);
      },
    });

    try {
      expect(adapter.processId).not.toBe(process.pid);
      expect(adapter.identity.fingerprint).toBe("fixture-fingerprint");
      expect(adapter.channelConfiguration).toMatchObject({
        index: 1,
        name: "fixture-channel",
      });
      expect(inbox).toEqual([{ channelMessage: { text: "preserved" } }]);

      const received: ChannelDatagram[] = [];
      adapter.onDatagram((datagram) => {
        received.push(datagram);
      });
      await adapter.send(1, new Uint8Array([4, 5, 6]));
      await adapter.waitUntilIdle();

      expect(received).toEqual([
        {
          channel: 1,
          dataType: FIELDLINK_DATA_TYPE,
          snrDb: -7,
          pathLength: 0xff,
          bytes: new Uint8Array([4, 5, 6]),
        },
      ]);
    } finally {
      await adapter.close();
    }
  });

  it("fails pending work when an adapter process exits", async () => {
    const adapter = await AdapterProcessRadio.start({
      path: "/dev/fixture",
      channel: 1,
      allowInboxDrain: true,
      program: {
        executable: process.execPath,
        arguments: ["-e", ADAPTER_FIXTURE, "exit-on-idle"],
      },
    });

    const expected = /closed its output|exited with code 7/;
    await expect(adapter.waitUntilIdle()).rejects.toThrow(expected);
    await expect(adapter.close()).rejects.toThrow(expected);
  });

  it("drains a close response written immediately before child exit", async () => {
    const adapter = await AdapterProcessRadio.start({
      path: "/dev/fixture",
      channel: 1,
      allowInboxDrain: true,
      program: {
        executable: process.execPath,
        arguments: ["-e", ADAPTER_FIXTURE, "exit-on-close"],
      },
    });

    await expect(adapter.close()).resolves.toBeUndefined();
  });

  it("makes a request timeout the adapter's persistent failure", async () => {
    const adapter = await AdapterProcessRadio.start({
      path: "/dev/fixture",
      channel: 1,
      allowInboxDrain: true,
      requestTimeoutMs: 20,
      program: {
        executable: process.execPath,
        arguments: ["-e", ADAPTER_FIXTURE, "ignore-idle"],
      },
    });

    const expected = "did not answer idle after 20 ms";
    await expect(adapter.waitUntilIdle()).rejects.toThrow(expected);
    await expect(adapter.close()).rejects.toThrow(expected);
  });

  it("keeps loader arguments but removes controller execution modes", () => {
    expect(
      adapterNodeArguments([
        "--inspect-brk",
        "--inspect-port=9230",
        "--watch",
        "--watch-path",
        "src",
        "--watch-preserve-output",
        "--import",
        "tsx",
        "--enable-source-maps",
      ]),
    ).toEqual(["--import", "tsx", "--enable-source-maps"]);
  });

  it("includes child stderr when adapter startup fails", async () => {
    await expect(
      AdapterProcessRadio.start({
        path: "/dev/fixture",
        channel: 1,
        allowInboxDrain: true,
        program: {
          executable: process.execPath,
          arguments: ["-e", 'console.error("radio open failed")'],
        },
      }),
    ).rejects.toThrow("radio open failed");
  });
});

function identity(): RadioIdentity {
  return {
    publicKey: new Uint8Array(32).fill(1),
    fingerprint: "fixture-fingerprint",
    name: "Fixture radio",
    model: "fixture",
    firmwareVersion: "1.0.0",
    firmwareBuildDate: "2026-08-22",
    firmwareProtocolCode: 12,
    clientProtocolVersion: 1,
    radio: {
      frequency: 910.525,
      bandwidth: 250,
      spreadingFactor: 10,
      codingRate: 5,
      transmitPower: 10,
      maximumTransmitPower: 22,
    },
  };
}

class TestMessageReader {
  readonly #messages: unknown[] = [];
  readonly #waiters: ((message: unknown) => void)[] = [];

  constructor(input: PassThrough) {
    const lines = createInterface({ input, crlfDelay: Infinity });
    lines.on("line", (line) => {
      const message = JSON.parse(line, wireReviver) as unknown;
      const waiter = this.#waiters.shift();
      if (waiter === undefined) {
        this.#messages.push(message);
      } else {
        waiter(message);
      }
    });
  }

  next(description: string): Promise<unknown> {
    const message = this.#messages.shift();
    if (message !== undefined) {
      return Promise.resolve(message);
    }
    return withTimeout(
      new Promise((resolve) => {
        this.#waiters.push(resolve);
      }),
      `adapter did not write ${description}`,
    );
  }
}

function withTimeout<Result>(
  promise: Promise<Result>,
  message: string,
): Promise<Result> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(message));
    }, 1_000);
    void promise.then(
      (result) => {
        clearTimeout(timer);
        resolve(result);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

function wireLine(value: unknown): string {
  return `${JSON.stringify(value, wireReplacer)}\n`;
}

function wireReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return { [BYTES_MARKER]: Buffer.from(value).toString("base64") };
  }
  return value;
}

function wireReviver(_key: string, value: unknown): unknown {
  if (
    typeof value === "object" &&
    value !== null &&
    BYTES_MARKER in value &&
    typeof (value as Record<string, unknown>)[BYTES_MARKER] === "string"
  ) {
    return Uint8Array.from(
      Buffer.from(
        (value as Record<string, string>)[BYTES_MARKER] ?? "",
        "base64",
      ),
    );
  }
  return value;
}

const ADAPTER_FIXTURE = String.raw`
const readline = require("node:readline");
const marker = "${BYTES_MARKER}";
const replacer = (_key, value) =>
  value instanceof Uint8Array
    ? { [marker]: Buffer.from(value).toString("base64") }
    : value;
const reviver = (_key, value) =>
  value && typeof value === "object" && typeof value[marker] === "string"
    ? Uint8Array.from(Buffer.from(value[marker], "base64"))
    : value;
const write = (message, callback) =>
  process.stdout.write(JSON.stringify(message, replacer) + "\n", callback);
const identity = ${JSON.stringify(identity(), wireReplacer)};
write({ type: "inbox-message", message: { channelMessage: { text: "preserved" } } });
write({
  type: "ready",
  processId: process.pid,
  identity: JSON.parse(JSON.stringify(identity), reviver),
  channel: {
    index: 1,
    name: "fixture-channel",
    secret: new Uint8Array(32).fill(2),
  },
});
const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on("line", (line) => {
  const request = JSON.parse(line, reviver);
  if (request.type === "idle" && process.argv.includes("exit-on-idle")) {
    process.exit(7);
  }
  if (request.type === "idle" && process.argv.includes("ignore-idle")) {
    return;
  }
  if (request.type === "send") {
    write({
      type: "datagram",
      datagram: {
        channel: 1,
        dataType: ${FIELDLINK_DATA_TYPE},
        snrDb: -7,
        pathLength: 255,
        bytes: request.bytes,
      },
    });
  }
  write({ type: "response", id: request.id, ok: true }, () => {
    if (request.type === "close") {
      lines.close();
      process.stdin.destroy();
      if (process.argv.includes("exit-on-close")) {
        process.exit(0);
      }
    }
  });
});
`;
