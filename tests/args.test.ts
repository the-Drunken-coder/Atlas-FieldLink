import { describe, expect, it } from "vitest";

import { parseCommand } from "../src/args.js";

describe("CLI arguments", () => {
  it("parses the required benchmark command", () => {
    expect(
      parseCommand([
        "bench",
        "--a",
        "/dev/a",
        "--b",
        "/dev/b",
        "--channel",
        "1",
        "--count",
        "100",
        "--payload-size",
        "64",
      ]),
    ).toMatchObject({
      name: "bench",
      channel: 1,
      count: 100,
      payloadSize: 64,
      timeoutMs: 30_000,
    });
  });

  it("rejects the same serial port for both radios", () => {
    expect(() =>
      parseCommand([
        "ping",
        "--a",
        "/dev/radio",
        "--b",
        "/dev/radio",
        "--channel",
        "1",
        "--count",
        "1",
      ]),
    ).toThrow("different serial ports");
  });
});
