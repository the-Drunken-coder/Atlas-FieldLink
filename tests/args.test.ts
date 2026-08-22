import { describe, expect, it } from "vitest";

import { parseCommand } from "../src/args.js";

describe("CLI arguments", () => {
  it("parses radios list", () => {
    expect(parseCommand(["radios", "list"])).toEqual({ name: "list" });
  });

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
        "--allow-inbox-drain",
      ]),
    ).toMatchObject({
      name: "bench",
      channel: 1,
      count: 100,
      payloadSize: 64,
      timeoutMs: 30_000,
      allowInboxDrain: true,
    });
  });

  it("uses the fixed ping datagram size and default timeout", () => {
    expect(
      parseCommand([
        "ping",
        "--a",
        "/dev/a",
        "--b",
        "/dev/b",
        "--channel",
        "1",
        "--count",
        "10",
        "--allow-inbox-drain",
      ]),
    ).toMatchObject({
      name: "ping",
      payloadSize: 16,
      timeoutMs: 30_000,
    });
  });

  it("requires explicit permission to drain both Companion inboxes", () => {
    expect(() =>
      parseCommand([
        "ping",
        "--a",
        "/dev/a",
        "--b",
        "/dev/b",
        "--channel",
        "1",
        "--count",
        "1",
      ]),
    ).toThrow("--allow-inbox-drain is required");
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
        "--allow-inbox-drain",
      ]),
    ).toThrow("different serial ports");
  });
});
