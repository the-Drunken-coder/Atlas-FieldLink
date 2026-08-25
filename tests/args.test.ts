import { describe, expect, it } from "vitest";

import { parseCommand } from "../src/args.js";
import { FIELDLINK_MAX_MESSAGE_BYTES } from "../src/frame.js";

describe("CLI arguments", () => {
  it("parses the three commands", () => {
    expect(parseCommand(["radios", "list"])).toEqual({
      name: "list-radios",
      json: false,
    });
    expect(parseCommand(["messages", "list", "--json"])).toEqual({
      name: "list-messages",
      json: true,
    });
    expect(
      parseCommand([
        "adapter",
        "--radio",
        "/dev/cu.a",
        "--channel",
        "2",
        "--output",
        "out",
        "--allow-inbox-drain",
      ]),
    ).toEqual({
      name: "adapter",
      radio: "/dev/cu.a",
      channel: 2,
      allowInboxDrain: true,
      evidenceManagedByParent: false,
      output: "out",
    });
    expect(
      parseCommand([
        "test",
        "--a",
        "/dev/cu.a",
        "--b",
        "/dev/cu.b",
        "--channel",
        "2",
        "--allow-inbox-drain",
      ]),
    ).toEqual({
      name: "test",
      message: "test",
      a: "/dev/cu.a",
      b: "/dev/cu.b",
      channel: 2,
      payloadSize: 64,
      retryStrategy: "selective-window",
      timeoutMs: 1_800_000,
      allowInboxDrain: true,
    });
  });

  it("accepts the maximum Test payload and rejects invalid values", () => {
    const base = [
      "test",
      "--a",
      "a",
      "--b",
      "b",
      "--channel",
      "7",
      "--allow-inbox-drain",
    ];
    expect(
      parseCommand([
        ...base,
        "--payload-size",
        String(FIELDLINK_MAX_MESSAGE_BYTES - 5),
        "--timeout-ms",
        "1",
        "--output",
        "out",
      ]),
    ).toMatchObject({
      payloadSize: FIELDLINK_MAX_MESSAGE_BYTES - 5,
      timeoutMs: 1,
      output: "out",
    });
    expect(() =>
      parseCommand([
        ...base,
        "--payload-size",
        String(FIELDLINK_MAX_MESSAGE_BYTES - 4),
      ]),
    ).toThrow("--payload-size");
    expect(() => parseCommand([...base, "--retry-strategy", "magic"])).toThrow(
      "selective-window",
    );
    expect(() => parseCommand([...base, "--message", "missing"])).toThrow(
      "--message must be one of: test",
    );
  });

  it("selects a shared channel automatically unless explicitly overridden", () => {
    const base = ["test", "--a", "a", "--b", "b", "--allow-inbox-drain"];
    expect(parseCommand(base)).toMatchObject({ channel: "auto" });
    expect(parseCommand([...base, "--channel", "auto"])).toMatchObject({
      channel: "auto",
    });
    expect(parseCommand([...base, "--channel", "3"])).toMatchObject({
      channel: 3,
    });
    expect(parseCommand([...base, "--channel", "39"])).toMatchObject({
      channel: 39,
    });
    expect(() => parseCommand([...base, "--channel", "256"])).toThrow(
      "between 0 and 255",
    );
  });

  it("requires distinct ports and explicit inbox-drain acknowledgement", () => {
    expect(() =>
      parseCommand([
        "test",
        "--a",
        "same",
        "--b",
        "same",
        "--channel",
        "1",
        "--allow-inbox-drain",
      ]),
    ).toThrow("different serial ports");
    expect(() =>
      parseCommand(["adapter", "--radio", "a", "--channel", "1"]),
    ).toThrow("--allow-inbox-drain");
    expect(() =>
      parseCommand([
        "adapter",
        "--radio",
        "a",
        "--channel",
        "1",
        "--allow-inbox-drain",
      ]),
    ).toThrow("--output");
    expect(() =>
      parseCommand([
        "adapter",
        "--radio",
        "a",
        "--channel",
        "1",
        "--evidence-managed-by-parent",
        "--allow-inbox-drain",
      ]),
    ).toThrow("--output");
    expect(
      parseCommand([
        "adapter",
        "--radio",
        "a",
        "--channel",
        "1",
        "--evidence-managed-by-parent",
        "--output",
        "out",
        "--allow-inbox-drain",
      ]),
    ).toMatchObject({ evidenceManagedByParent: true, output: "out" });
  });
});
