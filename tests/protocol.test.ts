import { describe, expect, it } from "vitest";

import {
  DatagramKind,
  FIELDLINK_HEADER_BYTES,
  FIELDLINK_MAX_DATAGRAM_BYTES,
  decodeDatagram,
  encodeDatagram,
  verifyDatagram,
} from "../src/protocol.js";

describe("FieldLink hardware-test datagrams", () => {
  it("round-trips the run, sequence, kind, size, and body pattern", () => {
    const bytes = encodeDatagram(DatagramKind.request, 0x1234_5678, 42, 64);
    const decoded = decodeDatagram(bytes);

    expect(bytes).toHaveLength(64);
    expect(decoded).not.toBeNull();
    expect(
      decoded === null
        ? false
        : verifyDatagram(decoded, DatagramKind.request, 0x1234_5678, 42, 64),
    ).toBe(true);
  });

  it("detects changed body bytes", () => {
    const bytes = encodeDatagram(DatagramKind.response, 9, 3, 32);
    bytes[FIELDLINK_HEADER_BYTES] = (bytes[FIELDLINK_HEADER_BYTES] ?? 0) ^ 0xff;
    const decoded = decodeDatagram(bytes);

    expect(decoded).not.toBeNull();
    expect(
      decoded === null
        ? true
        : verifyDatagram(decoded, DatagramKind.response, 9, 3, 32),
    ).toBe(false);
  });

  it("enforces the MeshCore channel-data limit", () => {
    expect(() =>
      encodeDatagram(DatagramKind.request, 1, 1, FIELDLINK_HEADER_BYTES - 1),
    ).toThrow(RangeError);
    expect(() =>
      encodeDatagram(
        DatagramKind.request,
        1,
        1,
        FIELDLINK_MAX_DATAGRAM_BYTES + 1,
      ),
    ).toThrow(RangeError);
  });
});
