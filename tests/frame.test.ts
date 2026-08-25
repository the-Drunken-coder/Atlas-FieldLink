import { describe, expect, it } from "vitest";

import {
  COMPLETE_MESSAGE_BODY_BYTES,
  decodeFrame,
  encodeFrame,
  FIELDLINK_FRAME_HEADER_BYTES,
  FrameKind,
  MESHCORE_DATAGRAM_BYTES,
  TRANSFER_FRAGMENT_BYTES,
  type FieldLinkFrame,
} from "../src/frame.js";
import { parseNodeId } from "../src/node-types.js";

const source = parseNodeId("0011223344556677");
const destination = parseNodeId("8899aabbccddeeff");
const base = { transmissionId: 7, source, destination } as const;

describe("FieldLink frames", () => {
  it("places the frame kind directly after the magic", () => {
    const encoded = encodeFrame({
      ...base,
      kind: FrameKind.completion,
      logicalId: 1n,
    });

    expect(FIELDLINK_FRAME_HEADER_BYTES).toBe(29);
    expect(encoded.slice(0, 3)).toEqual(
      Uint8Array.of(0x46, 0x4c, FrameKind.completion),
    );
    expect(encoded.slice(3, 5)).toEqual(Uint8Array.of(7, 0));
  });

  it.each<FieldLinkFrame>([
    {
      ...base,
      kind: FrameKind.complete,
      logicalId: 1n,
      messageType: 1,
      body: Uint8Array.of(1, 2),
    },
    {
      ...base,
      kind: FrameKind.transferStart,
      logicalId: 2n,
      messageType: 1,
      totalLength: 200,
      fragmentCount: 2,
      fragmentSize: TRANSFER_FRAGMENT_BYTES,
      digest: new Uint8Array(32),
      retryStrategy: 1,
    },
    {
      ...base,
      kind: FrameKind.fragment,
      logicalId: 2n,
      fragmentIndex: 0,
      body: new Uint8Array(TRANSFER_FRAGMENT_BYTES),
    },
    {
      ...base,
      kind: FrameKind.receiptRequest,
      logicalId: 2n,
      windowStart: 0,
      windowCount: 2,
    },
    {
      ...base,
      kind: FrameKind.receipt,
      logicalId: 2n,
      windowStart: 0,
      windowCount: 2,
      bitmap: 3,
    },
    { ...base, kind: FrameKind.transferReady, logicalId: 2n },
    { ...base, kind: FrameKind.completion, logicalId: 2n },
    { ...base, kind: FrameKind.rejection, logicalId: 2n, code: 3 },
    { ...base, kind: FrameKind.cancellation, logicalId: 2n, code: 4 },
  ])("round-trips kind $kind", (frame) => {
    expect(decodeFrame(encodeFrame(frame))).toEqual(frame);
  });

  it("fills the exact 163-byte complete and fragment thresholds", () => {
    const complete = encodeFrame({
      ...base,
      kind: FrameKind.complete,
      logicalId: 1n,
      messageType: 1,
      body: new Uint8Array(COMPLETE_MESSAGE_BODY_BYTES),
    });
    const fragment = encodeFrame({
      ...base,
      kind: FrameKind.fragment,
      logicalId: 2n,
      fragmentIndex: 0,
      body: new Uint8Array(TRANSFER_FRAGMENT_BYTES),
    });
    expect(complete).toHaveLength(MESHCORE_DATAGRAM_BYTES);
    expect(fragment).toHaveLength(MESHCORE_DATAGRAM_BYTES);
  });

  it("rejects malformed, unknown-kind, and oversized frames", () => {
    expect(() => decodeFrame(new Uint8Array(2))).toThrow("shorter");
    const valid = encodeFrame({
      ...base,
      kind: FrameKind.completion,
      logicalId: 1n,
    });
    valid[2] = 99;
    expect(() => decodeFrame(valid)).toThrow("kind");
    expect(() =>
      encodeFrame({
        ...base,
        kind: FrameKind.complete,
        logicalId: 1n,
        messageType: 1,
        body: new Uint8Array(COMPLETE_MESSAGE_BODY_BYTES + 1),
      }),
    ).toThrow("does not fit");
  });

  it("rejects invalid receipt window counts", () => {
    const frames: FieldLinkFrame[] = [
      {
        ...base,
        kind: FrameKind.receiptRequest,
        logicalId: 2n,
        windowStart: 0,
        windowCount: 1,
      },
      {
        ...base,
        kind: FrameKind.receipt,
        logicalId: 2n,
        windowStart: 0,
        windowCount: 1,
        bitmap: 1,
      },
    ];
    for (const frame of frames) {
      const encoded = encodeFrame(frame);
      encoded[FIELDLINK_FRAME_HEADER_BYTES + 2] = 0;

      expect(() => decodeFrame(encoded)).toThrow(
        "window count must be between 1 and 8",
      );
    }
  });
});
