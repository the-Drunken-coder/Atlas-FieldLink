import { nodeIdFromBytes, nodeIdToBytes, type NodeId } from "./node-types.js";

export const MESHCORE_DATAGRAM_BYTES = 163;
export const FIELDLINK_MAX_MESSAGE_BYTES = 1024 * 1024;
export const FIELDLINK_FRAME_HEADER_BYTES = 29;
export const COMPLETE_MESSAGE_BODY_BYTES =
  MESHCORE_DATAGRAM_BYTES - FIELDLINK_FRAME_HEADER_BYTES - 2;
export const TRANSFER_FRAGMENT_BYTES = COMPLETE_MESSAGE_BODY_BYTES;

const MAGIC_0 = 0x46;
const MAGIC_1 = 0x4c;

export enum FrameKind {
  complete = 1,
  transferStart = 2,
  transferReady = 3,
  fragment = 4,
  receiptRequest = 5,
  receipt = 6,
  completion = 7,
  rejection = 8,
  cancellation = 9,
}

interface FrameBase {
  readonly transmissionId: number;
  readonly source: NodeId;
  readonly destination: NodeId;
  readonly logicalId: bigint;
}

export type FieldLinkFrame =
  | (FrameBase & {
      readonly kind: FrameKind.complete;
      readonly messageType: number;
      readonly body: Uint8Array;
    })
  | (FrameBase & {
      readonly kind: FrameKind.transferStart;
      readonly messageType: number;
      readonly totalLength: number;
      readonly fragmentCount: number;
      readonly fragmentSize: number;
      readonly digest: Uint8Array;
      readonly retryStrategy: number;
    })
  | (FrameBase & { readonly kind: FrameKind.transferReady })
  | (FrameBase & {
      readonly kind: FrameKind.fragment;
      readonly fragmentIndex: number;
      readonly body: Uint8Array;
    })
  | (FrameBase & {
      readonly kind: FrameKind.receiptRequest;
      readonly windowStart: number;
      readonly windowCount: number;
    })
  | (FrameBase & {
      readonly kind: FrameKind.receipt;
      readonly windowStart: number;
      readonly windowCount: number;
      readonly bitmap: number;
    })
  | (FrameBase & { readonly kind: FrameKind.completion })
  | (FrameBase & {
      readonly kind: FrameKind.rejection;
      readonly code: number;
    })
  | (FrameBase & {
      readonly kind: FrameKind.cancellation;
      readonly code: number;
    });

export class FrameDecodeError extends Error {}

export function encodeFrame(frame: FieldLinkFrame): Uint8Array {
  const body = encodeFrameBody(frame);
  const bytes = new Uint8Array(FIELDLINK_FRAME_HEADER_BYTES + body.length);
  if (bytes.length > MESHCORE_DATAGRAM_BYTES) {
    throw new RangeError(
      `FieldLink frame is ${bytes.length} bytes; maximum is ${MESHCORE_DATAGRAM_BYTES}`,
    );
  }
  const view = new DataView(bytes.buffer);
  assertUint16(frame.transmissionId, "transmission ID");
  view.setUint8(0, MAGIC_0);
  view.setUint8(1, MAGIC_1);
  view.setUint8(2, frame.kind);
  view.setUint16(3, frame.transmissionId, true);
  bytes.set(nodeIdToBytes(frame.source), 5);
  bytes.set(nodeIdToBytes(frame.destination), 13);
  view.setBigUint64(21, frame.logicalId, true);
  bytes.set(body, FIELDLINK_FRAME_HEADER_BYTES);
  return bytes;
}

export function decodeFrame(bytes: Uint8Array): FieldLinkFrame {
  if (bytes.length < FIELDLINK_FRAME_HEADER_BYTES) {
    throw new FrameDecodeError("FieldLink frame is shorter than its header");
  }
  if (bytes.length > MESHCORE_DATAGRAM_BYTES) {
    throw new FrameDecodeError("FieldLink frame exceeds the MeshCore limit");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint8(0) !== MAGIC_0 || view.getUint8(1) !== MAGIC_1) {
    throw new FrameDecodeError("FieldLink magic does not match");
  }
  const kind = view.getUint8(2);
  if (!isFrameKind(kind)) {
    throw new FrameDecodeError(`Unknown FieldLink frame kind ${kind}`);
  }
  const base: FrameBase = {
    transmissionId: view.getUint16(3, true),
    source: nodeIdFromBytes(bytes.slice(5, 13)),
    destination: nodeIdFromBytes(bytes.slice(13, 21)),
    logicalId: view.getBigUint64(21, true),
  };
  const offset = FIELDLINK_FRAME_HEADER_BYTES;
  switch (kind) {
    case FrameKind.complete:
      requireLength(bytes, offset + 2, "complete frame");
      return {
        ...base,
        kind,
        messageType: view.getUint16(offset, true),
        body: bytes.slice(offset + 2),
      };
    case FrameKind.transferStart:
      requireExactLength(bytes, offset + 43, "transfer start");
      return {
        ...base,
        kind,
        messageType: view.getUint16(offset, true),
        totalLength: view.getUint32(offset + 2, true),
        fragmentCount: view.getUint16(offset + 6, true),
        fragmentSize: view.getUint16(offset + 8, true),
        digest: bytes.slice(offset + 10, offset + 42),
        retryStrategy: view.getUint8(offset + 42),
      };
    case FrameKind.transferReady:
    case FrameKind.completion:
      requireExactLength(bytes, offset, "control frame");
      return { ...base, kind };
    case FrameKind.fragment:
      requireLength(bytes, offset + 2, "fragment");
      return {
        ...base,
        kind,
        fragmentIndex: view.getUint16(offset, true),
        body: bytes.slice(offset + 2),
      };
    case FrameKind.receiptRequest:
      requireExactLength(bytes, offset + 3, "receipt request");
      return {
        ...base,
        kind,
        windowStart: view.getUint16(offset, true),
        windowCount: view.getUint8(offset + 2),
      };
    case FrameKind.receipt:
      requireExactLength(bytes, offset + 4, "receipt");
      return {
        ...base,
        kind,
        windowStart: view.getUint16(offset, true),
        windowCount: view.getUint8(offset + 2),
        bitmap: view.getUint8(offset + 3),
      };
    case FrameKind.rejection:
    case FrameKind.cancellation:
      requireExactLength(bytes, offset + 1, "control frame");
      return { ...base, kind, code: view.getUint8(offset) };
  }
}

function isFrameKind(value: number): value is FrameKind {
  switch (value) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
    case 9:
      return true;
    default:
      return false;
  }
}

function encodeFrameBody(frame: FieldLinkFrame): Uint8Array {
  switch (frame.kind) {
    case FrameKind.complete: {
      assertUint16(frame.messageType, "message type");
      if (frame.body.length > COMPLETE_MESSAGE_BODY_BYTES) {
        throw new RangeError("Complete message body does not fit in one frame");
      }
      const bytes = new Uint8Array(2 + frame.body.length);
      new DataView(bytes.buffer).setUint16(0, frame.messageType, true);
      bytes.set(frame.body, 2);
      return bytes;
    }
    case FrameKind.transferStart: {
      assertUint16(frame.messageType, "message type");
      assertUint32(frame.totalLength, "total length");
      assertUint16(frame.fragmentCount, "fragment count");
      assertUint16(frame.fragmentSize, "fragment size");
      assertByte(frame.retryStrategy, "retry strategy");
      if (frame.digest.length !== 32) {
        throw new RangeError("Transfer digest must be 32 bytes");
      }
      const bytes = new Uint8Array(43);
      const view = new DataView(bytes.buffer);
      view.setUint16(0, frame.messageType, true);
      view.setUint32(2, frame.totalLength, true);
      view.setUint16(6, frame.fragmentCount, true);
      view.setUint16(8, frame.fragmentSize, true);
      bytes.set(frame.digest, 10);
      view.setUint8(42, frame.retryStrategy);
      return bytes;
    }
    case FrameKind.transferReady:
    case FrameKind.completion:
      return new Uint8Array();
    case FrameKind.fragment: {
      assertUint16(frame.fragmentIndex, "fragment index");
      if (frame.body.length > TRANSFER_FRAGMENT_BYTES) {
        throw new RangeError("Transfer fragment body is too large");
      }
      const bytes = new Uint8Array(2 + frame.body.length);
      new DataView(bytes.buffer).setUint16(0, frame.fragmentIndex, true);
      bytes.set(frame.body, 2);
      return bytes;
    }
    case FrameKind.receiptRequest: {
      assertUint16(frame.windowStart, "window start");
      assertWindowCount(frame.windowCount);
      const bytes = new Uint8Array(3);
      const view = new DataView(bytes.buffer);
      view.setUint16(0, frame.windowStart, true);
      view.setUint8(2, frame.windowCount);
      return bytes;
    }
    case FrameKind.receipt: {
      assertUint16(frame.windowStart, "window start");
      assertWindowCount(frame.windowCount);
      assertByte(frame.bitmap, "receipt bitmap");
      const bytes = new Uint8Array(4);
      const view = new DataView(bytes.buffer);
      view.setUint16(0, frame.windowStart, true);
      view.setUint8(2, frame.windowCount);
      view.setUint8(3, frame.bitmap);
      return bytes;
    }
    case FrameKind.rejection:
    case FrameKind.cancellation:
      assertByte(frame.code, "control code");
      return Uint8Array.of(frame.code);
  }
}

function requireLength(bytes: Uint8Array, minimum: number, name: string): void {
  if (bytes.length < minimum) {
    throw new FrameDecodeError(`${name} is truncated`);
  }
}

function requireExactLength(
  bytes: Uint8Array,
  length: number,
  name: string,
): void {
  if (bytes.length !== length) {
    throw new FrameDecodeError(`${name} has an invalid length`);
  }
}

function assertUint16(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
    throw new RangeError(`${name} must be a uint16`);
  }
}

function assertUint32(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new RangeError(`${name} must be a uint32`);
  }
}

function assertByte(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xff) {
    throw new RangeError(`${name} must be a byte`);
  }
}

function assertWindowCount(value: number): void {
  if (!Number.isInteger(value) || value < 1 || value > 8) {
    throw new RangeError("window count must be between 1 and 8");
  }
}
