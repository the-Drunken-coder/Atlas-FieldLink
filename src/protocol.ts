export const FIELDLINK_HEADER_BYTES = 12;
export const FIELDLINK_MAX_DATAGRAM_BYTES = 163;
export const FIELDLINK_PING_DATAGRAM_BYTES = 16;

const MAGIC_0 = 0x46;
const MAGIC_1 = 0x4c;
const VERSION = 1;

export const DatagramKind = {
  request: 1,
  response: 2,
} as const;

export type DatagramKind = (typeof DatagramKind)[keyof typeof DatagramKind];

export interface DecodedDatagram {
  readonly kind: DatagramKind;
  readonly runId: number;
  readonly sequence: number;
  readonly bytes: Uint8Array;
}

export function encodeDatagram(
  kind: DatagramKind,
  runId: number,
  sequence: number,
  byteLength: number,
): Uint8Array {
  if (
    byteLength < FIELDLINK_HEADER_BYTES ||
    byteLength > FIELDLINK_MAX_DATAGRAM_BYTES
  ) {
    throw new RangeError(
      `Datagram size must be ${FIELDLINK_HEADER_BYTES}-${FIELDLINK_MAX_DATAGRAM_BYTES} bytes`,
    );
  }

  const bytes = new Uint8Array(byteLength);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  bytes[0] = MAGIC_0;
  bytes[1] = MAGIC_1;
  bytes[2] = VERSION;
  bytes[3] = kind;
  view.setUint32(4, runId, true);
  view.setUint32(8, sequence, true);

  for (let index = FIELDLINK_HEADER_BYTES; index < bytes.length; index += 1) {
    bytes[index] = patternByte(runId, sequence, index);
  }

  return bytes;
}

export function decodeDatagram(bytes: Uint8Array): DecodedDatagram | null {
  if (
    bytes.length < FIELDLINK_HEADER_BYTES ||
    bytes[0] !== MAGIC_0 ||
    bytes[1] !== MAGIC_1 ||
    bytes[2] !== VERSION
  ) {
    return null;
  }

  const kind = bytes[3];
  if (kind !== DatagramKind.request && kind !== DatagramKind.response) {
    return null;
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    kind,
    runId: view.getUint32(4, true),
    sequence: view.getUint32(8, true),
    bytes,
  };
}

export function verifyDatagram(
  datagram: DecodedDatagram,
  expectedKind: DatagramKind,
  expectedRunId: number,
  expectedSequence: number,
  expectedByteLength: number,
): boolean {
  if (
    datagram.kind !== expectedKind ||
    datagram.runId !== expectedRunId ||
    datagram.sequence !== expectedSequence ||
    datagram.bytes.length !== expectedByteLength
  ) {
    return false;
  }

  for (
    let index = FIELDLINK_HEADER_BYTES;
    index < datagram.bytes.length;
    index += 1
  ) {
    if (
      datagram.bytes[index] !==
      patternByte(expectedRunId, expectedSequence, index)
    ) {
      return false;
    }
  }

  return true;
}

function patternByte(runId: number, sequence: number, index: number): number {
  return (runId ^ sequence ^ Math.imul(index, 31)) & 0xff;
}
