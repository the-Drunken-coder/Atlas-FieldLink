import { createHash } from "node:crypto";

export type Priority = "high" | "normal" | "bulk";
export type NodeId = string & { readonly __nodeId: unique symbol };

const NODE_ID_PATTERN = /^[0-9a-f]{16}$/;

export function nodeIdFromPublicKey(publicKey: Uint8Array): NodeId {
  if (publicKey.length === 0) {
    throw new Error("A MeshCore public key is required");
  }
  return createHash("sha256")
    .update(publicKey)
    .digest("hex")
    .slice(0, 16) as NodeId;
}

export function parseNodeId(value: string): NodeId {
  if (!NODE_ID_PATTERN.test(value)) {
    throw new Error("Node ID must be 16 lowercase hexadecimal characters");
  }
  return value as NodeId;
}

export function nodeIdToBytes(nodeId: NodeId): Uint8Array {
  return Uint8Array.from(Buffer.from(nodeId, "hex"));
}

export function nodeIdFromBytes(bytes: Uint8Array): NodeId {
  if (bytes.length !== 8) {
    throw new Error("Node ID must contain exactly 8 bytes");
  }
  return Buffer.from(bytes).toString("hex") as NodeId;
}
