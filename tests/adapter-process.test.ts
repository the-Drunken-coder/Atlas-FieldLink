import { PassThrough } from "node:stream";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";

import {
  AdapterProcessNode,
  filteredExecArguments,
  serveAdapter,
} from "../src/adapter-process.js";
import { encodeFrame, FrameKind } from "../src/frame.js";
import { testMessage } from "../src/messages/test.js";
import { FieldLinkNode, parseNodeId } from "../src/node.js";
import { MemoryTransport } from "./helpers.js";

const nodeA = parseNodeId("aaaaaaaaaaaaaaaa");
const nodeB = parseNodeId("bbbbbbbbbbbbbbbb");

describe("NDJSON adapter server", () => {
  it("reports safe ready metadata and carries typed bytes as base64", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const transport = new MemoryTransport();
    const node = new FieldLinkNode({ nodeId: nodeB, transport });
    const reader = lineReader(output);
    const serving = serveAdapter({
      path: "/dev/cu.test",
      channel: 2,
      input,
      output,
      processId: 123,
      createRuntime: () =>
        Promise.resolve({
          node,
          ready: ready(123),
        }),
    });
    expect(await reader.nextType("ready")).toMatchObject({
      processId: 123,
      nodeId: nodeB,
      supportedMessages: [{ id: 1, name: "test" }],
      retryStrategies: [{ id: 1, name: "selective-window" }],
    });
    input.write(
      `${JSON.stringify({
        id: 1,
        type: "send",
        message: {
          type: "test",
          kind: "response",
          correlationId: 7,
          payload: { $fieldlinkBytes: "AP8=" },
        },
        destination: nodeA,
      })}\n`,
    );
    expect(await reader.nextType("response")).toMatchObject({
      id: 1,
      ok: true,
      result: { delivery: "complete", encodedBytes: 7 },
    });

    transport.inject({
      bytes: encodeFrame({
        transmissionId: 1,
        kind: FrameKind.complete,
        source: nodeA,
        destination: nodeB,
        logicalId: 5n,
        messageType: 1,
        body: testMessage.encode({
          type: "test",
          kind: "response",
          correlationId: 9,
          payload: Uint8Array.of(1, 2, 3),
        }),
      }),
    });
    const message = await reader.nextType("message");
    expect(message).toMatchObject({
      message: {
        message: {
          type: "test",
          kind: "response",
          correlationId: 9,
          payload: { $fieldlinkBytes: "AQID" },
        },
      },
    });
    input.write(`${JSON.stringify({ id: 2, type: "close" })}\n`);
    expect(await reader.nextType("response")).toMatchObject({
      id: 2,
      ok: true,
    });
    input.end();
    await serving;
    reader.close();
  });

  it("keeps every stdout line valid JSON", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const lines: string[] = [];
    output.setEncoding("utf8");
    output.on("data", (chunk: string) =>
      lines.push(...chunk.trim().split("\n")),
    );
    const node = new FieldLinkNode({
      nodeId: nodeB,
      transport: new MemoryTransport(),
    });
    const serving = serveAdapter({
      path: "test",
      channel: 1,
      input,
      output,
      createRuntime: () => Promise.resolve({ node, ready: ready(1) }),
    });
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
    input.write('{"id":1,"type":"close"}\n');
    input.end();
    await serving;
    expect(lines.length).toBeGreaterThan(0);
    expect(() => {
      for (const line of lines) {
        JSON.parse(line);
      }
    }).not.toThrow();
  });
});

describe("adapter process proxy", () => {
  it("sends typed messages and closes cooperatively", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(cooperativeChildScript()),
    });
    const result = await adapter.send(
      {
        type: "test",
        kind: "response",
        correlationId: 1,
        payload: Uint8Array.of(1, 2),
      },
      { destination: nodeB },
    );
    expect(result).toMatchObject({ delivery: "complete", encodedBytes: 7 });
    await expect(adapter.close()).resolves.toBeUndefined();
  });

  it("rejects pending work when the child exits", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(
        `${writeReady()} process.stdin.once("data",()=>process.exit(7));`,
      ),
    });
    await expect(
      adapter.send(
        {
          type: "test",
          kind: "response",
          correlationId: 1,
          payload: new Uint8Array(),
        },
        { destination: nodeB },
      ),
    ).rejects.toThrow("code 7");
  });

  it("drains the final response before treating child exit as failure", async () => {
    const script = `${writeReady()}
process.stdin.once("data",chunk=>{const request=JSON.parse(String(chunk).trim());const response={type:"response",id:request.id,ok:true,result:{logicalId:"0000000000000001",messageType:1,messageName:"test",destination:request.destination,priority:"normal",delivery:"complete",encodedBytes:5,fragments:1,retransmissions:0,receipts:0,durationMs:1}};process.stdout.write(JSON.stringify(response)+"\\n",()=>process.exit(0));});`;
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(script),
    });
    await expect(
      adapter.send(
        {
          type: "test",
          kind: "response",
          correlationId: 1,
          payload: new Uint8Array(),
        },
        { destination: nodeB },
      ),
    ).resolves.toMatchObject({ delivery: "complete" });
  });

  it("propagates abort and persistent request timeout", async () => {
    const aborting = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(abortChildScript()),
    });
    const controller = new AbortController();
    const send = aborting.send(
      {
        type: "test",
        kind: "response",
        correlationId: 1,
        payload: new Uint8Array(),
      },
      { destination: nodeB, signal: controller.signal },
    );
    controller.abort(new Error("stop"));
    await expect(send).rejects.toThrow("aborted");
    await aborting.close();

    const timingOut = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      requestTimeoutMs: 10,
      program: nodeScript(
        `${writeReady()} process.stdin.on("end",()=>process.exit(0)); process.stdin.resume();`,
      ),
    });
    await expect(
      timingOut.send(
        {
          type: "test",
          kind: "response",
          correlationId: 1,
          payload: new Uint8Array(),
        },
        { destination: nodeB },
      ),
    ).rejects.toThrow("timed out");
    await expect(timingOut.close()).rejects.toThrow("timed out");
  });

  it("removes controller-only execution flags from child arguments", () => {
    expect(
      filteredExecArguments([
        "--import",
        "tsx",
        "--inspect=9229",
        "--watch-path",
        "src",
        "--conditions=development",
      ]),
    ).toEqual(["--import", "tsx", "--conditions=development"]);
  });
});

function ready(processId: number) {
  return {
    processId,
    identity: {
      nodeId: nodeB,
      fingerprint: nodeB,
      name: "test",
      model: "fake",
      firmwareVersion: "1",
      firmwareBuildDate: "2026-01-01",
      firmwareProtocolCode: 12,
      clientProtocolVersion: 1,
      radio: {
        frequency: 915_000_000,
        bandwidth: 250_000,
        spreadingFactor: 10,
        codingRate: 5,
        transmitPower: 10,
        maximumTransmitPower: 22,
      },
    },
    channel: {
      index: 1,
      name: "test",
      configured: true,
      keyFingerprint: "0011223344556677",
    },
    nodeId: nodeB,
    supportedMessages: [
      { id: 1, name: "test", defaultPriority: "normal" as const },
    ],
    retryStrategies: [{ id: 1, name: "selective-window" as const }],
    delivery: {
      meshCoreDataType: 0xffff,
      meshCoreMode: "flood" as const,
      maximumChannelDatagramBytes: 163 as const,
    },
  };
}

function lineReader(output: PassThrough) {
  const lines = createInterface({ input: output, crlfDelay: Infinity });
  const iterator = lines[Symbol.asyncIterator]();
  return {
    async nextType(type: string): Promise<Record<string, unknown>> {
      for (;;) {
        const next = await iterator.next();
        if (next.done) {
          throw new Error(`stdout ended before ${type}`);
        }
        const value = JSON.parse(next.value) as Record<string, unknown>;
        if (value.type === type) {
          return value;
        }
      }
    },
    close: () => {
      lines.close();
    },
  };
}

function nodeScript(source: string) {
  return { executable: process.execPath, arguments: ["-e", source] };
}

function writeReady(): string {
  return `process.stdout.write(${JSON.stringify(`${JSON.stringify({ type: "ready", ...ready(999) })}\n`)});`;
}

function cooperativeChildScript(): string {
  return `${writeReady()}
let pending="";
process.stdin.setEncoding("utf8");
process.stdin.on("data",chunk=>{pending+=chunk;let i;while((i=pending.indexOf("\\n"))>=0){const line=pending.slice(0,i);pending=pending.slice(i+1);if(!line)continue;const request=JSON.parse(line);if(request.type==="send"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true,result:{logicalId:"0000000000000001",messageType:1,messageName:"test",destination:request.destination,priority:"normal",delivery:"complete",encodedBytes:7,fragments:1,retransmissions:0,receipts:0,durationMs:1}})+"\\n");}else if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n",()=>process.exit(0));}}});`;
}

function abortChildScript(): string {
  return `${writeReady()}
let pending="",sendId;
process.stdin.setEncoding("utf8");
process.stdin.on("data",chunk=>{pending+=chunk;let i;while((i=pending.indexOf("\\n"))>=0){const line=pending.slice(0,i);pending=pending.slice(i+1);if(!line)continue;const request=JSON.parse(line);if(request.type==="send"){sendId=request.id;}else if(request.type==="abort"){process.stdout.write(JSON.stringify({type:"response",id:sendId,ok:false,error:"Adapter request aborted"})+"\\n");}else if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n",()=>process.exit(0));}}});`;
}
