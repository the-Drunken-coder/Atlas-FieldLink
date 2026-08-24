import { PassThrough } from "node:stream";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";

import {
  AdapterProcessNode,
  filteredExecArguments,
  serveAdapter,
  type StartAdapterProcessOptions,
} from "../src/adapter-process.js";
import { encodeFrame, FrameKind } from "../src/frame.js";
import { testMessage } from "../src/messages/test.js";
import { FieldLinkNode, parseNodeId } from "../src/node.js";
import type { InboxMessage } from "../src/radio.js";
import { eventually, MemoryTransport } from "./helpers.js";

const nodeA = parseNodeId("aaaaaaaaaaaaaaaa");
const nodeB = parseNodeId("bbbbbbbbbbbbbbbb");

describe("NDJSON adapter server", () => {
  it("reports safe ready metadata and carries typed bytes as base64", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const transport = new MemoryTransport();
    const node = new FieldLinkNode({ nodeId: nodeB, transport });
    let started = false;
    let activated = false;
    const preservedInbox: unknown[] = [];
    let emitInbox:
      ((message: InboxMessage) => void | Promise<void>) | undefined;
    const reader = lineReader(output);
    const serving = serveAdapter({
      path: "/dev/cu.test",
      channel: 2,
      input,
      output,
      processId: 123,
      onInboxMessage: (message) => {
        preservedInbox.push(message);
      },
      createRuntime: (options) => {
        emitInbox = options.onInboxMessage;
        return Promise.resolve({
          node,
          start: () => {
            started = true;
            return Promise.resolve();
          },
          activate: () => {
            activated = true;
            return Promise.resolve();
          },
          ready: ready(123),
        });
      },
    });
    expect(await reader.nextType("ready")).toMatchObject({
      processId: 123,
      nodeId: nodeB,
      supportedMessages: [{ id: 1, name: "test" }],
      retryStrategies: [{ id: 1, name: "selective-window" }],
    });
    expect(started).toBe(true);
    await emitInbox?.({
      channelMessage: {
        channelIdx: 2,
        pathLen: 1,
        txtType: 0,
        senderTimestamp: 1,
        text: "stale",
      },
    });
    expect(await reader.nextType("inbox-message")).toMatchObject({
      message: { channelMessage: { text: "stale" } },
    });
    expect(preservedInbox).toHaveLength(1);
    input.write(
      `${JSON.stringify({
        id: 1,
        type: "activate",
      })}\n`,
    );
    expect(await reader.nextType("response")).toMatchObject({
      id: 1,
      ok: true,
    });
    expect(activated).toBe(true);
    input.write(
      `${JSON.stringify({
        id: 2,
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
      id: 2,
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
    input.write(`${JSON.stringify({ id: 3, type: "close" })}\n`);
    expect(await reader.nextType("response")).toMatchObject({
      id: 3,
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
  it("requires runtime inbox-drain acknowledgement before spawning", async () => {
    const options = {
      path: "test",
      channel: 1,
      allowInboxDrain: false,
      program: nodeScript(cooperativeChildScript()),
    } as unknown as StartAdapterProcessOptions;

    await expect(AdapterProcessNode.start(options)).rejects.toThrow(
      "explicit inbox-drain acknowledgement",
    );
  });

  it("sends typed messages and closes cooperatively", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(cooperativeChildScript()),
    });
    await adapter.activate();
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

  it("cancels adapter activation and reaps the child", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      exitTimeoutMs: 10,
      program: nodeScript(`${writeReady()} process.stdin.resume();`),
    });
    const controller = new AbortController();
    const activation = adapter.activate(controller.signal);
    controller.abort(new Error("activation cancelled"));

    await expect(activation).rejects.toThrow("activation cancelled");
    await expect(adapter.close()).rejects.toThrow();
  });

  it("preserves startup inbox evidence without delivering stale messages", async () => {
    const inbox: unknown[] = [];
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 39,
      allowInboxDrain: true,
      onInboxMessage: (message) => {
        inbox.push(message);
      },
      program: nodeScript(preReadyEvidenceChildScript()),
    });
    const messages: unknown[] = [];
    adapter.onMessage((message) => {
      messages.push(message);
    });
    await adapter.activate();
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(adapter.channel.index).toBe(39);
    expect(inbox).toHaveLength(1);
    expect(messages).toHaveLength(0);
    await adapter.close();
  });

  it("reaps an adapter that fails before readiness", async () => {
    await expect(
      AdapterProcessNode.start({
        path: "test",
        channel: 1,
        allowInboxDrain: true,
        exitTimeoutMs: 10,
        program: nodeScript(malformedStartupChildScript()),
      }),
    ).rejects.toThrow();
  });

  it("rejects and reaps startup when its signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort(new Error("startup cancelled"));

    await expect(
      AdapterProcessNode.start({
        path: "test",
        channel: 1,
        allowInboxDrain: true,
        signal: controller.signal,
        exitTimeoutMs: 10,
        program: nodeScript(`${writeReady()} setInterval(()=>{},1000);`),
      }),
    ).rejects.toThrow("startup cancelled");
  });

  it("cancels and reaps an adapter while waiting for ready", async () => {
    const controller = new AbortController();
    let reportStarted = (): void => undefined;
    const started = new Promise<void>((resolve) => {
      reportStarted = resolve;
    });
    const starting = AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      signal: controller.signal,
      exitTimeoutMs: 10,
      onStderr: (message) => {
        if (message.includes("started")) {
          reportStarted();
        }
      },
      program: nodeScript(uncooperativeStartupChildScript()),
    });
    const rejected = expect(starting).rejects.toThrow("startup cancelled");
    await started;

    controller.abort(new Error("startup cancelled"));

    await rejected;
  });

  it("waits for startup evidence callbacks before completing close", async () => {
    let releaseInbox = (): void => undefined;
    const inboxReleased = new Promise<void>((resolve) => {
      releaseInbox = resolve;
    });
    let inboxStarted = false;
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      onInboxMessage: async () => {
        inboxStarted = true;
        await inboxReleased;
      },
      program: nodeScript(closeWithBufferedInboxChildScript()),
    });

    let closed = false;
    const closing = adapter.close().then(() => {
      closed = true;
    });
    await eventually(() => inboxStarted);
    expect(closed).toBe(false);
    releaseInbox();
    await closing;
    expect(closed).toBe(true);
  });

  it("rejects pending work when the child exits", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(
        `${writeReady()} ${activateThen("process.exit(7);")}`,
      ),
    });
    await adapter.activate();
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

  it("fails and reaps an adapter whose stdout closes unexpectedly", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      requestTimeoutMs: 10_000,
      exitTimeoutMs: 100,
      program: nodeScript(closedStdoutChildScript()),
    });

    await expect(adapter.activate()).rejects.toThrow(
      "Adapter stdout ended unexpectedly",
    );
    await expect(adapter.close()).rejects.toThrow();
  });

  it("drains the final response before treating child exit as failure", async () => {
    const script = `${writeReady()}
${activateThen('const response={type:"response",id:request.id,ok:true,result:{logicalId:"0000000000000001",messageType:1,messageName:"test",destination:request.destination,priority:"normal",delivery:"complete",encodedBytes:5,fragments:1,retransmissions:0,receipts:0,durationMs:1}};process.stdout.write(JSON.stringify(response)+"\\n",()=>process.exit(0));')}`;
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(script),
    });
    await adapter.activate();
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
    await aborting.activate();
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
        `${writeReady()} ${activateThen("")} process.stdin.on("end",()=>process.exit(0)); process.stdin.resume();`,
      ),
    });
    await timingOut.activate();
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

  it("force-stops and reaps an adapter that ignores termination", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      exitTimeoutMs: 10,
      program: nodeScript(uncooperativeCloseChildScript()),
    });
    await adapter.activate();

    await expect(adapter.close()).rejects.toThrow(
      "Could not close adapter process",
    );
  });

  it("bounds an ignored close request with the exit timeout", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      requestTimeoutMs: 500,
      exitTimeoutMs: 10,
      program: nodeScript(ignoredCloseChildScript()),
    });
    await adapter.activate();

    await expect(adapter.close()).rejects.toThrow("timed out after 10 ms");
  });

  it("reports a nonzero adapter exit during close", async () => {
    const adapter = await AdapterProcessNode.start({
      path: "test",
      channel: 1,
      allowInboxDrain: true,
      program: nodeScript(nonzeroCloseChildScript()),
    });
    await adapter.activate();

    await expect(adapter.close()).rejects.toThrow("code 7");
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

function ready(processId: number, channelIndex = 1) {
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
      index: channelIndex,
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

function writeReady(channelIndex = 1): string {
  return `process.stdout.write(${JSON.stringify(`${JSON.stringify({ type: "ready", ...ready(999, channelIndex) })}\n`)});`;
}

function preReadyEvidenceChildScript(): string {
  const inbox = {
    type: "inbox-message",
    message: {
      channelMessage: {
        channelIdx: 39,
        pathLen: 1,
        txtType: 0,
        senderTimestamp: 1,
        text: "stale",
      },
    },
  };
  const stale = {
    type: "message",
    message: {
      message: {
        type: "test",
        kind: "response",
        correlationId: 1,
        payload: { $fieldlinkBytes: "" },
      },
      source: nodeA,
      destination: nodeB,
      logicalId: "0000000000000001",
      delivery: "complete",
      receivedAt: "2026-08-24T12:00:00.000Z",
    },
  };
  return `process.stdout.write(${JSON.stringify(`${JSON.stringify(inbox)}\n${JSON.stringify(stale)}\n${JSON.stringify({ type: "ready", ...ready(999, 39) })}\n`)});
${activateThen('if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n",()=>process.exit(0));}')}`;
}

function closeWithBufferedInboxChildScript(): string {
  const inbox = {
    type: "inbox-message",
    message: {
      channelMessage: {
        channelIdx: 1,
        pathLen: 1,
        txtType: 0,
        senderTimestamp: 1,
        text: "final",
      },
    },
  };
  return `${writeReady()}
process.stdin.once("data",chunk=>{const request=JSON.parse(String(chunk).trim());const lines=JSON.stringify({type:"response",id:request.id,ok:true})+"\\n"+${JSON.stringify(JSON.stringify(inbox))}+"\\n";process.stdout.write(lines,()=>process.exit(0));});`;
}

function cooperativeChildScript(): string {
  return `${writeReady()}
let pending="";
process.stdin.setEncoding("utf8");
process.stdin.on("data",chunk=>{pending+=chunk;let i;while((i=pending.indexOf("\\n"))>=0){const line=pending.slice(0,i);pending=pending.slice(i+1);if(!line)continue;const request=JSON.parse(line);if(request.type==="activate"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n");}else if(request.type==="send"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true,result:{logicalId:"0000000000000001",messageType:1,messageName:"test",destination:request.destination,priority:"normal",delivery:"complete",encodedBytes:7,fragments:1,retransmissions:0,receipts:0,durationMs:1}})+"\\n");}else if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n",()=>process.exit(0));}}});`;
}

function closedStdoutChildScript(): string {
  return `${writeReady()}
process.on("SIGTERM",()=>process.exit(0));
process.stdout.end();
setInterval(()=>{},1000);`;
}

function abortChildScript(): string {
  return `${writeReady()}
let pending="",sendId;
process.stdin.setEncoding("utf8");
process.stdin.on("data",chunk=>{pending+=chunk;let i;while((i=pending.indexOf("\\n"))>=0){const line=pending.slice(0,i);pending=pending.slice(i+1);if(!line)continue;const request=JSON.parse(line);if(request.type==="activate"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n");}else if(request.type==="send"){sendId=request.id;}else if(request.type==="abort"){process.stdout.write(JSON.stringify({type:"response",id:sendId,ok:false,error:"Adapter request aborted"})+"\\n");}else if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n",()=>process.exit(0));}}});`;
}

function activateThen(next: string): string {
  return `let pending="";
process.stdin.setEncoding("utf8");
process.stdin.on("data",chunk=>{pending+=chunk;let i;while((i=pending.indexOf("\\n"))>=0){const line=pending.slice(0,i);pending=pending.slice(i+1);if(!line)continue;const request=JSON.parse(line);if(request.type==="activate"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n");}else{${next}}}});`;
}

function uncooperativeCloseChildScript(): string {
  return `${writeReady()}
${activateThen('if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n");}')}
process.on("SIGTERM",()=>{});
setInterval(()=>{},1000);`;
}

function ignoredCloseChildScript(): string {
  return `${writeReady()}
${activateThen("")}
process.stdin.on("end",()=>process.exit(0));
process.stdin.resume();`;
}

function nonzeroCloseChildScript(): string {
  return `${writeReady()}
${activateThen('if(request.type==="close"){process.stdout.write(JSON.stringify({type:"response",id:request.id,ok:true})+"\\n",()=>process.exit(7));}')}`;
}

function malformedStartupChildScript(): string {
  return `process.on("SIGTERM",()=>{});
setInterval(()=>{},1000);
process.stdout.write("{malformed}\\n");`;
}

function uncooperativeStartupChildScript(): string {
  return `process.stderr.write("started\\n");
process.on("SIGTERM",()=>{});
setInterval(()=>{},1000);`;
}
