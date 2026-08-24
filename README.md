# Atlas FieldLink

FieldLink delivers registered binary messages through one MeshCore Companion USB radio. The public module is `FieldLinkNode`. A thin NDJSON adapter exposes the same interface across a process boundary, and the local `fieldlink test` command starts two adapters to prove a real RF echo.

```text
Atlas-side caller
  -> FieldLinkNode or adapter process
  -> MeshCore Companion USB radio
  -> MeshCore channel and RF mesh
```

FieldLink does not flash firmware, write radio configuration, change channels, or replace MeshCore routing. It uses MeshCore channel data type `0xFFFF`, flood delivery, and the 163-byte channel-datagram limit.

## Requirements

- Node.js 24
- Python 3 for the optional terminal console
- MeshCore.js 1.13.0
- MeshCore Companion USB firmware with channel-data support
- A shared non-empty channel configured in the same slot on both radios
- Two dedicated radios with matching LoRa and channel settings for hardware testing

Install exactly from the lockfile:

```bash
npm ci
```

## FieldLinkNode

```ts
import { FieldLinkNode, type FieldLinkTransport } from "atlas-fieldlink";

async function sendTest(nodeId: string, transport: FieldLinkTransport) {
  const node = new FieldLinkNode({ nodeId, transport });

  const unsubscribe = node.onMessage((received) => {
    console.log(received.source, received.message);
  });

  try {
    return await node.send(
      {
        type: "test",
        kind: "request",
        correlationId: 1,
        payload: Uint8Array.of(1, 2, 3),
      },
      {
        destination: "0123456789abcdef",
        priority: "normal",
        retryStrategy: "selective-window",
      },
    );
  } finally {
    unsubscribe();
    await node.close();
  }
}
```

The module exposes this interface:

```ts
send(message, {
  destination,
  priority?,
  retryStrategy?,
  signal?,
}): Promise<SendResult>

onMessage(listener): () => void
onEvent(listener): () => void
close(): Promise<void>
```

A Node ID is the first eight bytes of the SHA-256 hash of a MeshCore public key, written as 16 lowercase hexadecimal characters. It is an address, not proof of identity. Any member of the MeshCore channel can spoof a FieldLink source or destination Node ID. FieldLink relies on MeshCore channel membership as its only sender trust.

## Messages

Message-specific behavior lives in one file under `src/messages/`. A message definition owns its stable `uint16` ID, name, default priority, runtime validation, binary codec, examples, hardware exercise, and optional inbound handler. `src/messages/test.ts` documents and implements the only registered message.

The explicit registry is `src/messages/index.ts`. Adding a message requires one new message file and one registry entry. Startup rejects duplicate IDs or names. Generic contract tests validate every registered example and codec round trip.

The hardware exercise constructs a representative message and recognizes successful end-to-end delivery. This keeps message-specific test input and completion rules in the message file while the CLI continues to own radios, transport, evidence, and timing.

Test has request and response variants. Both carry a `uint32` correlation ID and arbitrary bytes. A received request is echoed to its source with identical correlation and payload. A response is delivered to listeners and never echoed.

## Delivery

An encoded message of 132 bytes or less uses one complete FieldLink frame. Larger messages use an in-memory transfer with 132-byte fragments. The maximum encoded message is 1 MiB.

Every FieldLink frame submission carries a 16-bit transmission ID. MeshCore can suppress duplicate RF copies of one submission, while an intentional FieldLink retry has a new ID and reaches the receiver. Fragment indexes and logical transfer IDs still make reassembly idempotent.

FieldLink ships `selective-window`, retry strategy ID 1:

- The receiver accepts the transfer before fragments are sent.
- The sender transmits windows of eight fragments.
- A one-byte receipt bitmap identifies received fragments.
- Only missing fragments are repaired.
- Each window allows five repair rounds with a 30-second receipt timeout.
- Completion is sent only after length and SHA-256 validation.

FieldLink permits one outbound transfer, four active inbound transfers, and 64 pending sends per node. Inactive inbound state expires after two minutes. High, normal, and bulk queues are checked between every MeshCore frame. Core Stats `queueLen` keeps the radio queue shallow so a high-priority complete message can preempt a bulk transfer.

Transfers are not persisted. Restart, disconnect, shutdown, abort, or exhausted retries fail the transfer and require the caller to send it again. FieldLink does not add compression, persistent resume, replay protection, or signatures.

MeshCore remains responsible for channel encryption and integrity, RF routing, repeater forwarding, radio-packet duplicate suppression, its transmit queue, and the shared Companion inbox. FieldLink adds only message framing, destination filtering, transfer reassembly, selective repair, application priority, and delivery evidence that MeshCore does not provide.

## Commands

List current USB serial radio candidates:

```bash
npm run fieldlink -- radios list
```

On macOS, discovery reads current `/dev/cu.*` entries and keeps USB serial and USB modem callout paths. It hides Bluetooth, debug-console, audio, and other unrelated serial endpoints. A listed path is still unverified. The adapter confirms MeshCore Companion identity and capabilities during preflight before any test traffic is sent.

List the message registry and its runnable payload presets:

```bash
npm run fieldlink -- messages list
npm run fieldlink -- messages list --json
```

Run one deployed adapter process:

```bash
npm run fieldlink -- adapter \
  --radio /dev/cu.usbmodem-A \
  --channel 1 \
  --output results/adapter-A \
  --allow-inbox-drain
```

The adapter creates `events.jsonl` in the output directory before opening the radio and records every consumed Companion inbox item there. It reserves stdout for typed NDJSON and sends diagnostics to stderr. Its `ready` event includes safe radio identity, selected channel metadata, Node ID, supported messages, retry strategies, and delivery limits. `Uint8Array` values cross NDJSON as base64.

Run a two-radio Test echo:

```bash
npm run fieldlink -- test \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --message test \
  --payload-size 64 \
  --retry-strategy selective-window \
  --timeout-ms 1800000 \
  --allow-inbox-drain
```

`--payload-size` is the Test message payload, excluding its five-byte message-local header. It defaults to 64 bytes. The overall test timeout defaults to 30 minutes.

Before starting the adapters, the controller asks MeshCore for every channel slot available on both radios. It chooses the lowest configured slot whose name and key fingerprint match exactly. This inspection does not write radio configuration or transmit RF. If no slot matches, the test stops with an error. `--channel <index>` skips automatic selection and forces one slot for diagnostics.

The controller then starts one adapter per radio, verifies distinct identities and matching LoRa and selected-channel settings, and sends one deterministic Test request from A to B. B's registered handler echoes it. The test passes only when A receives the matching response with identical bytes and, for a fragmented response, B receives the final transfer completion.

## Terminal console

Run the standard-library Python console from the repository root:

```bash
npm run fieldlink:tui
```

Use the arrow keys and Enter to select a registered message, source radio, destination radio, payload, and retry strategy. The console reads both lists from the real FieldLink CLI. Radio entries are USB serial candidates and remain marked unverified until MeshCore preflight succeeds. FieldLink finds the shared MeshCore channel automatically and shows the chosen slot in the live log. Every registered message supplies its own runnable hardware exercise.

Test offers these presets:

- 64 payload bytes in one frame
- 127 payload bytes, the largest Test payload that fits in one frame
- 4096 payload bytes across 32 fragments
- a custom size up to the message limit

The test starts after retry-strategy selection. During the run, the console shows the CLI's RF and inbox-drain warning, then follows `events.jsonl` for frames, fragmentation, receipts, retransmissions, delivery on both radios, SNR, errors, and cleanup. The header, events, and statistics form one scrolling transcript. It follows new events until you press the up arrow, then holds that position while more events arrive. Press the down arrow to return to the bottom. Press `q` to stop cooperatively.

The console writes the normal `manifest.json`, `events.jsonl`, and `summary.json` under `results/`. It does not implement radio or delivery behavior itself. It launches `fieldlink test` and renders its evidence.

## Inbox and evidence safety

MeshCore exposes one shared Companion inbox containing channel data, channel text, and contact messages. FieldLink must drain the complete inbox while it runs. `--allow-inbox-drain` is an explicit acknowledgement of that behavior.

Before either radio opens, `fieldlink test` creates:

- `manifest.json` with requested test inputs
- `events.jsonl` for streamed inbox, frame, message, fragment, receipt, retry, SNR, interruption, error, and cleanup evidence
- `summary.json` with an initial `running` state that is replaced by the final or partial result

The default directory is `results/<timestamp>-test/`. Existing evidence is never overwritten. Full public keys and channel keys are never written or exposed by the process adapter.

Use dedicated test radios. Automated validation never transmits RF. A hardware run requires explicit authorization and confirmed `/dev/cu.*` paths.

## Development

```bash
npm ci
npm run check
git diff --check
```

Start at [`docs/README.md`](docs/README.md) for the dictionary and design decisions.
