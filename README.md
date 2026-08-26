# Atlas FieldLink

FieldLink delivers registered binary messages through one MeshCore Companion USB radio. The public module is `FieldLinkNode`. A thin NDJSON adapter exposes the same interface across a process boundary, and the local `fieldlink test` command starts two adapters to prove a real RF echo.

```text
Atlas-side caller
  -> FieldLinkNode or adapter process
  -> MeshCore Companion USB radio
  -> MeshCore channel and RF mesh
```

FieldLink does not flash firmware, write radio configuration, change channels, or replace MeshCore routing. It uses MeshCore channel data type `0xFFFF`, flood delivery, and the 163-byte channel-datagram limit.

## Project status

Only the first row describes code that exists in this repository today.

| Status                   | Scope                                                                                                                                                                                                                                                                                                               |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Implemented now          | Registered Test and Resource messages, FieldLink framing, fragmentation, reassembly, priority, selective repair, delivery evidence, the adapter process, the two-radio test controller, the terminal console, and a test-only Atlas SDK Resource gateway. Resource supports Entity and Object CRUD plus Task reads. |
| Planned, not implemented | A deployed Gateway OS and Asset OS integration, Asset self-registration, Task push and lifecycle messages, passive state collection through FieldLink Picture, Observation and Object-content messages, congestion estimates, and a one-gateway, three-simulated-asset acceptance exercise over real radios.        |
| Still to design          | The exact Runtime, Task, Observation, and Object-content message shapes; FieldLink Picture storage and queries; congestion calculations; Object-content transfer limits; and the package boundaries between Atlas Protocol, Gateway OS, Asset OS, and FieldLink.                                                    |
| Deferred                 | Radio sender authentication, a deny or quarantine system, congestion-based traffic control, and transfer recovery across process restarts. FieldLink Track fusion is not planned. Future Track fusion belongs to Atlas Core.                                                                                        |

The Resource gateway is an integration test capability, not a deployed Core
bridge. The terminal console can execute real Atlas API calls over two radios,
but this repository does not yet run an operational asset or gateway.

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

Message-specific behavior lives in one file under `src/messages/`. A message definition owns its stable `uint16` ID, name, default priority, runtime validation, binary codec, examples, hardware exercise, and optional inbound handler. FieldLink currently registers Test as message ID 1 and Resource as message ID 2.

The explicit registry is `src/messages/index.ts`. Adding a message requires one new message file and one registry entry. Startup rejects duplicate IDs or names. Generic contract tests validate every registered example and codec round trip.

The hardware exercise constructs a representative message and recognizes successful end-to-end delivery. This keeps message-specific test input and completion rules in the message file while the CLI continues to own radios, transport, evidence, and timing.

Test has request and response variants. Both carry a `uint32` correlation ID and arbitrary bytes. A received request is echoed to its source with identical correlation and payload. A response is delivered to listeners and never echoed.

Resource carries a typed UTF-8 JSON envelope for Entity and Object CRUD plus Task reads. Create and patch bodies use Atlas JSON; Object bodies are metadata only. Every request has a `request_id`, and every response returns the same ID, a numeric status, and an optional JSON body. Resource carries no HTTP routes or credentials. The two-radio test can explicitly enable an Atlas SDK gateway on adapter B; normal nodes do not execute Resource requests. See the [Resource message contract](docs/messages/resource.md) for its exact operations, gateway setup, security boundary, and exclusions.

## Delivery

An encoded message of 132 bytes or less uses one complete FieldLink frame. Larger messages use an in-memory transfer with 132-byte fragments. The maximum encoded message is 1 MiB.

Every FieldLink frame submission carries a 16-bit transmission ID. MeshCore can suppress duplicate RF copies of one submission, while an intentional FieldLink retry has a new ID and reaches the receiver. Fragment indexes and logical transfer IDs still make reassembly idempotent.

FieldLink ships `selective-window`, retry strategy ID 1:

- The receiver accepts the transfer before fragments are sent.
- The sender transmits windows of eight fragments.
- A one-byte receipt bitmap identifies received fragments.
- Only missing fragments are repaired.
- Each window allows five repair rounds. A receipt request waits 30 seconds
  and is retried once before any fragment repair.
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

`--payload-size` is the selected message's exercise payload, excluding its message-local envelope. Test defaults to 64 bytes. Resource defaults to 32 JSON string bytes and exercises delivery only; it does not call an Atlas API. The overall test timeout defaults to 30 minutes.

To exercise the actual Atlas API, build the SDK in an Atlas Modernization
checkout, copy `.env.example` to `.env`, and provide the checkout path, Atlas
base URL, and API key. Then pass one Resource request JSON file:

```bash
npm run fieldlink -- test \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --message resource \
  --resource-request request.json \
  --retry-strategy selective-window \
  --allow-inbox-drain
```

The CLI validates the JSON before opening a radio. After preflight it binds the
destination gateway to radio A's Node ID, loads `.env` only in adapter B,
handshakes through the Atlas SDK, sends the request across FieldLink, and waits
for the matching response. The API key never enters the Resource message or
evidence. See the [Resource message contract](docs/messages/resource.md#atlas-sdk-test-gateway)
for setup and trust limitations.

Before starting the adapters, the controller asks MeshCore for every channel slot available on both radios. It chooses the lowest configured slot whose name and key fingerprint match exactly. This inspection does not write radio configuration or transmit RF. If no slot matches, the test stops with an error. `--channel <index>` skips automatic selection and forces one slot for diagnostics.

The controller then starts one adapter per radio, verifies distinct identities and matching LoRa and selected-channel settings, and sends one deterministic Test request from A to B. B's registered handler echoes it. The test passes only when A receives the matching response with identical bytes, any fragmented response finishes, cleanup succeeds, and neither adapter reports a listener or protocol error.

## Terminal console

Run the standard-library Python console from the repository root:

```bash
npm run fieldlink:tui
```

Use the arrow keys and Enter to select a registered message, source radio, destination radio, input, and retry strategy. The console reads both lists from the real FieldLink CLI. Radio entries are USB serial candidates and remain marked unverified until MeshCore preflight succeeds. FieldLink finds the shared MeshCore channel automatically and shows the chosen slot in the live log.

Test offers these presets:

- 64 payload bytes in one frame
- 127 payload bytes, the largest Test payload that fits in one frame
- 4096 payload bytes across 32 fragments
- a custom size up to the message limit

When you select Resource, the console shows operation-aware fields on the left
and the generated request JSON on the right. Choose `create`, `get`, `list`,
`patch`, or `delete`; select the allowed resource type; fill its ID, page query,
or body; then press `s` to continue. Body JSON uses a multiline editor where
Ctrl-G saves. The console skips synthetic payload selection, enables the Atlas
SDK gateway on B, and displays the returned Resource response in the run
transcript. A manual CLI run can still use `--payload-size` to exercise
Resource transport without calling Atlas.

The test starts after retry-strategy selection. During the run, the console shows the CLI's RF and inbox-drain warning, then follows `events.jsonl` for frames, fragmentation, receipts, retransmissions, delivery on both radios, SNR, errors, and cleanup. The header, events, and statistics form one scrolling transcript. It follows new events until you press the up arrow, then holds that position while more events arrive. Press the down arrow to return to the bottom. Press `q` to stop cooperatively.

When a test starts, the console replaces `tools/results.txt` with a human-readable transcript of that run. The exact Resource request JSON is at the top, events are flushed into the file while the test runs, and the decoded response is at the bottom after the run finishes. The normal timestamped `manifest.json`, `events.jsonl`, and `summary.json` under `results/` remain the complete machine-readable evidence and are not overwritten. The console does not implement radio or delivery behavior itself. It launches `fieldlink test` and renders its evidence.

## Inbox and evidence safety

MeshCore exposes one shared Companion inbox containing channel data, channel text, and contact messages. FieldLink must drain the complete inbox while it runs. `--allow-inbox-drain` is an explicit acknowledgement of that behavior.

Before either radio opens, `fieldlink test` creates:

- `manifest.json` with requested test inputs
- `events.jsonl` for streamed inbox, frame, message, fragment, receipt, retry, SNR, interruption, error, and cleanup evidence
- `summary.json` with an initial `running` state that is replaced by the final or partial result

The final summary records request and response delivery separately. Each side
includes encoded bytes, fragments, retransmissions, receipts, and sender
duration when available, including receipt requests and their retries for
transfers. Verification reports response correlation, the fragment digest when
a transfer was used, and the Atlas status for a Resource response. A successful
run is `clean` when neither direction needed recovery and `recovered` when a
fragment repair or receipt-request retry succeeded. Listener and protocol errors
make the final run fail even if the expected response arrived first.

Each adapter also creates `adapters/a/events.jsonl` or `adapters/b/events.jsonl` before opening its radio. It appends every consumed inbox item to that local file before sending the item to the controller. The root `events.jsonl` remains the combined test transcript.

The default directory is `results/<timestamp>-test/`. Existing evidence is never overwritten. Full public keys and channel keys are never written or exposed by the process adapter.

Use dedicated test radios. Automated validation never transmits RF. A hardware run requires explicit authorization and confirmed `/dev/cu.*` paths.

## Development

```bash
npm ci
npm run check
git diff --check
```

Start at [`docs/README.md`](docs/README.md) for the dictionary and design decisions.
