# Atlas FieldLink

FieldLink is a single-radio software adapter plus a local two-radio test controller. One adapter process owns one MeshCore Companion USB radio. The deployed shape is one Raspberry Pi, one FieldLink adapter process, and one radio.

The current `ping` and `bench` commands start two copies of that adapter on one computer. Each copy gets one serial port. The controller drives both adapters through newline-delimited JSON over standard input and output, but it never opens either radio itself.

```text
Test controller
  |-- Adapter A process -- USB radio A ~~ RF ~~ USB radio B -- Adapter B process
```

This structure exercises the same radio-owning process intended for a Raspberry Pi deployment. It does not integrate with Atlas yet. It does not flash firmware, configure radio parameters, change channels, implement routing, or send text messages.

## Requirements

- Node.js 24
- Two radios already flashed with MeshCore Companion USB firmware
- The same non-empty channel configured in the same slot on both radios
- Both radios connected to the test computer over USB

Use dedicated test radios. MeshCore exposes a shared Companion inbox, so FieldLink must consume both inboxes while it looks for channel datagrams. The required `--allow-inbox-drain` flag acknowledges this behavior. Every consumed channel, contact, and text message is preserved in the run's `events.jsonl` artifact.

Each adapter opens its own radio, reads its identity and selected channel, and returns that preflight information to the controller. The controller checks that the radios have different full public keys and matching LoRa and channel settings. It records short SHA-256 fingerprints and never writes radio configuration.

## Install

```bash
npm install
```

## Find serial ports

```bash
npm run fieldlink -- radios list
```

The command lists every serial port reported by the host. Use the `/dev/cu.*` paths for the two Companion radios.

## Adapter process

```bash
npm run fieldlink -- adapter \
  --radio /dev/cu.usbmodem-A \
  --channel 1 \
  --allow-inbox-drain
```

`adapter` is the single-radio process. It reserves standard input and output for its newline-delimited JSON control protocol. The test controller starts this command twice automatically, so normal bench use does not require starting it by hand.

The adapter is the deployment unit under test, but its Atlas-facing interface does not exist yet. A future Raspberry Pi deployment will run one adapter process with one attached radio and connect the Atlas-side code to the same adapter interface.

## Ping

```bash
npm run fieldlink -- ping \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --channel 1 \
  --count 10 \
  --allow-inbox-drain
```

Ping sends a 16-byte binary request through adapter A. Adapter B reports the received bytes to the controller, which verifies them and asks adapter B to send the response. The round trip completes when adapter A reports the verified response.

## Benchmark

```bash
npm run fieldlink -- bench \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --channel 1 \
  --count 100 \
  --payload-size 64 \
  --allow-inbox-drain
```

Benchmark runs two independent phases, first A to B and then B to A. `--count` applies to each phase. The controller timestamps both adapter processes on the same host. Each phase therefore reports controller-observed one-way latency, including local process communication, without claiming synchronized radio clocks.

`--payload-size` is the total MeshCore channel-data payload, including FieldLink's 12-byte test header. MeshCore's channel-data upper limit is 163 bytes; FieldLink imposes the 12-byte lower limit needed for its header. The CLI sends one datagram at a time so harness queueing does not inflate the result.

Both commands accept `--timeout-ms` for the full send-and-delivery deadline and `--output` for a specific artifact directory. Counts are capped at 10,000 per phase so an accidental command cannot create an unbounded hardware run.

## Results

Each run creates an artifact directory under `results/` before either radio opens:

- `manifest.json` records the requested operation.
- `events.jsonl` streams adapter process IDs, samples, anomalies, inbox messages, errors, and interruption events as they occur.
- `summary.json` records the final result, adapter process IDs, safe radio identities, cleanup failures, and interrupted state.

This layout leaves useful evidence if a run is interrupted. Latency distributions include minimum, mean, p50, p95, p99, and maximum.

Ping RTT starts immediately before FieldLink submits the request to radio A and ends when the verified response reaches FieldLink through radio A. Its deadline includes the outbound send. FieldLink will not begin the next sample while a timed-out send still owns a radio command queue.

Application goodput counts only verified bytes after the 12-byte FieldLink header. Mesh datagram bitrate counts the complete verified channel datagrams. Neither is a claim about raw LoRa bitrate or MeshCore airtime efficiency.

The command exits with status 1 if any requested delivery fails or if FieldLink observes a duplicate, malformed datagram, unexpected run ID/kind/sequence, or payload mismatch. `SIGINT` and `SIGTERM` stop the run cooperatively, close both radios, write a partial summary, and exit with status 130.

## Protocol boundary

Each adapter process uses `@liamcottle/meshcore.js` for USB framing, Companion Protocol commands, and inbound message parsing. Test traffic uses MeshCore channel data datagrams with the developer data type `0xFFFF` and flood delivery. MeshCore remains responsible for radio transport and routing.

The 12-byte FieldLink test header contains a magic value, version, operation kind, random run ID, and sequence number. The remaining bytes follow a deterministic pattern so the receiver can detect truncation or corruption.

## Documentation

Start at [`docs/README.md`](docs/README.md) for focused project documentation, durable design decisions, and active problem notes. Repository-wide agent guidance lives in [`AGENTS.md`](AGENTS.md).

## Development

```bash
npm run check
```
