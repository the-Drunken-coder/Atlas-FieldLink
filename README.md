# Atlas FieldLink

This repository currently contains one thing: a Node.js CLI that validates binary traffic across two real MeshCore Companion USB radios.

It does not integrate with Atlas yet. It does not flash firmware, configure radio parameters, change channels, implement routing, or send text messages.

## Requirements

- Node.js 24
- Two radios already flashed with MeshCore Companion USB firmware
- The same non-empty channel configured in the same slot on both radios
- Both radios connected to the laptop over USB

Use dedicated test radios. MeshCore exposes a shared Companion inbox, so FieldLink must consume both inboxes while it looks for channel datagrams. The required `--allow-inbox-drain` flag acknowledges this behavior. Every consumed channel, contact, and text message is preserved in the run's `events.jsonl` artifact.

FieldLink checks that the ports report different full public keys, records short SHA-256 fingerprints and firmware/radio details, and refuses to start if the LoRa settings or selected channel names and keys differ. It never writes radio configuration.

## Install

```bash
npm install
```

## Find serial ports

```bash
npm run fieldlink -- radios list
```

The command lists every serial port reported by the host. Use the `/dev/cu.*` paths for the two Companion radios.

## Ping

```bash
npm run fieldlink -- ping \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --channel 1 \
  --count 10 \
  --allow-inbox-drain
```

Ping sends a 16-byte binary request from A to B. The laptop receives it through B, verifies every byte, and sends a binary response through B. The round trip completes only when the laptop receives and verifies that response through A.

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

Benchmark runs two independent phases, first A to B and then B to A. `--count` applies to each phase. Because both serial connections are observed by the same Node.js process, each phase reports host-observed one-way latency without claiming radio clock synchronization.

`--payload-size` is the total MeshCore channel-data payload, including FieldLink's 12-byte test header. MeshCore's channel-data upper limit is 163 bytes; FieldLink imposes the 12-byte lower limit needed for its header. The CLI sends one datagram at a time so harness queueing does not inflate the result.

Both commands accept `--timeout-ms` for the full send-and-delivery deadline and `--output` for a specific artifact directory. Counts are capped at 10,000 per phase so an accidental command cannot create an unbounded hardware run.

## Results

Each run creates an artifact directory under `results/` before either radio opens:

- `manifest.json` records the requested operation.
- `events.jsonl` streams samples, anomalies, inbox messages, errors, and interruption events as they occur.
- `summary.json` records the final result, safe radio identities, cleanup failures, and interrupted state.

This layout leaves useful evidence if a run is interrupted. Latency distributions include minimum, mean, p50, p95, p99, and maximum.

Ping RTT starts immediately before FieldLink submits the request to radio A and ends when the verified response reaches FieldLink through radio A. Its deadline includes the outbound send. FieldLink will not begin the next sample while a timed-out send still owns a radio command queue.

Application goodput counts only verified bytes after the 12-byte FieldLink header. Mesh datagram bitrate counts the complete verified channel datagrams. Neither is a claim about raw LoRa bitrate or MeshCore airtime efficiency.

The command exits with status 1 if any requested delivery fails or if FieldLink observes a duplicate, malformed datagram, unexpected run ID/kind/sequence, or payload mismatch. `SIGINT` and `SIGTERM` stop the run cooperatively, close both radios, write a partial summary, and exit with status 130.

## Protocol boundary

The CLI uses `@liamcottle/meshcore.js` for USB framing, Companion Protocol commands, and inbound message parsing. Test traffic uses MeshCore channel data datagrams with the developer data type `0xFFFF` and flood delivery. MeshCore remains responsible for radio transport and routing.

The 12-byte FieldLink test header contains a magic value, version, operation kind, random run ID, and sequence number. The remaining bytes follow a deterministic pattern so the receiver can detect truncation or corruption.

## Development

```bash
npm run check
```
