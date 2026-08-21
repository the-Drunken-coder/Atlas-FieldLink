# Atlas FieldLink

This repository currently contains one thing: a Node.js CLI that validates binary traffic across two real MeshCore Companion USB radios.

It does not integrate with Atlas yet. It does not flash firmware, configure radio parameters, change channels, implement routing, or send text messages.

## Requirements

- Node.js 24
- Two radios already flashed with MeshCore Companion USB firmware
- The same non-empty channel configured in the same slot on both radios
- Both radios connected to the laptop over USB

FieldLink reads each selected channel before a run and refuses to start if the channel names or keys differ. It never writes radio configuration.

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
  --count 10
```

Ping sends a 16-byte binary request from A to B. The laptop receives it through B, verifies every byte, and sends a binary response through B. The round trip completes only when the laptop receives and verifies that response through A.

## Benchmark

```bash
npm run fieldlink -- bench \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --channel 1 \
  --count 100 \
  --payload-size 64
```

`--payload-size` is the total MeshCore channel-data payload, including FieldLink's 12-byte test header. MeshCore allows 12 to 163 bytes here. The CLI sends one request at a time so queueing from the harness does not distort the result.

Both commands accept `--timeout-ms` for the per-round-trip timeout and `--output` for a specific JSON result path.

## Results

Each normal run writes a JSON report under `results/` unless `--output` is set. Reports include every sample, byte counts, packet loss, forward and return SNR, and RTT minimum, mean, p50, p95, and maximum.

RTT starts immediately before FieldLink submits the request to radio A and ends when the verified response reaches FieldLink through radio A. FieldLink does not report one-way latency because the two radio paths do not provide a shared measured clock.

"Verified goodput" counts only request and response bytes from completed round trips. It is not a claim about raw LoRa bitrate or MeshCore airtime efficiency.

The command exits with status 1 if any requested round trip fails, times out, or contains corrupt data. It still writes the report.

## Protocol boundary

The CLI uses `@liamcottle/meshcore.js` for USB framing, Companion Protocol commands, and inbound message parsing. Test traffic uses MeshCore channel data datagrams with the developer data type `0xFFFF` and flood delivery. MeshCore remains responsible for radio transport and routing.

The 12-byte FieldLink test header contains a magic value, version, request or response kind, random run ID, and sequence number. The remaining bytes follow a deterministic pattern so the receiver can detect truncation or corruption.

## Development

```bash
npm run check
```
