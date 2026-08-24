# Agent guidance

Keep this file limited to durable, repository-wide constraints that are easy to miss from code or documentation. When the project surprises you, tell the developer and add a concise note here only if the lesson is likely to recur. Put subsystem behavior in its nearest README or design decision, code-specific reasoning beside the code, and temporary blockers in `docs/problems/`.

## Working principles

- FieldLink is greenfield and has no users or production data yet. Prefer the simplest correct long-term design over compatibility shims, duplicated paths, or preserving an awkward implementation.
- FieldLink does not use versions. Never add FieldLink package or release numbers, numbered stage labels, or revision fields to FieldLink frames, messages, schemas, NDJSON, evidence, or subsystems. Change the whole greenfield system in place and delete superseded shapes instead of adding compatibility or migration paths. External dependency, runtime, and radio firmware pins may remain only where the build or hardware compatibility requires them; they are not FieldLink versions.
- Measure twice, cut once. Understand the problem, constraints, and current behavior before building; cleverness is often what gets written when the problem is not yet understood.
- Fight for the obvious solution. Push back when a simpler path meets the real need, even when the requested or existing approach is more elaborate.
- During planning, do not be afraid to propose a seemingly insane or "boil the ocean" solution when it is genuinely the cleanest answer. Explain the scope and trade-offs, and get the user's approval before expanding implementation beyond the request.
- Apply YAGNI aggressively. The biggest simplicity win is refusing to solve a problem that we do not have; do not add extension points, configuration, abstractions, or data models for hypothetical requirements.
- Good code is the simplest thing that delivers the required functionality and performance. Trade away neither, and bolt on nothing unnecessary.
- Prefer a direct expression, guard clause, or call when it is clearer than a helper. Split code only when it improves readability, error handling, or reuse that already exists.
- Keep changes scoped. Greenfield status is not permission for unrelated refactors.
- For UI work, ask targeted questions when selection, focus, hover, keyboard, or pointer behavior is ambiguous; confirm the user-visible precedence instead of guessing.
- Treat ignored build outputs and local configuration as disposable. Update source, examples, templates, or generators rather than `node_modules/` or `dist/`. The ignored `results/` directories are user-owned hardware evidence, so never edit them as source or delete them without explicit direction.

## Repository boundaries

- This is a Node.js 24 TypeScript ESM package using npm and the root lockfile. Run package commands from the repository root.
- `@liamcottle/meshcore.js` owns USB framing, Companion Protocol commands, inbound parsing, channel encryption and integrity, RF routing, repeater forwarding, radio-packet duplicate suppression, its transmit queue, and the shared inbox. FieldLink owns registered messages, its envelope and transfer protocol, application priority, run coordination, and evidence artifacts.
- FieldLink sends registered traffic as MeshCore channel data with developer data type `0xFFFF` and flood delivery. Do not add a second radio routing or encryption layer.
- Reuse MeshCore's messaging behavior before adding a FieldLink equivalent. Check the exact firmware and client support for routing, encryption, packet deduplication, acknowledgements, fragmentation, retries, queues, backpressure, inboxes, and delivery metadata. FieldLink should add only missing Atlas semantics; document why any overlap is necessary.
- Keep all behavior unique to one message in its file under `src/messages/`. This includes its hardware exercise input and completion matcher. Shared registry contracts belong in `src/messages/definition.ts`, the explicit registry in `src/messages/index.ts`, FieldLink framing in `src/frame.ts`, transfer coordination and scheduling in `src/node.ts`, retry state machines under `src/retry-strategies/`, MeshCore integration in `src/radio.ts`, process behavior in `src/adapter-process.ts`, CLI parsing in `src/args.ts`, and artifact behavior in `src/evidence.ts`.
- Adding a supported message requires one message file and one explicit registry entry. Every registered message must provide a valid hardware exercise so the CLI and terminal console can list and send it. Do not add runtime discovery, code generation, message-specific transport code, or compatibility paths for the removed ping and benchmark protocol.
- Keep `tools/fieldlink_tui.py` a thin standard-library interface over the CLI and streamed evidence. Do not open serial ports, encode messages, implement retries, or duplicate test coordination in Python.
- On macOS, radio discovery must return current `/dev/cu.*` USB serial or USB modem candidates, not `/dev/tty.*`, Bluetooth, debug-console, audio, or other generic serial endpoints. Treat every listed path as unverified until the existing MeshCore preflight succeeds.
- Keep `types/meshcore-js.d.ts` aligned with the dependency behavior FieldLink actually uses. Do not widen it into a speculative declaration of the whole package.

## Hardware safety

- FieldLink validates two Companion USB radios. It must not flash firmware, write radio configuration, change channels, or expose full public keys or channel keys.
- Use dedicated test radios with the same non-empty channel configured in the same slot. Two-radio tests default to asking MeshCore for every available slot without transmitting, then select the lowest slot with an exact name and key-fingerprint match. Do not assume a fixed channel count. Keep the numeric override for diagnostics. Preserve the preflight checks for distinct Node IDs, matching LoRa settings, and matching selected channel names and key fingerprints.
- MeshCore exposes a shared Companion inbox. Adapter and Test runs consume channel data, channel text, and contact messages. Keep the explicit `--allow-inbox-drain` acknowledgement and preserve every consumed item in `events.jsonl`.
- Preserve the per-submission transmission ID in every FieldLink frame. MeshCore duplicate suppression must collapse RF copies of one submission without swallowing intentional FieldLink retries that reuse the same logical transfer and fragment indexes.
- Hardware commands send real radio traffic and drain inboxes. Do not run them without explicit user direction and confirmed `/dev/cu.*` device paths.
- Preserve interruption-safe evidence. Create `manifest.json`, `events.jsonl`, and the initial `summary.json` before opening either radio, stream events during the run, write a partial summary on failure or interruption, absorb signals during finalization, and close both radios cooperatively.
- Preserve the 1 MiB encoded-message bound, one outbound and four active inbound transfer limits, 64 pending-send bound, two-minute inactive cleanup, digest validation, and one-frame-at-a-time Core Stats pacing unless the protocol contract changes. Priority must be reconsidered between every MeshCore frame.

## Documentation

- `docs/README.md` is the documentation entry point.
- Record a design decision only when it is hard to reverse, surprising without context, and the result of a real trade-off. Use `docs/design-decisions/_EXAMPLE_DESIGN_DECISION_.md`.
- Use `docs/problems/` only for active, short-lived blockers that another session may need. Update notes when evidence changes and delete them when resolved or invalidated.
- Keep operational instructions and expected behavior in `README.md` or a focused subsystem document. Keep this file for recurring agent constraints.

## Workflow and validation

Start by confirming the checkout:

```sh
git status --short --branch
git worktree list
```

Codex worktrees can be detached or checked out on a different branch than their directory suggests. Verify branch ownership before editing or committing.

Use the narrowest relevant checks. Run the full automated ladder before handing off code changes:

```sh
npm ci
npm run check
git diff --check
```

Automated tests do not replace a hardware run, but a hardware run is not required for documentation-only or isolated pure-logic changes. For documentation-only changes, check affected links and paths, run `npx prettier --check AGENTS.md README.md docs`, and run `git diff --check`.
