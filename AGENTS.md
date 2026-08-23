# Agent guidance

Keep this file limited to durable, repository-wide constraints that are easy to miss from code or documentation. When the project surprises you, tell the developer and add a concise note here only if the lesson is likely to recur. Put subsystem behavior in its nearest README or design decision, code-specific reasoning beside the code, and temporary blockers in `docs/problems/`.

## Working principles

- FieldLink is greenfield and has no users or production data yet. Prefer the simplest correct long-term design over compatibility shims, duplicated paths, or preserving an awkward implementation.
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
- `@liamcottle/meshcore.js` owns USB framing, Companion Protocol commands, and inbound parsing. FieldLink owns the validation protocol, run coordination, anomaly detection, and evidence artifacts.
- FieldLink sends test traffic as MeshCore channel data with developer data type `0xFFFF`. MeshCore remains responsible for radio transport and routing.
- Keep CLI parsing in `src/args.ts`, protocol encoding and validation in `src/protocol.ts`, radio integration in `src/radio.ts`, run coordination in `src/runner.ts`, and artifact/report behavior in `src/report.ts`. Move a responsibility only when the new boundary is clearer than this one.
- Keep `types/meshcore-js.d.ts` aligned with the dependency behavior FieldLink actually uses. Do not widen it into a speculative declaration of the whole package.

## Hardware safety

- FieldLink validates two Companion USB radios. It must not flash firmware, write radio configuration, change channels, or expose full public keys or channel keys.
- Use dedicated test radios with the same non-empty channel configured in the same slot. Preserve the preflight checks for distinct radio identities, matching LoRa settings, and matching selected channel names and keys.
- MeshCore exposes a shared Companion inbox. Ping and benchmark runs consume channel, contact, and text messages from both radios. Keep the explicit `--allow-inbox-drain` acknowledgement and preserve every consumed message in `events.jsonl`.
- Hardware commands send real radio traffic and drain inboxes. Do not run them without explicit user direction and confirmed `/dev/cu.*` device paths.
- Preserve interruption-safe evidence. Create the output directory and manifest before opening either radio, stream events during the run, write a partial summary on failure or interruption, and close both radios cooperatively.
- Keep payload bounds, count limits, one-datagram-at-a-time flow control, anomaly failures, and send-queue ownership rules unless the underlying protocol contract changes.

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

Automated tests do not replace a hardware run, but a hardware run is not required for documentation-only or isolated pure-logic changes. For documentation-only changes, check affected links and paths, run `npm run format:check`, and run `git diff --check`.
