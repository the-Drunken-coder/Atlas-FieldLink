# Problem template

Each active entry in `docs/problems/` is a short-lived note for agent-to-agent handoff. Most should live for minutes or days, not months. Update a note when its evidence changes. Delete it when the issue is fixed, abandoned, or invalidated. Git history preserves closed problems.

Use this format:

1. **Time and date:** [UTC timestamp or timestamp with time zone]
2. **Name:** [One-line problem]
3. **Issue:** [Observable failure]
4. **Severity:** [S1 through S5 label from the scale below]
5. **Location:** [Component and specific file or folder]
6. **Expected:** [What should happen]
7. **Actual:** [What happens instead]
8. **Reproduction:** [Numbered steps, a single command, or a test name]
9. **Notes:** [Optional investigation hints, short error excerpts, or links. Omit when empty.]

## What belongs here

- A current problem hit while building, testing, debugging, or running hardware.
- Evidence another session needs to reproduce or continue the investigation.

## What does not belong here

- Recurring agent confusion. After it happens more than once, capture the durable constraint in `AGENTS.md`.
- Architectural decisions. Put those in `docs/design-decisions/`.
- Expected system behavior. Put that in `README.md` or a focused document under `docs/`.

## Severity levels

- **S1, blocker:** Data exposure, security issue, hardware safety issue, or a complete block on the current task.
- **S2, major:** A core path is broken with no reasonable workaround.
- **S3, moderate:** An edge case is broken or only a painful workaround exists.
- **S4, minor:** An annoyance, documentation drift, or flaky test that does not block the task.
- **S5, note:** Context worth handing to the next session with no direct impact on the current task.

## File naming

Keep this file as the style guide. Add one file per active problem named `YYYY-MM-DD-short-slug.md`.
