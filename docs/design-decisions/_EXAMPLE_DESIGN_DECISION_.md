# Design decision template

Each entry in `docs/design-decisions/` records a durable architectural or implementation choice for Atlas FieldLink.

Create an entry only when the decision meets all three tests:

- Changing it later would be costly.
- The chosen approach would surprise a future contributor without context.
- Real alternatives existed and the choice involved a trade-off.

Use this format:

1. **Time and date:** [UTC timestamp or timestamp with time zone]
2. **Name:** [One-line decision]
3. **Context:** [Problem or constraint that required a decision]
4. **Decision:** [What was chosen and why]
5. **Alternatives considered:** [Other options and why they were rejected]
6. **Consequences:** [Non-obvious trade-offs, follow-up work, or constraints]
7. **Location:** [Relevant files or documents]
8. **Notes:** [Optional links, related decisions, or open questions. Omit when empty.]

## What belongs here

- Hardware, protocol, artifact, or package-boundary choices that are expensive to reverse.
- Deliberate deviations from an approach a reasonable contributor would otherwise choose.
- Constraints not visible in the code that must survive future changes.

## What does not belong here

- Transient bugs or blockers. Put those in `docs/problems/`.
- Operational instructions or expected behavior. Put those in `README.md` or a focused document under `docs/`.
- Code-specific reasoning. Put that beside the code when a comment is necessary.

## File naming

Keep this file as the style guide. Add one file per decision named `YYYY-MM-DD-short-slug.md`.
