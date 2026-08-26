# Atlas FieldLink documentation index

This is the entry point for project documentation. Keep the root `README.md` focused on setup, hardware requirements, commands, and expected behavior. Add focused documents here when a subject needs more room.

## Project documents

| Location                                                                                                                 | What it holds                                                  | Use it when                                                   |
| ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------- | ------------------------------------------------------------- |
| [`dictionary.md`](dictionary.md)                                                                                         | Shared terms for FieldLink, Atlas, and MeshCore delivery.      | Naming messages, delivery behavior, and ownership boundaries. |
| [`messages/resource.md`](messages/resource.md)                                                                           | Entity/Object CRUD and Task read message contract.             | Sending or handling broad Atlas resource operations.          |
| [`design-decisions/2026-08-24-message-centered-fieldlink.md`](design-decisions/2026-08-24-message-centered-fieldlink.md) | Message, registry, node, and retry architecture.               | Adding messages or changing transfer behavior.                |
| [`design-decisions/`](design-decisions/)                                                                                 | Durable architectural or implementation choices for FieldLink. | A future contributor may ask what was decided and why.        |
| [`problems/`](problems/)                                                                                                 | Short-lived notes about active blockers.                       | Another session needs current evidence to continue debugging. |

Start from the [design decision template](design-decisions/_EXAMPLE_DESIGN_DECISION_.md) or [problem template](problems/_EXAMPLE_PROBLEM_.md). Do not create an entry until there is a real decision or active problem to record.

When a protocol, hardware workflow, or subsystem needs reference material, give it a focused folder under `docs/` and add it to this index.

## Other root files

- [`AGENTS.md`](../AGENTS.md) holds hard constraints and recurring agent gotchas for the whole repository.
- [`README.md`](../README.md) is the project overview and operator guide.
