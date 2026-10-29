# Project context and continuation

Reconciled on 2026-10-29 against main `49be26234d7829a9199e49810e4236b71a98007c`.

FieldLink delivers registered messages over MeshCore channel data. It owns the application envelope, transfer coordination, priorities and delivery evidence. MeshCore owns the radio routing, channel encryption/integrity, duplicate suppression, transmit queue and shared Companion inbox.

## Implemented and planned work

The [root status table](../README.md#project-status) is the current implementation boundary. Registered Test and Resource messages, fragmentation/reassembly, selective repair, the adapter process, two-radio test controller, terminal console and test-only Resource gateway exist.

The gateway exercises the older Atlas Modernization SDK. It is not a deployed asset/gateway system and does not establish interoperability with the separate [atlas-field-systems/atlas-core](https://github.com/atlas-field-systems/atlas-core). Inspect and agree the current application contract before implementing that bridge.

Gateway OS/Asset OS deployment, self-registration, Task push/lifecycle, passive FieldLink Picture collection, Observation and Object-content messages remain planned or undesigned. Transfer recovery across process restarts and authenticated sender identity remain deferred.

## Message size and success semantics

A complete frame or fragment can carry **132 encoded-message bytes**. That count excludes FieldLink and radio overhead. The 1 MiB encoded-message limit is a memory/protocol bound, not a practical throughput or latency claim.

A frame delivery, a completed transfer, an application response and successful asset execution are separate events. Resource responses carry the matching request ID and application status. A radio acknowledgment alone does not establish that the requested operation succeeded.

A Node ID is an address derived from a radio key, not authentication of the supplied FieldLink source. Any channel member can spoof a FieldLink address. The existing Resource test gateway's binding and operation limits are test safeguards; they do not create per-asset authorization for an operational deployment.

## Source and evidence map

| Location                                                                           | Use                                                              |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Root README](../README.md)                                                        | Installation, radio preflight, commands and status               |
| [Resource contract](messages/resource.md)                                          | Allowed operations, Atlas SDK test gateway and exclusions        |
| [Dictionary](dictionary.md)                                                        | Layer and delivery terminology                                   |
| [Architecture decision](design-decisions/2026-08-24-message-centered-fieldlink.md) | Why messages and registry own their behavior                     |
| [Message source](../src/messages/)                                                 | Codecs, validation, registry and hardware exercises              |
| [Frame and node source](../src/)                                                   | Framing, repair, scheduling, adapter and evidence implementation |
| [Tests](../tests/)                                                                 | Automated contract, framing, transfer and adapter behavior       |
| [Terminal console](../tools/fieldlink_tui.py)                                      | Thin UI over the CLI and streamed evidence                       |

No additional external project artifact was recovered for this context update. Generated benchmark/run evidence stays in the existing gitignored output locations. Do not commit private channel material, credentials, full public keys, consumed unrelated inbox messages or raw generated runs. Keep reusable inputs, source, tests and concise human-readable findings in version control.

## Resume a concrete integration

1. Choose the application operation and its current owning contract before expanding the message registry.
2. Decide what transport delivery, application acceptance and operation completion mean for that use case.
3. Reuse the existing registered-message path and two-radio exercise instead of adding routing or encryption beside MeshCore.
4. Keep dedicated-radio preflight, explicit inbox-drain consent, interruption-safe evidence and cooperative cleanup.
5. Run hardware exercises manually with explicit authorization and confirmed device paths. Automated checks must not open radios or transmit.

This documentation update changes no wire format, gateway behavior or hardware configuration and supplies no new radio test result.
