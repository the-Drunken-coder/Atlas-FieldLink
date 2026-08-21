# Atlas FieldLink

Atlas FieldLink is a compact communications bridge between Atlas and MeshCore.

It carries small, authenticated operational messages—such as asset status, commands, acknowledgments, and results—over constrained LoRa mesh networks. MeshCore provides the radio transport and routing, while Atlas Modernization remains responsible for application contracts, task lifecycle, and asset execution.

## Goals

- Connect remote Atlas assets through MeshCore companion radios.
- Support reliable, duplicate-safe command delivery.
- Prioritize compact messages suitable for low-bandwidth LoRa links.
- Preserve operation through temporary disconnections.

## Scope

FieldLink is not a replacement for Atlas Mesh and does not implement its own routing protocol. It is a focused integration layer intended to make practical Atlas-over-LoRa deployments achievable.
