# contracts/source/

Owns the per-log-type Zeek source contracts: required field/types, `DateTime`
epoch-seconds convention, ID/sensor policy, additive-field policy, and invalid
reason codes for a record as it arrives on the external Kafka topic, before any
parsing or mapping.

- `zeek-conn-source-v1.json` — the `conn` topic. Frozen Day 1 (Roadmap.md Section 5).
- `zeek-dns-source-v1.json` — the `dns` topic.
