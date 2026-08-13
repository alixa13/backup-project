# contracts/

The immutable, versioned, language-neutral shared source of truth for this
platform. Both Java and Python read these contracts; neither owns them alone.

Java/Flink creates canonical `FeatureVector` records from these contracts; Python
consumes those vectors for training and embeds fitted scaling inside ONNX. This
directory is what prevents duplicate preprocessing logic between the two languages.

## Rule

A contract is **frozen** once released: a changed value rule, order, unit,
default, or type creates a new version (e.g. `conn-feature-v1` → `conn-feature-v2`)
rather than editing the existing file in place. See `FINAL_ARCHITECTURE.md`,
"Feature versions can be changed in place," and Roadmap.md Section 5 for the
freeze-day schedule per contract.

## Layout

| Directory | Contract | Owner (freeze day, per Roadmap.md Section 5) |
|---|---|---|
| `source/` | `zeek-conn-source-v1.json` — required fields/types, invalid reason codes | Both; software implements parser (Day 1) |
| `domain/` | `network-event-v1.json` — normalized domain event shape | Both; software owns domain (Day 2) |
| `features/` | `conn-feature-schema-v1.json` — 20 ordered float32 features, content hash | AI owns semantics; software owns execution (Day 2, window semantics Day 5) |
| `stream/` | `network-event-v1.json`, `feature-vector-v1.json`, `prediction-v1.json`, `invalid-event-v1.json`, `dlq-v1.json` — internal Kafka record shapes | Software with AI reason-code review (Days 3-9) |
| `dataset/` | `dataset-manifest-v1.json` — snapshot query/dedup rule, schema hash, time range, label revision | AI owns; software reviews CH query (Day 6) |
| `model/` | `model-bundle-manifest-v1.json` — version/SHA, schema hash/count, ONNX I/O, class index, threshold | Both (Days 7-8) |

## Status

Step 1: directory skeleton only. Contract files are committed starting at
Roadmap.md Day 1-2, per `FINAL_ARCHITECTURE.md` Step 14, "Next implementation step."
