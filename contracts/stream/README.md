# contracts/stream/

Internal, versioned Kafka record contracts. Software owns with AI reason-code
review. Frozen Days 3-9 (Roadmap.md Section 5).

| Contract | Topic | Status |
|---|---|---|
| `feature-vector-v1.json` | `netsec.conn.feature-vector.v1` | **Frozen.** Written by the online job, read by the archive job and by Python training. |
| `dlq-v1.json` | `netsec.conn.dlq.v1` | **Frozen.** Carries both PARSE-stage and MAP-stage rejections, distinguished by the `stage` field. |
| `network-event-v1.json` | `netsec.network-event.v1` | Not frozen. No producer exists; the online job has no normalized-event sink. |
| `prediction-v1.json` | `netsec.prediction.v1` | Not frozen. Arrives with inference on Roadmap Day 9. |
| `invalid-event-v1.json` | `netsec.invalid-event.v1` | Not frozen. Deferred — `dlq-v1` carries both stages today, and no consumer needs them on separate topics yet. |

These files are immutable. A change to a frozen contract creates a new version
(`-v2`); it never edits the committed file. `StreamContractDriftTest` in
`modules/adapter-kafka` asserts that the serializers and these descriptors
describe the same messages.
