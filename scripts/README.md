# scripts/

Safe, reviewed Bash entry points, grouped by purpose (see subdirectory README files).
No script here duplicates logic that belongs in the Java modules or the Python
training project — they only orchestrate.

The most important script, added at Roadmap.md Day 17-20, is
`scripts/verify-e2e.sh`: a repeatable command that asserts
`Kafka event -> Flink -> FeatureVector -> ONNX Prediction -> Kafka output ->
ClickHouse` using IDs, schema hash, and model version captured at every stage
(Roadmap.md Section 9, Day 20 Demo Script). It lives directly under `scripts/`,
not in a subdirectory, since it spans every stage.

Not yet committed — Step 1 scope is directory skeleton only.
