# Modbus Stage 1 Scoring — Design

**Status:** agreed in conversation 2026-09-26, section by section; written here for review.
**Branch:** `feat/modbus-scoring`, cut from `feat/deploy-mvp` at `b8d3979`.
**Scope in one line:** first make two Modbus inputs match the detector's training data (section
2.1); then the online job scores every Modbus feature vector with the delivered, frozen Stage 1
anomaly detector and publishes one prediction per event to `netsec.modbus.prediction.v1`; the
archive job writes them to ClickHouse.

## 1. The upstream authority

The model team delivered the Modbus two-stage pipeline in `models/modbus/` (untracked: `models/`
ignores everything but its README). This design was written against these exact files:

| File (under `models/modbus/stage1_anomaly_detector/`) | SHA-256 | Role |
|---|---|---|
| `models/dual_head_model_fp32.onnx` | `b5f28fec…bf4f` | The detector: one graph, two heads |
| `models/dual_head_model_fp32.onnx.manifest.json` | `a455de5c…e31f` | Upstream's own record of the ONNX hash, shapes and Keras parity |
| `contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json` | `9ecff68f…896a` | The frozen 42 → 42 transform |
| `contracts/modbus_dual_head_temporal_dense_detector_v1/THRESHOLD_CONTRACT_V1.json` | `9da034b4…6c69` | The two scores and two thresholds |
| `contracts/modbus_dual_head_temporal_dense_detector_v1/DETECTOR_CONTRACT_V1.json` | `e4b3326c…5dd9` | Input, sequence rules, decision rule, accepted metrics |
| `contracts/modbus_feature_contract_v1/FEATURE_CONTRACT_V1.json` | `620c9d00…5411` | The 42 features, in order |
| `src/07b_materialize_feature_engine_v1.py` | `e2abcfa3…c225` | The authoritative feature engine |
| `training/17_run_dual_head_temporal_dense.py` | `61bf46ab…6b9a` | `score_dense` / `score_temporal`: the scoring arithmetic |

The feature contract, the preprocessing contract and `07b` are **byte-identical** to the copies in
`two-models-info/` and `tests/fixtures/contracts/` that `modbus-feature-v1` already mirrors and
`ModbusEngineParityTest` already proves against. The platform's 42-value Modbus vectors are this
detector's input, before preprocessing, with nothing to adapt — given the `response_matched` fix
(`b8d3979`), without which every response would reach the detector with a value it almost never saw
in training (1 on 99.99% of training responses).

## 2. What is in scope, and what is not

In scope: Modbus Stage 1 only — NORMAL / ANOMALY per event, with both scores; its prediction stream
contract, Kafka topic, ClickHouse table and archive chain; the model bundle, its loader and its
deployment to the server.

Out of scope, each a later unit: S7 scoring (its detector is `SHADOW_ONLY_NOT_PRODUCTION_VALIDATED`
and needs a conformal decision port); Modbus Stage 2 (the attack-family classifier is provisional and
needs a 252-feature sequence summarizer); conn scoring (the parked `feat/conn-scoring-path`);
alerting, dashboards and any UI; retraining, recalibration or any change to the delivered model,
thresholds, preprocessing or sequence semantics — upstream's own rule makes any of those a v2
detector.

## 2.1 Prerequisite: the detector's input must match its training data

Added 2026-09-26, found while planning. The detector's training table was built by upstream's own
capture adapter, not by Zeek (`0761`'s `FIELD_ALIASES`: `request_values`, `matched`,
`pcap_adapter_matched_rtt_ms`, `tcp_reassembled`), and two of its inputs differ systematically from
what icsnpp-modbus v1.0.0 writes. Unfixed, most windows would score as unlike training whatever the
scoring code does — and code-parity tests cannot see it: they compare code with code, not Zeek's data
with upstream's.

| Features | Training (`TRAIN_TRANSFORM_VERIFICATION_V1.csv`) | Zeek v1.0.0 through the pipeline (ICSNPP sample, 44 vectors) | Cause |
|---|---|---|---|
| Value summaries (13–22) | `response_values_present` on 50.00% of rows: every response | 0 of 44 | Zeek writes a comma-separated `values` string (`"170,171"`, `"T,F,F"`); the mapper reads only `request_values`/`response_values` arrays |
| `address_present` (9), `quantity_present` (11) | 100% of rows | address on 5 of 22 responses, 14 of 22 requests | Zeek leaves them off most responses; upstream's adapter evidently carried the request's |

The rules (decided 2026-09-26; to be confirmed with the model team, like `response_matched`):

- **F1 — values.** When a record carries no `request_values`/`response_values` array, its `values`
  string is parsed — comma-separated decimals as numbers, `T`/`F` (coils, discrete inputs) as 1/0 —
  into `request_values` on a request and `response_values` on a response. A `values` that is not
  wholly numeric (`\x00\x00`, `see modbus_mask_write_register.log`, an exception name, empty) is
  absent, never a rejection: upstream's own `parse_numeric_vector` cannot parse such strings either,
  so its training data never held them. An array, when present, wins.
- **F2 — address and quantity.** A response that carries no address (or quantity) takes its pending
  request's, through the causal pairing `response_matched` already uses. A response whose request is
  not pending (unanswered, evicted by the 4096 cap, or across a segment start) and a request without
  them stay absent — the latter (diagnostics, for example) is an input the detector never saw.
- **State.** F2 keeps each pending request's address and quantity, so `ModbusEntityState`'s layout
  changes; it is Kryo-serialized, so the state is renamed `modbus-entity-state-v2`, and the deployed
  `modbus-entity-state` (test data only) is left unread in the savepoint: every Modbus stream starts
  fresh once, on this upgrade.

## 3. What the detector needs

From the detector and threshold contracts, and upstream's scoring code:

- **Input:** `sequence_20x42`, float32 `[batch, 20, 42]`: the 20 most recent events of one stream,
  oldest first, each already preprocessed.
- **Stream:** `(client_ip, server_ip, unit_id)` — `ModbusEntityKey`, which adds only the sensor. No sequence crosses a
  segment (a >15 s gap, or time going backwards) or a capture. No padding.
- **Outputs:** `modbus_dense_autoencoder` `[batch, 20, 42]`, the reconstruction of the whole window;
  `modbus_causal_next_event_predictor` `[batch, 19, 42]`, next-event predictions from events 1–19.
- **Scores:** `dense = mean |input − reconstruction|` over all 20 × 42 values;
  `temporal = mean |prediction[18] − input[19]|` over 42 values — event 20 predicted from 1–19.
- **Decision:** `ANOMALY` iff `dense > 0.2483385056257248` **or** `temporal > 0.4121147692203522`.
- **Preprocessing** (`transform_definitions`, parameters fitted on Normal Train only, no clipping):
  `PASSTHROUGH_BINARY` and `PASSTHROUGH_BOUNDED_OR_CONSTANT`: `y = x`; `GLOBAL_STANDARD`:
  `(x − mean) / std`; `GLOBAL_LOG1P_ONLY`: `log1p(x)`; `CONDITIONAL_STANDARD`,
  `CONDITIONAL_LOG1P_ONLY`, `CONDITIONAL_LOG1P_THEN_STANDARD`: the same where the feature's mask is
  1, and exactly `0.0` where it is 0.
- **Accepted:** 99.17% normal specificity, 99.73% attack recall, 0.21 ms per sequence on CPU.

## 4. Architecture and data flow

```
netsec.modbus.raw.v1 → modbus-parse → modbus-event-narrow → keyBy(stream) → modbus-features ──→ netsec.modbus.feature-vector.v1   (unchanged)
                                                                                │
                                                              side output: (stream key, vector)
                                                                                ↓
                                                               keyBy(stream) → modbus-score ──→ netsec.modbus.prediction.v1
                                                                                                          ↓
                                                                     archive job, ninth chain → ClickHouse modbus_detector_predictions
```

`modbus-features` gains exactly one thing: a side output carrying each vector with its stream key,
read from the operator's current key. Its main output, its state, the feature-vector topic and the
parity-tested use case are untouched.

`modbus-score` is a new keyed operator on the same stream key. For each vector it: (1) empties the
window if the vector starts a segment; (2) preprocesses the vector and appends it; (3) with fewer
than 20 events in the window, emits `WARMUP`; otherwise runs the detector on the window and emits
`NORMAL` or `ANOMALY` with both scores. The scorer never reads or writes feature state: it consumes
finished vectors, so upstream's "state is updated only after the current event is scored" rule
holds by construction.

### 4.1 Components by layer

Names are indicative; the implementation plan may refine them.

- **domain:** `ModbusPreprocessing` (the 42 policies, masks, means and standard deviations, and the
  transform); `ModbusScoreWindow` (the last ≤ 20 preprocessed vectors, the events-in-segment count and
  the bundle id that filled it); `ModbusDetectorPrediction` (the record in section 6);
  `DetectorVerdict` (`WARMUP`, `NORMAL`, `ANOMALY`, `UNSCORABLE`); `DetectorTrigger` (`NONE`, `DENSE`,
  `TEMPORAL`, `BOTH`).
- **ports:** `SequenceScorer` — scores one `20 × 42` window, returning `(dense, temporal)`;
  `SequenceScorerFactory`, `Serializable`, one scorer per subtask, created in `open()`.
- **application:** `ScoreModbusSequenceUseCase` — owns the window rules, preprocessing and the
  decision rule; testable with a stub scorer and no ONNX Runtime on the classpath.
- **adapter-onnx:** `OnnxSequenceScorer` — one `OrtSession` per subtask, intra- and inter-op threads
  pinned to 1, scores computed in double and carried as float32.
- **adapter-registry-filesystem:** `SequenceDetectorBundleLoader` — reads and verifies the bundle
  (section 7), yields the preprocessing, thresholds and scorer factory.
- **adapter-flink:** `ModbusScoringProcessFunction`; the side-output tag and a
  `KeyedModbusVector(ModbusEntityKey, FeatureVector)` record.
- **adapter-kafka:** the prediction serializer (online job) and deserializer (archive job).
- **adapter-clickhouse:** the prediction row and its mapper; DDL migration `003`.
- **bootstrap-online-job:** wiring, `MODBUS_DETECTOR_BUNDLE`, the bundle pre-check in `main()`.
- **bootstrap-archive-job:** the ninth chain.

## 5. Keying, state and lifetime

The window is keyed state on `modbus-score`, named `modbus-score-window` — a state name is checkpoint
identity and is never renamed. It holds at most 20 × 42 float32 values (about 3.4 KB) per stream,
plus a count and the bundle id. It carries the same idle TTL as the feature state
(`MODBUS_STATE_TTL_MINUTES`, one hour, OnCreateAndWrite, NeverReturnExpired, incremental and
full-snapshot cleanup), so the key set is bounded the same way; an expired window reads as empty and
the stream warms up again. The outage caveat is the feature state's: after an outage longer than the
TTL a stream re-warms even when its event time has no gap.

A segment start is read from the vector itself: `prev_event_available` (index 23) is 0 exactly on the
first event of a segment — a key's first event, one after a >15 s gap, and one reset for arriving
out of order. The scorer therefore needs no segment logic of its own and can never disagree with the
feature engine about where a segment starts.

## 6. The prediction contract and table

A new stream contract, `contracts/stream/modbus-detector-prediction-v1.json`, on topic
`netsec.modbus.prediction.v1` (one partition, seven days, created by `deploy.sh up` from
`topics.conf`). `prediction-v1` is untouched: contracts are immutable, and its single score,
threshold and decision cannot carry this detector. One JSON record per Modbus event:

| Field | Meaning |
|---|---|
| `predictionId` | `Prediction.deriveId(eventId, modelName, modelVersion)`: 64 hex, stable on replay |
| `eventId`, `eventTime`, `sensor`, `connectionUid` | copied from the vector; the join back to it |
| `clientIp`, `serverIp`, `unitId` | the stream key: which device |
| `modelName`, `modelVersion`, `modelSha`, `schemaId`, `schemaHash` | which model scored which feature schema |
| `verdict` | `WARMUP`, `NORMAL`, `ANOMALY` or `UNSCORABLE` |
| `denseScore`, `temporalScore` | both scores; null unless `NORMAL` or `ANOMALY` |
| `denseThreshold`, `temporalThreshold` | always present, from the bundle |
| `trigger` | `NONE`, `DENSE`, `TEMPORAL` or `BOTH`: which head exceeded its threshold |
| `windowEvents` | the window's size after this event, 0–20: 20 once full; unchanged by an `UNSCORABLE` event, which never enters it |
| `qualityFlags` | the bitwise OR of this vector's own flags and those of every vector in the window |
| `inferenceMicros`, `producedAt` | inference time (0 unless scored) and emission time |

ClickHouse gets a new table, `modbus_detector_predictions`, in an idempotent migration
`infrastructure/clickhouse/ddl/003_modbus_detector_predictions.sql`: `ReplacingMergeTree(row_version)`,
`ORDER BY (model_name, model_version, event_time, event_id)`, `PARTITION BY toYYYYMMDD(event_time)`,
180-day TTL — the existing `predictions` table's rules — with nullable score columns and
`LowCardinality` verdict and trigger. A new table rather than columns on `predictions`, because that
table's single, non-null score, threshold and decision belong to conn's contract. `SchemaDriftTest`
and `DdlDirectoryTest` cover the new row type and DDL as they cover the others.

## 7. The model bundle and its deployment

The layout `models/README.md` already defines, `models/<name>/<version>/`:

```
models/modbus-stage1-detector/v1/
  model.onnx          ← dual_head_model_fp32.onnx, as delivered
  preprocessing.json  ← PREPROCESSING_CONTRACT_V1.json, as delivered
  thresholds.json     ← THRESHOLD_CONTRACT_V1.json, as delivered
  bundle.json         ← the manifest (new contract: contracts/model/sequence-detector-bundle-v1.json)
```

`bundle.json` records the name and version, `schemaId: modbus-feature-v1`, each file's SHA-256, the
sequence length (20), feature count (42), and the ONNX input and output names.
`deploy/models/package-modbus-detector.sh <delivery-dir>` builds it, and first checks the ONNX file
against the delivery's own manifest hash (`b5f28fec…`), so a corrupted copy is never packaged.

The loader refuses to start the job — a deployment error, like an unknown feature schema — unless:
every file's SHA-256 matches `bundle.json`; the preprocessing contract's 42 feature names, in order,
equal the registered `modbus-feature-v1` schema's; the ONNX graph's input and outputs have the names
and shapes of section 3; and both thresholds are read from `thresholds.json`, never from code.

Deployment: the bundle is packaged on the workstation that holds the delivery and copied over SSH to
the server's `models/` folder, which Git ignores, so `git pull` never touches it. Compose mounts
`models/` read-only into the TaskManager and the job supervisor. The pin is
`MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1` in `.env.template`, with a compose fallback for a
`deploy/.env` written before it existed (the `MODBUS_STATE_TTL_MINUTES` pattern). `main()` checks the
pinned bundle exists before submitting, and `deploy.sh up`'s preflight does too, so a missing bundle
is a clear error at `up`, never a crash loop. An empty pin leaves `modbus-score` and its sink off the
graph: a site without the model runs features only, and turning scoring on later is safe, since the
new operator simply starts with empty state.

Upgrades: package `v2` beside `v1`, change the pin, restart; rolling back is changing the pin back.
The window records the bundle id that filled it, and a restored window from any other bundle is
emptied, so vectors preprocessed for one model are never fed to another.

## 8. Error handling

| Situation | Behaviour |
|---|---|
| Bundle missing, corrupt or mismatched | The job refuses to start (section 7). The supervisor's cause-aware warning names it as not a state-restore failure |
| ONNX Runtime throws during inference | Rethrown, never swallowed: the job restarts under exponential delay and, if it keeps failing, is failed for the supervisor |
| A vector preprocesses to NaN or infinity | `UNSCORABLE`, no scores, a metric counter; the vector does not enter the window; the job never fails on data. Not expected — every `log1p` input is a non-negative Modbus quantity — a guard, not a path |
| Idle stream | The window expires with the TTL; the stream re-warms |
| Restore | The window resumes mid-sequence; a window from another bundle is emptied |
| Scoring disabled | `modbus-score` is not built; everything else is unchanged |

## 9. Rulings this design makes

- **S1 — Stage 1 only.** S7 and both Stage 2 models are later units (section 2).
- **S2 — A separate scoring operator in the online job** (approach A), not scoring inside
  `ModbusFeatureProcessFunction` and not a separate scoring job: it follows `FINAL_ARCHITECTURE.md`'s
  "keyed features → ONNX score → Kafka", keeps ML runtime out of the parity-tested feature code, and
  needs no change to the `feature-vector-v1` contract. A separate job remains the path if scoring
  ever needs to deploy independently of features.
- **S3 — The stream key reaches the scorer through a side output and `keyBy`,** not
  `reinterpretAsKeyedStream` (an experimental API): one small shuffle of 42 floats per event.
- **S4 — Segment starts come from the vector's `prev_event_available`** (section 5).
- **S5 — The window holds preprocessed vectors and the id of the bundle that filled it.**
- **S6 — A new stream contract and a new table** (section 6), not `prediction-v1`.
- **S7 — Every event gets a prediction:** `WARMUP` before the window is full and `UNSCORABLE` for
  non-finite input, so coverage gaps are visible in ClickHouse, not only by an anti-join.
- **S8 — Thresholds are strict (`>`), global, and come from the bundle,** never from code.
- **S9 — `qualityFlags` is the OR over this vector and the window,** so an `ANOMALY` scored on a window containing,
  for example, `MODBUS_WINDOW_SATURATED` vectors says so.
- **S10 — An empty bundle pin disables scoring;** a missing pinned bundle fails at `up`.
- **S11 — The inputs match training before anything is scored** (section 2.1): F1 and F2 land first,
  and the one-time reset of the Modbus state is accepted because the deployed state is test data.

## 10. How correctness is proven

1. **An upstream oracle**, as `S7commUpstreamOracleTest` does for S7:
   `tests/fixtures/modbus/generate_detector_oracle.py` runs a deterministic synthetic polling stream
   (several streams, some over 20 events in one segment, at least one segment break; raw records in
   Zeek v1.0.0's own shape — `values` strings, responses without address — so F1 and F2 are
   exercised end to end) through
   upstream's own `07b` engine, applies the preprocessing contract and runs the delivered ONNX model
   in Python ONNX Runtime, writing raw events, 42-value vectors, preprocessed values and both scores
   to `tests/fixtures/modbus/detector_oracle_v1.jsonl`. The Java test drives the same raw events
   through the real parser, mapper, use case, preprocessing and Java ONNX scorer: vectors and
   preprocessed values must match exactly; scores to 1e-5 (Python and Java run different ONNX
   Runtime versions). A planted bug must break it.
2. **The use case, with a stub scorer:** `WARMUP` on events 1–19 and the first score on event 20;
   a segment start re-warms; `trigger` per head; strict thresholds; flags OR'd over the window; a
   window from another bundle is emptied; non-finite input gives `UNSCORABLE`.
3. **The operator, in a Flink harness:** TTL expiry; a snapshot and restore mid-window gives the
   same predictions as an uninterrupted run; a response lands in its request's stream.
4. **The bundle loader:** each failure — a SHA mismatch per file, a feature-order mismatch, a wrong
   ONNX name or shape, a missing file — fails with a message naming it.
5. **Contracts and topology:** the serializer emits exactly the contract's fields; the drift tests
   cover the new table; the topology tests pin the new uids (`modbus-score`, `modbus-prediction-sink`
   and the archive chain's three) and that an empty pin removes the online two.
6. **Live on server3, after deploying:** a 25-event polling stream on one stream gives 19 `WARMUP`
   rows and 6 scored rows in `modbus_detector_predictions`, with scores matching the offline Python
   scorer for the same vectors. Container end-to-end tests are written and compiled, but not run on
   the development machine.

## 11. Rollout

On `feat/modbus-scoring`, commit by commit, each verified as CLAUDE.md prescribes (module by module,
`-am`, the actual test counts read). Then: package the bundle on the workstation; copy it to the
server's `models/`; push; on the server `git pull`, `deploy.sh build --jars-only`, `deploy.sh restart`
(which creates the new topic and applies DDL `003` through `up`); then the live check of section 10.
The online job's conn, dns and s7comm state restores unchanged and the new operator's state is new;
the Modbus feature state starts fresh once (section 2.1's rename). The archive job gains a chain whose
source starts from the new topic's earliest offset. The live check also replays the ICSNPP v1.0.0
sample and confirms value and address presence now match section 2.1's rules.

## 12. Records

- This unit depends on `b8d3979` (`response_matched` derived when a record carries no `matched`).
- F1 and F2 (section 2.1) are this platform's reading of upstream's adapter from its code and
  training statistics, as `response_matched` was; the model team should confirm them.
- The model files never enter Git; their identity is `bundle.json`'s hashes and section 1's table.
  *(Note 2026-09-27: not held. The plan committed a byte-identical copy of the delivered model,
  preprocessing and thresholds as the test fixture `tests/fixtures/models/modbus-stage1-detector/v1/`,
  which the loader, scorer and oracle tests read, and `feat/modbus-scoring` has been pushed with it.
  Keeping it, or rewriting the branch's history and making those tests read a local `models/` copy
  instead, is the user's decision. The deployed bundle under `models/` stays out of Git.)*
- The first 19 events of every stream segment are never scored; that is the detector's design (no
  padding), made visible as `WARMUP`, not a platform choice.
- S7 scoring and Modbus Stage 2 are the next units; each gets its own design.
