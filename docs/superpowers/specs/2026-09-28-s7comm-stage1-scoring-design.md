# S7comm Stage 1 Scoring — Design

**Status:** agreed in conversation 2026-09-28, section by section; written here for review.
**Branch:** `feat/s7comm-scoring`, cut from `feat/deploy-mvp` at `1cbbc04`.
**Scope in one line:** first make two S7comm inputs match the detector's training data (section
2.1); then the online job scores every S7comm feature vector with the delivered, frozen Stage 1
anomaly detector and publishes one prediction per event to `netsec.s7comm.prediction.v1`; the
archive job writes them to ClickHouse. It mirrors the Modbus scoring unit
(`2026-09-26-modbus-stage1-scoring-design.md`) wherever the two detectors allow.

## 1. The upstream authority

The model team delivered the S7comm two-stage pipeline in `models/S7/` (untracked: `models/`
ignores everything but its README). This design was written against these exact files:

| File (under `models/S7/`) | SHA-256 | Role |
|---|---|---|
| `models/stage1_anomaly/v4_causal_final_r1/artifacts/v4_causal_final_model/s7comm_lstm_autoencoder_debiased.onnx` | `2a2e5fe2…6858` | The detector: an LSTM autoencoder, `input` `[?, 16, 21]` → `reconstruction` `[?, 16, 21]` |
| `…/v4_causal_final_model/preprocessor_contract.json` | `faafdba8…2f6b` | The frozen 16 → 21 transform |
| `…/v4_causal_final_model/causal_online_shadow_policy.json` | `3bc3fa79…c559` | Score semantics, groups, the alpha per group |
| `…/v4_causal_final_model/causal_online_conformal_calibration_scores.npz` | `425b839c…9630` | Normal-validation scores per group, the conformal reference |
| `…/v4_causal_final_model/shadow_deployment_manifest.json` | `5bd2a3a4…aa4a` | The score's zero-weight features (`s7_operation`), ONNX parity |
| `…/v4_causal_final_model/preprocessor.joblib` | `6b91d4b7…4175` | The fitted Python preprocessor: the oracle's reference, never loaded by Java |
| `models/stage1_anomaly/v4_causal_final_r1/FROZEN_MANIFEST.json` | `68d84a5d…ff22` | Upstream's own SHA-256 of every release file |
| `src/s7zeek/inference/causal_shadow.py` | `2cded3d7…67e3` | The last-timestep score and the group-conditional conformal decision |
| `src/s7zeek/modeling/debiased.py` | `faf09cf2…62b1` | `operation_groups`, `conformal_pvalues` |
| `src/s7zeek/modeling/preprocessing.py` | `63d4c7ec…0891` | `StableNumericTransformer`: the continuous transform |
| `src/s7zeek/features/s7_parser.py` | `8ef6bbde…77fe` | The pcap parser that built the training table (section 2.1) |

The five feature-path files the delivery ships (`events.py`, `s7_parser.py`, both builders,
`kafka_source.py`) are **byte-identical** to the copies `tests/fixtures/s7comm/generate_upstream_oracle.py`
pins, so `S7commUpstreamOracleTest`'s parity proof holds for this delivery: the platform's 16-value
S7comm vectors are this detector's raw input, subject to section 2.1.

The release is `FROZEN_INTERNAL_SHADOW_MODEL`: its report states that no independent benign traffic
was available, that the training normal set is one S7 connection (one uid, 238,172 events), and that
the FPR after a state reset is high for about 64 events (section 3.5). This unit deploys it as
delivered and measures it on independent traffic (section 11); it does not retrain or retune it.

## 2. What is in scope, and what is not

In scope: section 2.1's two input fixes; the Stage 1 detector end to end — preprocessing, the
16-event window per connection, ONNX inference, the conformal decision, the prediction topic, the
archive chain and table, the deployment; and the real-traffic check.

Not in scope: the Stage 2 COMMAND/FLOODING router (`attack_mode_router_v1_r2`, its own later unit,
once Stage 1's anomalies are shown trustworthy on real traffic; its preprocessor exists only as a
pickle and must first be exported); Modbus Stage 2; scoring conn or dns; batching inference; a
protocol-neutral scoring framework (section 9, D2).

## 2.1 Prerequisite: the detector's input must match its training data

The training table was not built from Zeek. `two-models-info/S7___/06_build_full_canonical.py`
built it from pcap through upstream's own `s7_parser.parse_s7comm_tcp_payload`, and the platform
reads ICSNPP's `s7comm.log`. The two differ where it reaches the model twice.

**S1 — the operation's spelling.** The preprocessor's `s7_operation` categories are
`FUNCTION_0X00`, `READ_VAR`, `SETUP_COMMUNICATION`, `WRITE_VAR`. Every delivered code path spells a
function code with no `FUNCTION_NAMES` entry `f"FUNCTION_0x{code:02X}"` — lower-case `x` — and so
does `S7commCategories.decodeOperation`. The upper-case spelling came from a training step that was
not delivered; either way it is function code 0x00, and as delivered no live event can ever match
it. Rule: the scorer's one-hot compares the decoded operation **upper-cased** against the
categories. It changes nothing but code 0x00's column (every other category is already upper case),
and the feature vector is untouched.

**S2 — a USERDATA PDU's function code.** `s7_parser` reads a PDU's function code as the parameter's
first byte, for every ROSCTR. A USERDATA (ROSCTR 7) parameter always begins with the parameter head
`00 01 12`, so in training **every user-data PDU had function code 0x00** — `FUNCTION_0X00` is the
training table's user-data category. ICSNPP writes the type/function-group byte instead (0x44
"Request: CPU Functions", 0x84 its response, 0x43, 0x47, …; `tests/fixtures/zeek/`). That reaches
more than the one-hot: `s7_function_changed`, the function run length, change rate, entropy and
transition entropy all read the request function code, so a PLC's user-data traffic that training
saw as one repeating function arrives from Zeek as several. Rule: `S7commBuildFeaturesUseCase`
reads a ROSCTR 7 event's function code as 0x00 before the state advances and the vector is
extracted. With S1, its operation then one-hot encodes as `FUNCTION_0X00`.

S2 changes the vectors on `netsec.s7comm.feature-vector.v1`, not only what is scored, as Modbus F1/F2
did. It changes the input the engine reads, not `S7commConnectionState`'s layout, so the state keeps
its name and its savepoints restore; a connection whose history already holds Zeek's user-data codes
carries them until they age out of its 16/32-event windows. ROSCTR 2 (ACK) is not a fidelity gap:
it has no parameter, so both parsers give it no function code (`__MISSING__`); training simply never
saw one (its `s7_rosctr` categories are 1, 3, 7), and ICSNPP writes it when it occurs.

Both rules are this platform's reading of upstream's parser and training artifacts, like
`response_matched`, F1 and F2 for Modbus; the model team should confirm them. The parity oracle
takes Zeek's codes as written today, so its generator gains S2 on the input it hands upstream's
builder (the raw records stay as Zeek writes them) and the fixture is regenerated (section 10).

## 3. What the detector needs

### 3.1 The window

Per connection — the key `S7commConnectionKey(sensor, uid)` that `s7comm-features` already uses, and
that the release's runtime config requires (`state.key: uid`) — the last 16 **preprocessed** vectors,
oldest first. Sequence length 16, stride 1: every event past the warm-up is scored once, and only
the latest event of its window is judged.

### 3.2 Preprocessing: 16 raw values → 21 (`preprocessor_contract.json`)

- **Continuous, indices 0-11.** A NaN is imputed with the contract's median
  (`imputer_statistics`); none is expected, since every S7 feature is finite. The eight features in
  `bounded_ranges` (the rates and ratios) are clipped to `[0, 1]`. The other four
  (`s7_outstanding_requests`, `s7_outstanding_mean_16`, `s7_same_function_run_length`,
  `s7_same_direction_run_length`) become `(x − robust_center) / robust_scale`, clipped to
  `±transformed_clip` (20). Computed in double, carried as float32, as upstream does.
- **Binary, 12-13.** A NaN becomes 0; otherwise passed through.
- **Categorical, 14-15.** The code is decoded to upstream's string by `S7commCategories`
  (`s7_operation` then upper-cased, S1) and one-hot encoded over the contract's categories in their
  listed order: `s7_rosctr` over `1, 3, 7`; `s7_operation` over the four above. Anything else —
  `__MISSING__`, `__UNSEEN__`, ROSCTR 2, an unlisted operation — is the all-zero one-hot
  (`unknown_policy`).
- **Output order** is the contract's `transformed_feature_order` (21 names), which the loader checks
  against the rules above.

### 3.3 The score (`causal_shadow.py`, `shadow_deployment_manifest.json`)

The ONNX graph reconstructs the whole window; only the last timestep is scored:
`score = Σⱼ wⱼ · (reconstruction[15][j] − x[15][j])² / Σⱼ wⱼ`, with `wⱼ = 0` for the four
`categorical__s7_operation_*` columns (the manifest's `identity_features_excluded_from_event_score`,
resolved to columns by upstream's `transformed_columns_for_raw_features` rule) and 1 elsewhere, so
`Σw = 17`. Float32 arithmetic, as upstream's torch code; the score then widens to double for the
decision.

### 3.4 The decision (`debiased.py`, `causal_online_shadow_policy.json`)

- **Group**, from the vector: not a request (`is_request_direction`, index 12, is 0) →
  `RESPONSE`; a request whose operation code (index 15) is 4 → `READ_REQUEST`, 5 →
  `WRITE_REQUEST`, anything else → `OTHER_REQUEST` (`operation_groups`).
- **p-value** against that group's finite calibration scores, sorted:
  `p = (#{calibration ≥ score} + 1) / (n + 1)` (`conformal_pvalues`: a one-sided rank, larger
  error is stranger).
- **Verdict:** `ANOMALY` iff `p ≤ alpha`, else `NORMAL`.
- **Alphas** from the policy's `alpha_by_group`: `RESPONSE` 0.001, `READ_REQUEST` 0.001,
  `WRITE_REQUEST` 0.015625, `OTHER_REQUEST` 0.01. A group with no calibration scores uses every
  group's scores pooled (35,647) and `fallback_alpha` (0.001) instead — which is `OTHER_REQUEST`'s
  case, so its configured 0.01 never applies (`apply_group_conformal_policy`).
- `WRITE_REQUEST`'s 0.015625 is the policy file's value, not the 0.05 in
  `debiased_causal_final_v4.yaml`: the policy is the runtime artifact, and 1/64 is the smallest
  p-value its 63 calibration scores can give, so a write is `ANOMALY` only when it scores above all
  63 (section 9, D6).

### 3.5 Warm-up and resets

A connection's first 15 events after a reset are `WARMUP`, with no score; the 16th is the first
scored (`minimum_events_before_score: 16`), as the frozen policy requires. The release's own stress
test (`state_reset_stress.csv`) shows ~10% of scored events flagged for about 64 events after a
reset, falling to ~0.07% after; the policy is not changed for it. Instead every prediction carries
`eventsSinceReset`, so a consumer can discount early decisions, and the real-traffic check reports
both sides of 64 (section 11). A reset is: a connection's first event, a TTL expiry, and a restore
without state — exactly when `s7comm-features` starts the connection from empty state (section 5).

## 4. Architecture and data flow

```
netsec.s7comm.raw.v1 → s7comm-parse → s7comm-event-narrow → keyBy(uid) → s7comm-features ──→ netsec.s7comm.feature-vector.v1   (vectors change only by S2)
                                                                               │
                                     side output: (key, vector, freshState, clientIp, serverIp)
                                                                               ↓
                                                                 keyBy(uid) → s7comm-score ──→ netsec.s7comm.prediction.v1
                                                                                                          ↓
                                                                     archive job, tenth chain → ClickHouse s7comm_detector_predictions
```

`s7comm-features` gains a side output carrying each vector with its key, whether the connection
started from empty state on this event (`freshState`: the state read `null`), and the connection's
client and server IPs (a request's source and destination; a response's the other way round).
Its main output, its state and its topic are unchanged.

`s7comm-score` is a new keyed operator on the same key. For each record it: (1) empties the window
if `freshState`; (2) preprocesses the vector — a non-finite result is `UNSCORABLE` and does not
enter the window — and appends it; (3) with fewer than 16 vectors in the window, emits `WARMUP`;
otherwise runs the detector and emits `NORMAL` or `ANOMALY` with the score, the p-value, the group
and the alpha. It never reads or writes feature state.

### 4.1 Components by layer

Names are indicative; the implementation plan may refine them.

- **domain:** `S7commPreprocessing` (the contract's parameters and the 16 → 21 transform, S1
  included); `S7commConformalPolicy` (groups, sorted calibration scores, alphas, fallback, the
  p-value); `S7commScoreWindow` (≤ 16 preprocessed vectors, `eventsSinceReset`, the OR of flags, the
  bundle id); `S7commDetectorBundle`; `S7commDetectorPrediction` (section 6); `S7commScoreGroup`.
  `DetectorVerdict` is reused.
- **ports:** `ReconstructionScorer` — reconstructs one `16 × 21` window; a `Serializable` factory,
  one scorer per subtask, created in `open()`.
- **application:** `ScoreS7commSequenceUseCase` — the window rules, the score, the group and the
  verdict; testable with a stub scorer. `S7commBuildFeaturesUseCase` gains S2.
- **adapter-onnx:** `OnnxReconstructionScorer` — one `OrtSession` per subtask, intra- and inter-op
  threads pinned to 1.
- **adapter-registry-filesystem:** `S7commDetectorBundleLoader` — reads and verifies the bundle
  (section 7), including the `.npz` (a zip of `.npy` little-endian float64 arrays) read as delivered.
- **adapter-flink:** `S7commScoringProcessFunction` (with the disabled mode); the side-output tag and
  `KeyedS7commVector`.
- **adapter-kafka:** the prediction serializer (online job) and deserializer (archive job).
- **adapter-clickhouse:** the prediction row and its mapper; DDL migration `004`.
- **bootstrap-online-job:** wiring, `S7COMM_DETECTOR_BUNDLE`, the bundle pre-check in `main()`.
- **bootstrap-archive-job:** the tenth chain.

## 5. Keying, state and lifetime

The window is keyed state on `s7comm-score`, named `s7comm-score-window` — a state name is
checkpoint identity and is never renamed. It holds at most 16 × 21 float32 values (about 1.3 KB) per
connection, the events-since-reset count (a `long`: S7 run lengths are already pinned past
`Integer.MAX_VALUE`), the flags and the bundle id. It carries the feature state's idle TTL
(`S7COMM_STATE_TTL_MINUTES`, one hour, OnCreateAndWrite, NeverReturnExpired, incremental and
full-snapshot cleanup), so the key set is bounded the same way.

Resets come from `freshState`, never from the scorer's own clock, so the scorer and the feature engine
can never disagree about where a connection's history starts. Both states are written on every event
for the same key with the same TTL, so they expire together; if they ever do not, `freshState` still
empties the window, and an expired window with live feature state simply warms up again.
Out-of-order events (`S7COMM_OUT_OF_ORDER`) are scored normally: the feature path does not reset on
them either. A restored window filled by a different bundle is emptied.

## 6. The prediction contract and table

A new stream contract, `contracts/stream/s7comm-detector-prediction-v1.json`, on topic
`netsec.s7comm.prediction.v1` (one partition, seven days, created from `topics.conf`). One JSON
record per S7comm event:

| Field | Meaning |
|---|---|
| `predictionId` | `Prediction.deriveId(eventId, modelName, modelVersion)`: 64 hex, stable on replay |
| `eventId`, `eventTime`, `sensor`, `connectionUid` | copied from the vector; the join back to it |
| `clientIp`, `serverIp` | the connection's endpoints (kept as raw IPs, the owner's 2026-09-28 decision for Modbus) |
| `modelName`, `modelVersion`, `modelSha`, `schemaId`, `schemaHash` | which model scored which feature schema |
| `verdict` | `WARMUP`, `NORMAL`, `ANOMALY` or `UNSCORABLE` |
| `score` | float; null unless `NORMAL` or `ANOMALY` |
| `pValue` | double; null unless `NORMAL` or `ANOMALY` |
| `scoreGroup` | `RESPONSE`, `READ_REQUEST`, `WRITE_REQUEST` or `OTHER_REQUEST`; always present |
| `alpha` | the alpha that applies to this event's group (the fallback when the group has no calibration); always present |
| `eventsSinceReset` | this connection's events since the scorer last reset, this one included (1-based) |
| `qualityFlags` | the bitwise OR of this vector's flags and those of every vector in the window |
| `inferenceMicros`, `producedAt` | inference time (0 unless scored) and emission time |

ClickHouse gets `s7comm_detector_predictions` in an idempotent migration
`infrastructure/clickhouse/ddl/004_s7comm_detector_predictions.sql`, with the Modbus table's rules:
`ReplacingMergeTree(row_version)`, `ORDER BY (model_name, model_version, event_time, event_id)`,
`PARTITION BY toYYYYMMDD(event_time)`, 180-day TTL; `score Nullable(Float32)`,
`p_value Nullable(Float64)`, `alpha Float64`, `events_since_reset UInt64`, `LowCardinality` verdict
and group. `SchemaDriftTest` and `DdlDirectoryTest` cover them as they cover the others.

## 7. The model bundle and its deployment

```
models/s7comm-stage1-detector/v1/
  model.onnx          ← s7comm_lstm_autoencoder_debiased.onnx, as delivered
  preprocessing.json  ← preprocessor_contract.json, as delivered
  policy.json         ← causal_online_shadow_policy.json, as delivered
  calibration.npz     ← causal_online_conformal_calibration_scores.npz, as delivered
  bundle.json         ← the manifest (new contract: contracts/model/s7comm-detector-bundle-v1.json)
```

`bundle.json` records the name and version, `schemaId: s7comm-feature-v1`, `sequenceLength` 16,
`featureCount` 21, `inputName` `input`, `outputName` `reconstruction`, `zeroWeightFeatures`
`["s7_operation"]`, and the four files' SHA-256. `deploy/models/package-s7comm-detector.sh
<delivery-dir>` builds it, refuses any file whose SHA-256 differs from the delivery's own
`FROZEN_MANIFEST.json`, and never overwrites a bundle. The same bundle, about 170 KB, is committed as
the test fixture `tests/fixtures/models/s7comm-stage1-detector/v1/`, as the Modbus one is.

The loader refuses to start the job unless: every file's SHA-256 matches `bundle.json`; the
preprocessing contract's 16 `raw_feature_order` names, in order, equal the registered
`s7comm-feature-v1` schema's, and its `transformed_feature_order` is the 21 names section 3.2 derives;
the policy's sequence length is 16, its scored timestep `LAST_ONLY` and its alphas name only the four
groups; the `.npz` holds exactly those four arrays; the ONNX graph's input and output have the names
and shapes of section 1.

Deployment, as for Modbus: packaged on the workstation that holds the delivery, copied over SSH to
the server's `models/` (Git-ignored), mounted read-only into the TaskManager and the job supervisor
(already done). The pin is `S7COMM_DETECTOR_BUNDLE=s7comm-stage1-detector/v1` in `.env.template`,
with a compose fallback for an older `deploy/.env`. `check_detector_bundle` checks both pins before
anything stops or starts (`up` and `restart`); `main()` loads and verifies the pinned bundle before
submitting. An **empty pin runs `s7comm-score` disabled** — no model, no output, each window
cleared — while its uids stay in the graph, so switching scoring off or on never orphans savepoint
state. `submit-jobs.sh` names a failing S7 bundle as it names the Modbus one; `selftest`, with S7
scoring on, waits for the S7 pair's two `WARMUP` predictions and deletes them afterwards.

## 8. Error handling

| Situation | Behaviour |
|---|---|
| Bundle missing, corrupt or mismatched | The job refuses to start (section 7); preflight names it before anything stops |
| ONNX Runtime throws during inference | Rethrown: the job restarts under exponential delay |
| A vector preprocesses to NaN or infinity | `UNSCORABLE`, no score; the vector does not enter the window. Not expected — every continuous transform clips and every S7 feature is finite — a guard, not a path |
| Idle connection | The window expires with the TTL; the connection warms up again |
| Restore | The window resumes mid-sequence; a window from another bundle is emptied |
| Scoring disabled | `s7comm-score` runs disabled; everything else is unchanged |

## 9. Rulings this design makes

- **D1 — Stage 1 only** (section 2).
- **D2 — Mirror the Modbus unit** with S7-specific classes, not a protocol-neutral framework first
  and not a separate scoring job. It leaves the live Modbus path untouched (a renamed uid or state
  there loses savepoint state); two detectors that share plumbing but differ in preprocessing, score
  and decision are too few to design a framework from; and a separate job would need a new
  feature-vector contract to carry the uid and IPs. Cost: parallel code, recorded as a known limit —
  a third detector is the point to generalise.
- **D3 — The frozen warm-up (16) stands; `eventsSinceReset` exposes the post-reset FPR** rather
  than hiding decisions until event 64, which would depart from the frozen policy and never flag an
  attack in a connection's first 64 events.
- **D4 — Resets come from `freshState`** (section 5).
- **D5 — The group reads the operation code, not the record's function code.** They are equal
  whenever the record carries a code, which ICSNPP always does when it names a function; a record
  with only a name that resolves to `READ_VAR`/`WRITE_VAR` would be grouped READ/WRITE here and
  OTHER upstream.
- **D6 — Alphas come from `causal_online_shadow_policy.json`'s `alpha_by_group`,** never from code or
  from the training YAML.
- **D7 — The `.npz` is read as delivered,** not converted, so every bundle file is byte-identical to
  the model team's and checked against their manifest.
- **D8 — Every event gets a prediction** (`WARMUP`, `UNSCORABLE` included), so coverage gaps are
  visible in ClickHouse.
- **D9 — `qualityFlags` is the OR over this vector and the window.**
- **D10 — The inputs match training before anything is scored** (section 2.1).

## 10. How correctness is proven

1. **The scoring oracle.** `tests/fixtures/s7comm/generate_detector_oracle.py` runs seeded synthetic
   ICSNPP-shaped S7 streams — connections over 64 events, reads, writes, setup, user-data, ACKs, a
   TTL-style reset, and at least one `ANOMALY` in every group that can produce one — through
   upstream's own builder (S2 applied to its input), the delivered `preprocessor.joblib` (S1 applied),
   Python ONNX Runtime and upstream's `operation_groups` / `conformal_pvalues`, writing raw records,
   vectors, preprocessed values, scores, p-values, groups and verdicts to
   `tests/fixtures/s7comm/detector_oracle_v1.jsonl`. The Java test drives the raw records through the
   real parser, mapper, feature use case, preprocessing and Java ONNX scorer: vectors and verdicts
   must match exactly, p-values exactly, preprocessed values to 1e-6, scores to 1e-6 relative. The
   generator reports the smallest gap between an oracle score and its nearest calibration score, and
   the fixture keeps it above the score tolerance, so exact verdicts are sound. A planted bug must
   break it.
2. **The feature parity oracle** (`upstream_oracle_v1.jsonl`) is regenerated with S2 in its
   generator; `S7commUpstreamOracleTest` stays bit-identical, and its planted bug still breaks it.
3. **Preprocessing:** a hand-built contract with hand-worked outputs; the real contract against the
   delivered `preprocessor.joblib` on fixed rows (values generated in Python).
4. **The conformal policy:** the p-value formula on a hand-built calibration; each group; the empty
   group's fallback; the WRITE boundary (above all 63 → `ANOMALY`; equal to the maximum → `NORMAL`).
5. **The use case, with a stub scorer:** `WARMUP` on events 1-15 and the first score on 16;
   `freshState` resets; `eventsSinceReset`; flags OR'd; a window from another bundle is emptied;
   non-finite input gives `UNSCORABLE`.
6. **The operator, in a Flink harness:** TTL expiry; a snapshot and restore mid-window gives the same
   predictions as an uninterrupted run; scoring switched off and on again through savepoints.
7. **The loader:** each failure in section 7 fails with a message naming it; the `.npz` reader on the
   real file gives upstream's array lengths and first values.
8. **Contracts and topology:** the serializer emits exactly the contract's fields; the drift tests
   cover the new table; the topology tests pin the new uids (`s7comm-score`,
   `s7comm-prediction-sink`, its committer, and the archive chain's three) and that an empty pin keeps
   them.
9. **Deploy scripts:** the second pin in preflight and `restart`, the fourteenth topic, the selftest's
   S7 predictions (`deploy/tests/`).

## 11. The real-traffic check (before merge)

On server3, read-only, with another project's captures:
`/root/1405-06-15/Models/data/raw/benign/pcap/s7comm/` (`s7.pcap`, `s701.pcap`, `s702.pcap`,
`S7COMM.pcap`) and `…/attack/pcap/s7comm/aS700-aS703.pcap` with `…/attack/labels/s7comm/*.csv`. Each
capture goes through the deployed Zeek image (`zeek -r`) and then through the production parser,
mapper, feature use case, preprocessing and ONNX scorer in a scratch harness (not committed). The
same records also go through upstream's delivered Python runtime without S1/S2 (the oracle
generator's machinery), which is the "as delivered" comparison.

Reported: % `NORMAL` per capture, per group and per ROSCTR/operation, separately for
`eventsSinceReset` ≤ 64 and > 64, with and without S1/S2; the share of attack-capture events flagged
(joined to the label CSVs where their rows map to S7 records — the attack captures' names suggest
TCP-level attacks, which may produce few S7 records at all); and inference time per window.

**Stop rule:** if benign events past the 64th of their connection score below 99% `NORMAL` (the
model reports 99.88% on its own normal test), the causes are investigated and brought to the owner
before merging. The ≤ 64 figure is reported with section 3.5's explanation, not held to that bar.

## 12. Rollout

On `feat/s7comm-scoring`, commit by commit, each verified as CLAUDE.md prescribes. Then the
real-traffic check (section 11). Then: package the bundle on the workstation; copy it to the
server's `models/`; push; on the server `git pull`, `deploy.sh build --jars-only`, `deploy.sh
restart` (which creates the new topic and applies DDL `004` through `up`). Both jobs resume from
their savepoints: every existing state restores unchanged, `s7comm-score`'s state is new, and the
archive job's new chain starts from the new topic's earliest offset. Then `selftest`, and a live S7
check: a connection of at least 20 events gives 15 `WARMUP` rows and scored rows whose scores match
the offline Python scorer for the same vectors. Then CLAUDE.md: implementation state, the
verification table, and the S7comm limits (S1/S2, the post-reset FPR, the real-traffic result).

## 13. Records

- S1 and S2 (section 2.1) await the model team's confirmation, as F1/F2 and `response_matched` do.
- The delivered model files never enter Git except as the committed test fixture (section 7), the
  owner's 2026-09-28 decision for Modbus applied here; the deployed bundle under `models/` stays out.
- The release is a shadow model with no independent benign validation; section 11 is the first.
- Stage 2 (`attack_mode_router_v1_r2`) is the next S7 unit and gets its own design.
