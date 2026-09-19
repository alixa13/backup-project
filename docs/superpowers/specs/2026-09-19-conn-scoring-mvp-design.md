# Conn scoring MVP — design

**Status:** approved in conversation, 2026-09-19
**Scope:** score `conn` records end to end with a model trained on the platform's own frozen
`conn-feature-v1` vectors, using the owner's labelled packet captures as the source of training labels.

## 1. Purpose

The platform produces feature vectors and archives them. Nothing scores them. This MVP builds the missing
path — model registry, ONNX inference inside the online job, a prediction contract and topic, predictions
archived to ClickHouse — and proves it with a model trained on real labelled traffic.

It deliberately uses `conn`, the protocol already implemented, so the MVP adds no new log type.

### 1.1 What this MVP claims, and what it does not

**Claims:** a conn record flowing through the online job is scored by a pinned ONNX model; the prediction
reaches `netsec.prediction.v1` and the `predictions` table; the model's quality is measured on a held-out
split the owner's own pipeline defined, and reported per class.

**Does not claim:** attack-type classification (binary only), detection of content-driven attacks that
leave no connection-level trace, or that the resulting model transfers to the owner's OT network. The
training captures are lab traffic. The model is a demonstrator of the path, and a measurement of how much
signal survives in Zeek's `conn.log`.

## 2. Constraints

Inherited, not negotiable in this unit:

- **Zeek logs only.** Captures are replayed through Zeek *offline* to produce logs. The platform ingests
  logs, never packets. No pcap reader enters any module.
- `conn-feature-v1` is frozen at 20 values. This unit does not touch it, its content hash, or
  `ConnFeatureSchemaV1`.
- `contracts/` and `infrastructure/clickhouse/ddl/` are immutable. The existing `predictions` table is used
  exactly as written; new contracts are new files.
- CPU-only. ONNX Runtime with intra-op and inter-op threads pinned to 1 per subtask.
- The model bundle is pinned in job configuration and loaded once in `open()`. No hot reload.
- `domain → ports → application → adapters → bootstrap`. Adapters do not import each other, except the
  recorded `adapter-flink → adapter-kafka` exception.
- `feature_vectors` carries no addresses. The label join must not change that (§5).
- Development machine has 5.7 GiB; container tests run staged, never as one reactor build.

## 3. Data flow

**Training (offline, once per dataset):**

```
capture.pcap --zeek -r--> conn.log (JSON) --shipper--> conn topic
  --online job--> netsec.conn.feature-vector.v1 --archive job--> ClickHouse feature_vectors
labels: flow manifests + conn.log (uid <-> 5-tuple) --> join by (sensor, connection_uid)
  --> training matrix --> sklearn --> model.onnx + bundle.json
```

**Serving (the production path, unchanged except for the scoring stage):**

```
conn topic --> parse/validate --> feature extraction --> SCORE --> netsec.prediction.v1
  --> archive job --> ClickHouse predictions
```

Vectors keep flowing to their own topic and table as today; scoring is an additional branch, not a
replacement. A model failure must never stop feature production.

## 4. Components

### 4.1 `domain`

- `Prediction` — a record carrying `predictionId`, `eventId`, `eventTime`, `modelName`, `modelVersion`,
  `modelSha`, `schemaId`, `schemaHash`, `score`, `decision`, `threshold`, `inferenceMicros`,
  `qualityFlags`, `producedAt`. These are exactly the columns the `predictions` table declares — it has no
  sensor column, and the sensor is recoverable from `eventId` (`sensor:uid`), so the record does not carry
  one. `producedAt` maps to `row_version`, the same way `FeatureVector.producedAt` already does.
- `decision` is `score >= threshold`, stored as 0 or 1. The comparison lives in one place (§4.3) so it
  cannot drift between the operator and the archive.
- `predictionId` is `sha256(eventId + "|" + modelName + "|" + modelVersion)`, lowercase hex, 64
  characters — matching the table's `FixedString(64)` and making replays idempotent under
  `ReplacingMergeTree`.
- `ModelRef` — `name`, `version`, `schemaId`, `schemaHash`, `modelSha`, `threshold`, `classes`. Immutable,
  `Serializable` (it travels in Flink job configuration).

### 4.2 `ports`

- `ModelScorer` — `double score(float[] featureValues)` and `ModelRef ref()`. Deliberately narrow: it
  answers with a probability and nothing else, so the decision rule stays testable without ONNX on the
  classpath.

### 4.3 `application`

- `ScoreFeaturesUseCase` — takes a `ModelScorer` and a `Clock`; maps `FeatureVector → Prediction`. It:
  1. rejects a vector whose `schemaId`/`schemaHash` differ from the scorer's `ModelRef` — a model scoring
     a schema it was not trained on is a deployment error, not a runtime condition;
  2. times the scorer call in microseconds;
  3. applies `threshold` from the `ModelRef`, never from code;
  4. copies the vector's `qualityFlags` onto the prediction, so a prediction made on a degraded vector
     stays visibly degraded downstream.

### 4.4 `adapter-registry-filesystem`

- `FilesystemModelRegistry.load(Path dir)` → `LoadedModel` (`byte[] onnx`, `ModelRef`).
- Reads `bundle.json`, verifies the ONNX file's SHA-256 equals the bundle's `modelSha`, and fails loudly
  otherwise. A truncated or swapped model file must not reach the runtime.
- New frozen contract `contracts/model/model-bundle-v1.json` defines the bundle's shape: `name`,
  `version`, `schemaId`, `schemaHash`, `featureOrder`, `classes`, `threshold`, `modelSha`, `metrics`,
  `trainedAt`, `provenance`, `sampleVectors` (§7.2).

### 4.5 `adapter-onnx`

- `OnnxModelScorer implements ModelScorer`, built from model bytes + `ModelRef`, holding an
  `OrtEnvironment` and `OrtSession` with `intraOpNumThreads = 1`, `interOpNumThreads = 1`.
- At construction it checks the graph's input dimension equals the registered schema's `featureCount()`
  (20) and fails otherwise.
- Input tensor shape `[1, 20]` float32; output is the positive class probability. `AutoCloseable`.

### 4.6 `adapter-flink`

- `ScoreFeatureVectorFunction extends RichFlatMapFunction<FeatureVector, Prediction>` — flatMap, not map,
  because a scoring failure emits nothing rather than a fabricated prediction. It holds the registry path
  and `ModelRef` as serializable configuration, constructs the scorer and use case in `open()` as
  transient fields (the pattern every other operator on this branch already uses), and closes the scorer
  in `close()`.

### 4.7 `adapter-kafka`

- `PredictionSerializer` / `PredictionDeserializer` over `contracts/stream/prediction-v1.json`, a new
  frozen contract mirroring `feature-vector-v1`'s style: the envelope is fixed, values vary.

### 4.8 `adapter-clickhouse`

- `PredictionRow` + `PredictionRowMapper`, column-for-column with the existing table. No DDL change.

### 4.9 `bootstrap`

- Online job: a `conn-scoring` operator after `conn-feature-extraction`, then a `prediction-sink`.
  New uids only; conn's five existing uids stay byte-identical.
- Archive job: a fifth chain through the existing list form — `prediction-source`, `prediction-row`,
  `predictions-clickhouse-sink`.
- `.env.example` gains `MODEL_BUNDLE_DIR` and `PREDICTION_TOPIC=netsec.prediction.v1`.

### 4.10 `training/`

A Python project with two commands, which never reimplement feature logic — they read archived vectors:

1. `build-dataset` — inputs: the directory of replayed Zeek `conn.log` files, the owner's flow manifests,
   and a ClickHouse connection. Output: a parquet of `eventId`, the 20 values, `label`, `split`,
   `capture`. Prints join coverage.
2. `train` — reads the parquet, trains two candidates (a `LogisticRegression` baseline and a
   `HistGradientBoostingClassifier`, both class-weighted), selects between them on the validation split by
   PR-AUC, chooses the decision threshold on that same validation split by maximising positive-class F1,
   exports `model.onnx` and `bundle.json`, and writes a metrics report. The threshold is never re-tuned on
   `final_test`; the converter and scikit-learn versions are pinned and recorded in the bundle.

The replay shipper — a short script that reads Zeek JSON lines and produces them to the `conn` topic —
lives in `training/` as tooling. It is not platform code and carries no feature logic.

## 5. The label join

Each manifest row is `capture`, `flow_id` (`<pcap>::<proto>|<ip:port>|<ip:port>`), `flow_start`, `split`,
`domain`, and for attacks `pair_id`. Each Zeek record has `uid`, `ts`, `id.orig_h/p`, `id.resp_h/p`, `proto`.

**Each capture is replayed as its own run, with `SENSOR_ID` set to `capture:<pcap name>`.** Sensor id is
job-level configuration, so one replay run handles one capture. That makes every `eventId` (`sensor:uid`)
unique across captures and carries capture identity into the vector's `sensor` column — so the join needs
no addresses in `feature_vectors` and the privacy property holds.

Matching rule, in order:

1. same capture;
2. same transport protocol;
3. the manifest's two endpoints, as an unordered pair, equal the Zeek record's `(orig, resp)` pair —
   direction-agnostic, because the manifest's tuples are not consistently client-first;
4. `|zeek.ts − flow_start| ≤ 1s` (configurable).

More than one match: take the nearest in time and increment `ambiguous_matches`. No match: the record is
unlabelled, excluded from training, and counted. **Join coverage and ambiguity are reported as first-class
numbers** — a silent 60% join would flatter every metric computed after it.

The owner's `train` / `validation` / `final_test` splits are reused verbatim. `final_test` is measured once,
after the model is frozen.

## 6. Evaluation

The training data is 96% attack (140,216 attack flows against 5,328 normal); production traffic is the
inverse. Accuracy is therefore meaningless here and is not reported alone. The report carries:

- per-class precision, recall and F1, plus the confusion matrix;
- balanced accuracy and PR-AUC;
- **precision projected to realistic base rates** (attack prevalence 1% and 0.1%), computed from the test
  PR curve — the number that says whether the model would be usable on a real network;
- join coverage, ambiguity count, and the per-class labelled-flow counts;
- any class with fewer than 100 labelled flows is reported but excluded from headline claims.

## 7. Testing

### 7.1 Component tests

Fail-fast paths get tests, because each is a silent-corruption risk: bundle schema hash ≠ registered schema
hash; ONNX file SHA ≠ bundle `modelSha`; graph input width ≠ 20; a vector whose schema id differs from the
model's.

### 7.2 The golden-vector guard

`bundle.json` carries `sampleVectors`: a handful of real feature vectors from the training set with the
probability scikit-learn produced for each. A Java test feeds them through `OnnxModelScorer` and asserts
agreement within 1e-5. This is the guard against the classic failure of this design — a converter version
changing semantics between training and serving, which no Java-side test would otherwise notice.

### 7.3 End to end

One conn record with a bundle present produces a prediction on `netsec.prediction.v1`; the archive chain
lands it in `predictions` with the right `model_name`, `model_version` and `decision`. Both run against
real containers, staged, never as one reactor build.

### 7.4 Python

Dataset-build tests on a small fixture: direction-agnostic matching, the time tolerance, ambiguity
counting, and that an unlabelled Zeek record is excluded rather than defaulted.

## 8. Failure behaviour

A model that cannot be loaded, or whose bundle disagrees with the registered schema, **fails the job at
startup** — a mispaired model and schema is a deployment error, and starting anyway would emit confident
nonsense.

Once running, a scoring failure on a single record emits **no prediction** and increments a Flink counter;
the record's feature vector is untouched and still reaches its own topic, because scoring is a parallel
branch rather than a stage in the vector's path. Failures are deliberately *not* routed to the DLQ: that
contract (`dlq-v1`) describes a raw record that failed to parse, carrying its payload and a reason code,
and a vector that failed to score is neither. Feature production is the platform's primary obligation;
scoring is additive.

## 9. Out of scope

Multi-class attack typing; scoring DNS or HTTP; model hot reload; a registry service; drift monitoring;
dashboards and alerting. Each is a later unit, and none is needed to prove the path.

## 10. Risks

| Risk | Mitigation |
|---|---|
| Content-driven classes (command injection, browser hijacking) leave no conn-level trace | Expected; reported per class rather than hidden in an average. A poor number here is a finding about Zeek's fields, not a failed build. |
| Class balance inverted versus production | Base-rate projection in §6; threshold chosen on validation, never on test. |
| Lab captures are not the owner's OT network | Stated in the bundle's provenance and in the report; the model is a demonstrator. |
| Replay throughput on a 5.7 GiB machine | One capture at a time; the MVP may use a subset, and says which captures it used. |
| `skl2onnx` conversion drift | The golden-vector guard (§7.2), plus pinned converter versions recorded in the bundle. |

## 11. Relationship to existing documents

- `CLAUDE.md` lists ONNX inference and predictions as not implemented. This unit implements them for
  `conn` only, and updates that section with what is then true.
- The Roadmap's model registry (Day 7) arrives here in its thinnest honest form: a directory and a frozen
  bundle contract, no service.
- `docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md` §7.3 froze one stream
  contract per shape; `prediction-v1` follows that pattern rather than inventing a per-protocol variant.

## 12. Implementation sequencing

This spec is two units. Each produces working, testable software on its own, and each gets its own plan.

**Unit A — the serving path.** The two contracts, `Prediction` and `ModelRef`, the `ModelScorer` port, the
use case, the filesystem registry, the ONNX scorer, the Flink operator, the Kafka serializers, the
ClickHouse row and mapper, and the wiring in both jobs. Provable end to end with a deliberately trivial
model — a twenty-input logistic regression fitted in a few lines on synthetic vectors — because Unit A's
claim is that the path works, not that the model is good. Every fail-fast test and the golden-vector guard
belong here.

**Unit B — the model.** The replay, the dataset build with its join-coverage reporting, training,
evaluation, the honest report, and the real bundle that replaces the trivial one. Unit B changes no Java
code if Unit A is right; if it does, that is a finding about Unit A's interfaces.

The MVP the owner asked for is both units. Unit A can be reviewed and merged while Unit B's data work
proceeds.
