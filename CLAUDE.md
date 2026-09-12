# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Java (Maven reactor)
```sh
./mvnw clean verify                          # build + test all modules
./mvnw test -pl modules/domain               # test a single module
./mvnw test -pl modules/domain -Dtest=ProtocolTest   # run one test class
./mvnw package -pl modules/bootstrap-online-job -am  # build deployable JAR
```

Three things about running the tests that will otherwise cost you an hour:

- **`clean verify` does not complete on a small machine.** The container tests are
  OOM-killed on 5.7 GiB. Run them in stages instead: `./mvnw install -DskipTests`
  once, then one module at a time, clearing containers between
  (`docker ps -aq | xargs -r docker rm -f`). Everything passes staged; nothing
  proves the whole reactor green in one command until CI has more memory.
- **`-pl <module>` without `-am` resolves siblings from `~/.m2`**, producing
  phantom "cannot find symbol" errors against code that is fine.
- **`-Dtest=X` with `-Dsurefire.failIfNoSpecifiedTests=false` reports BUILD SUCCESS
  having run zero tests.** Always confirm the actual `Tests run:` count before
  believing a green build.

### Python (training/)
```sh
cd training
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                           # all tests
pytest tests/unit/               # unit tests only
pytest -k test_name              # single test
```

## Architecture

**Data flow:** External Kafka (`conn` topic) → Online Flink job (parse → validate → bounded keyed state → 20-feature vector → ONNX inference) → internal Kafka topics → Archive Flink job → ClickHouse.

**Hexagonal, one-way dependency chain:**
```
domain → ports → application → adapters → bootstrap
```
- `domain` — pure Java value objects and formulas. Zero imports of Kafka, Flink, ClickHouse, ONNX Runtime, Jackson, or Docker. Enforced by the module's own compile-time classpath.
- `ports` — input/output port interfaces. Depends only on `domain`.
- `application` — use cases and feature orchestration. Depends only on `domain` + `ports`. No Jackson, no Flink.
- `adapter-kafka/flink/onnx/clickhouse/registry-filesystem/monitoring` — each implements ports and carries framework imports. Adapters never import each other.
- `bootstrap-online-job` / `bootstrap-archive-job` — the **only** modules that wire concrete adapters together into a runnable Flink job.
- `training/` — independent Python project. Reads archived `FeatureVector` rows and contracts. Never reimplements Zeek parsing, normalization, windowing, categorical mapping, defaults, or feature ordering.

**Key invariants:**
- Java package root: `io.netsecml.platform`
- Java 21 throughout — use `record` for immutable data carriers, `sealed interface` + records + pattern-matching `switch` for closed hierarchies. Records with array components need defensive copies in compact constructor *and* in the accessor.
- Feature vector: exactly `schema.featureCount()` `float32` values, frozen per schema. Conn's registered schema reports 20, ordered per `contracts/features/conn-feature-schema-v1.json`. Feature order is frozen once defined — a change creates a new schema version.
- CPU-only: no GPU, no CUDA, no deep-learning frameworks. ONNX Runtime Java with intra/inter-op threads pinned to 1 per subtask.
- Model bundle is pinned in job config and loaded once in `open()`. No live hot reload.
- ClickHouse is never on the online scoring path. Predictions go to Kafka first; the archive job writes to ClickHouse asynchronously.
- ClickHouse inserts are idempotent (`ReplacingMergeTree`). Do not promise exactly-once for the archive sink.
- Bounded per-`(sensor, sourceIp)` state only — no unbounded per-IP maps or event history.
- `NetworkEvent` is a **sealed interface** over a shared `EventEnvelope`, with one record per log
  type. `permits` lists only log types that have a parser, mapper and feature schema — adding a
  record ahead of its implementation defeats the exhaustiveness checking that sealing buys.
- Feature schemas resolve through `FeatureSchemaRegistry`, by `LogType` at wiring time or by
  `schemaId` for an archived row. Both throw on an unknown key: an unresolvable schema is a
  deployment error, not a runtime condition. A vector's width, id and content hash all come from
  its registered schema, never from a literal.
- `BuildFeaturesUseCase<E, S>` has one implementation per log type, so no implementation casts or
  switches to discover what it was given.

**Contracts (`contracts/`)** are immutable, content-hashed, and language-neutral. A schema change creates a new version (`-v2`), never edits an existing file.

**Tech stack:** Java 21 / Flink 2.2.1 / `flink-connector-kafka:5.0.0-2.2` (Sink V2 / FLIP-27 Source API only — `SourceFunction`/`SinkFunction` V1 were removed in Flink 2.x) / Jackson 2.17 / ONNX Runtime Java / JUnit 5 / Python 3.10+ / scikit-learn / skl2onnx / onnxruntime / Testcontainers 1.21.4 (Kafka) for E2E.

## Implementation state

The conn foundation pipeline (Steps 2-7) is implemented and merged to `main`.
The ClickHouse archive job (Step 8) is complete on `feat/clickhouse-archive-job`
but not yet merged to `main`. Implementation order is tracked in
`Repository_Structure.md` Section E (18 steps).

The pipeline is: external `conn` topic → parse/validate → bounded keyed state →
20-value `FeatureVector` → `netsec.conn.feature-vector.v1` and
`netsec.conn.dlq.v1` → archive job → ClickHouse `feature_vectors` and
`invalid_events`.

**`main` cannot currently run the online job at all.** Three serialization defects
(`SensorId` and both Kafka serializers not `Serializable`; two
`setValueSerializationSchema` lambdas erasing their generic type) make
`env.execute()` fail before a single record is read. The fix is on
`fix/flink-job-serializability`, which should land before this branch. Until it
does, treat any claim that the pipeline "works" as applying to
`feat/clickhouse-archive-job` only.

### Verification state

Docker was unavailable for most of this branch's development, so every
Testcontainers test SKIPPED and read as neutral. When Docker became available the
skips were hiding real defects — including a deduplication query that was
syntactically invalid and could never have executed. **Do not read a skipped
container test as a passing one.**

Executed and green on `feat/clickhouse-archive-job`, against real containers:

| Suite | Result | What it actually proves |
|---|---|---|
| `adapter-clickhouse` | 37/37, 0 skipped | Includes `DdlMigrationTest` — `001_mvp_tables.sql` has now been executed by a real ClickHouse 25.8 server, not merely read |
| `FeatureVectorDeduplicationTest` | 3/3 | The committed dedup query runs and resolves duplicates |
| `ClientV2InserterTest` | 4/4 | An unknown column is rejected, not silently skipped |
| `OnlineFeatureJobE2ETest` | 1/1 | The online job can be submitted and produces a feature vector — first pass in this project's history |
| `ArchiveJobE2ETest` | 1/1 (171 s) | Kafka → ClickHouse, end to end |
| domain, ports, application, adapter-kafka, adapter-flink | pass | — |

**Not verified:** `ClickHouseOutageTest` — the Definition of Done's headline claim
that a ClickHouse failure cannot stop feature production. It is OOM-killed during
container startup (two Flink mini-clusters plus two containers do not fit in
5.7 GiB) and has never run. It was deliberately not weakened to fit the machine.

The common feature tier (`contracts/features/common-feature-tier-v1.json`) and
its `conn.log` enrichment carrier are implemented, but no protocol consumes them
yet — `conn-feature-v1` predates the tier and is frozen without it. The first
consumer is the DNS unit.

Not yet implemented: ONNX inference (Day 9), predictions and `netsec.prediction.v1`
(Day 9), the model registry (Day 7), and the Python training project.

Design records: `docs/conn-foundation-pipeline.md` (Steps 2-7),
`docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md` and
`docs/clickhouse.md` (Step 8).

## Key reference files

| File | Purpose |
|---|---|
| `FINAL_ARCHITECTURE.md` | Architecture decisions and rationale |
| `PILOT_ARCHITECTURE.md` | Phased architecture implementation guide |
| `Repository_Structure.md` | Annotated tree, build map, 18-step implementation order |
| `Roadmap.md` | 20-day execution plan |
| `contracts/` | Canonical, immutable interface contracts |
| `.env.example` | All environment variable names (never commit `.env`) |
