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
- Feature vector: exactly 20 `float32` values, ordered per `contracts/features/conn-feature-schema-v1.json`. Feature order is frozen once defined — a change creates a new schema version.
- CPU-only: no GPU, no CUDA, no deep-learning frameworks. ONNX Runtime Java with intra/inter-op threads pinned to 1 per subtask.
- Model bundle is pinned in job config and loaded once in `open()`. No live hot reload.
- ClickHouse is never on the online scoring path. Predictions go to Kafka first; the archive job writes to ClickHouse asynchronously.
- ClickHouse inserts are idempotent (`ReplacingMergeTree`). Do not promise exactly-once for the archive sink.
- Bounded per-`(sensor, sourceIp)` state only — no unbounded per-IP maps or event history.

**Contracts (`contracts/`)** are immutable, content-hashed, and language-neutral. A schema change creates a new version (`-v2`), never edits an existing file.

**Tech stack:** Java 21 / Flink 2.2.1 / `flink-connector-kafka:5.0.0-2.2` (Sink V2 / FLIP-27 Source API only — `SourceFunction`/`SinkFunction` V1 were removed in Flink 2.x) / Jackson 2.17 / ONNX Runtime Java / JUnit 5 / Python 3.10+ / scikit-learn / skl2onnx / onnxruntime / Testcontainers 1.19.8 (Kafka) for E2E.

## Implementation state

The conn foundation pipeline (Steps 2-7) is implemented and merged to `main`.
The ClickHouse archive job (Step 8) is complete on `feat/clickhouse-archive-job`
but not yet merged to `main`. Implementation order is tracked in
`Repository_Structure.md` Section E (18 steps).

Working today, end to end: external `conn` topic → parse/validate → bounded keyed
state → 20-value `FeatureVector` → `netsec.conn.feature-vector.v1` and
`netsec.conn.dlq.v1` → archive job → ClickHouse `feature_vectors` and
`invalid_events`.

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
