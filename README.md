# Network Security ML Platform

Kafka `conn` telemetry → Flink feature pipeline → CPU ONNX inference → Kafka outputs →
asynchronous ClickHouse archive. Built for two engineers, Java 21 / Flink 2.2.1,
an external Kafka cluster, and **no GPU**.

> **Status: Step 1 — repository skeleton.** No feature logic, infrastructure
> behavior, or model code has been implemented yet. This commit only proves the
> build boundaries exist and compile/build cleanly. See
> [`Repository_Structure.md`](./Repository_Structure.md) Section F for the exact
> scope and Definition of Done, and Section E for the full 18-step implementation
> order that follows.

## Architecture in one paragraph

One canonical Java feature pipeline runs inside Flink for both online scoring and
bounded historical backfills. Python trains only from the `FeatureVector` rows that
pipeline materializes — it never reimplements parsing, normalization, windowing,
categorical mapping, defaults, or feature ordering. The online job publishes durable
feature and prediction events to internal Kafka topics; a separate archive job writes
them to ClickHouse in batches, so ClickHouse is never a synchronous dependency for a
prediction. The MVP model is one immutable, pinned ONNX bundle — no hot reload, no
feature store, no Kubernetes, no multi-model routing.

## CPU-only constraint

There is no GPU in this environment. The MVP model is a CPU-friendly
`StandardScaler` + `LogisticRegression` pipeline exported to ONNX and served with
ONNX Runtime Java on CPU (target: 8 CPU / 32 GiB caps, ONNX intra/inter-op threads
pinned to 1 per subtask). Do not introduce GPU-only dependencies, deep learning
frameworks, or CUDA/TensorRT tooling into any module.

## Module boundaries (hexagonal architecture)

```
domain            pure Java values and feature/domain formulas — NO Kafka, Flink,
                   ClickHouse, ONNX Runtime, Jackson, or Docker imports
ports             input/output port interfaces — depends only on domain
application       use cases, validation, feature orchestration — depends only on
                   domain + ports
adapter-kafka     Kafka DTO/parser/mapper/source/sink — implements application ports
adapter-flink     watermarks, bounded keyed state, Flink process functions
adapter-onnx      ONNX Runtime Java loader, in-process CPU inference, startup validation
adapter-clickhouse   batched archive writer only — never on the online scoring path
adapter-registry-filesystem   read-only, content-addressed model bundle registry
adapter-monitoring   structured logging + Prometheus metrics
bootstrap-online-job    composition root: online Flink feature-and-score job
bootstrap-archive-job   composition root: independent Kafka-to-ClickHouse archive job
training/         independent Python project — consumes canonical feature vectors,
                   never raw Zeek parsing logic
contracts/        immutable, versioned, language-neutral source/domain/feature/
                   stream/dataset/model contracts — the shared source of truth
```

Dependency direction is one-way: `domain → ports → application → adapters →
bootstrap`. Bootstrap modules are the only place concrete adapters are wired
together. Python depends on `contracts/` and archived feature vectors — never on
Java classes or raw-event preprocessing.

See [`FINAL_ARCHITECTURE.md`](./FINAL_ARCHITECTURE.md) for the full architecture
review and rationale, and [`Roadmap.md`](./Roadmap.md) for the 20-day execution plan.

## Building

```sh
./mvnw clean verify      # Java reactor (empty modules at Step 1)
```

```sh
cd training
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"  # Python project shell (no ML implementation at Step 1)
pytest
```

## Repository layout

Full annotated tree: [`Repository_Structure.md`](./Repository_Structure.md), Section A.
Every top-level directory below also carries its own `README.md`.

| Directory | Purpose |
|---|---|
| `contracts/` | Versioned, language-neutral contracts (source, domain, feature, stream, dataset, model) |
| `modules/` | Java 21 Maven reactor — hexagonal architecture |
| `training/` | Independent CPU-only Python training project |
| `config/` | Non-secret environment configuration |
| `infrastructure/` | Declarative ClickHouse/Kafka/monitoring/Flink-state setup |
| `docker/` | Development overlays — extends existing local services |
| `models/` | Read-only runtime model mount (artifacts excluded from Git) |
| `scripts/` | Safe Bash entry points grouped by purpose |
| `tests/` | Fixtures plus cross-module, E2E, and performance tests |
| `docs/` | Living documentation and architecture decision records |
| `artifacts/` | Ignored benchmark and release evidence |
