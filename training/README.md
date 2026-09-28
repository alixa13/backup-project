# training/

Independent, CPU-only Python project for the Network Security ML Platform.

## Hard rule

This project consumes canonical `FeatureVector` rows already materialized by the
Java/Flink pipeline (`modules/application/.../feature`). It must **never**:

- parse raw Zeek `conn` records,
- recompute, reorder, or redefine any of the 20 `conn-feature-v1` values,
- duplicate normalization, windowing, categorical mapping, or default-handling logic.

The fitted `StandardScaler` and `LogisticRegression` transform are packaged inside
the exported ONNX graph, so Java performs no separate scaling at inference time.

## Layout

```
configs/            training run configuration (e.g. conn-v1-baseline.yaml)
src/netsec_ml/
  dataset/          immutable ClickHouse snapshot reader
  features/         feature-vector loading only — no extraction logic
  preprocessing/    scaler/encoder fitting (wrapped into the ONNX graph)
  training/         model training loop
  evaluation/        metrics, temporal validation, model card generation
  export/           skl2onnx export, fixed-tensor output configuration
  registry/         model bundle manifest writer for adapter-registry-filesystem
  cli/              command-line entry points
tests/
  unit/             fast, no external services
  integration/      ClickHouse snapshot + full pipeline
  parity/           Python vs. Java ONNX output comparison corpus
```

## Status

Step 1: project shell only (`pyproject.toml`, empty package tree). No ML code yet.
Implementation begins at Roadmap.md Day 6 (dataset snapshot tool).

## Setup (once implementation begins)

```sh
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## S7comm Stage 1 detector v2

Trains the S7comm anomaly detector on the platform's own features (spec
`docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md`; model card
`docs/models/s7comm-stage1-detector-v2.md`). Nothing here computes a feature:
`S7commFeatureExport` (bootstrap-online-job) writes every value.

```sh
# on the development machine: the unit tests
uv venv --python 3.12 training/.venv
uv pip install --python training/.venv/bin/python torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu
uv pip install --python training/.venv/bin/python -e 'training[s7comm,dev]'
training/.venv/bin/pytest training/tests/unit/s7comm -q
bash training/s7comm/tests/test_acquire.sh

# on server3 only: captures -> Zeek -> features -> training -> gated release
bash training/s7comm/run-pipeline.sh /root/s7data/v2 <online-job-all.jar> configs/s7comm-v2.yaml run-N
```
