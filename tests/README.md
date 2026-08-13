# tests/ (root)

Cross-module and cross-language tests and fixtures — distinct from the per-module
`src/test/java/...` unit tests inside each `modules/*` (and `training/tests/`).

| Directory | Contents |
|---|---|
| `fixtures/zeek_conn/` | Sanitized `conn` records for source-contract and parser tests |
| `fixtures/feature_golden/` | Golden `conn-feature-v1` vectors for the canonical Java extractor |
| `fixtures/onnx_parity/` | The 10,000-vector Python/Java ONNX parity corpus |
| `fixtures/training/` | Small labelled fixture dataset for the Day-1 hard-gate fallback (fixture/synthetic model if real labels aren't ready) |
| `integration/` | Root Maven/Docker-backed integration tests spanning multiple modules |
| `e2e/` | Full pipeline tests exercised by `scripts/verify-e2e.sh` |
| `performance/` | Replay-benchmark scaffolding paired with `scripts/performance/` |

Not yet committed — Step 1 scope is directory skeleton only.
