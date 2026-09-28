# S7comm Stage 1 detector v2 — model card

## What it is

This is the S7comm Stage 1 anomaly detector, retrained by this project on four real sources of
normal S7 polling. The delivered model (`models/S7/`, `v4_causal_final_r1`) was trained on a single
QUT connection, and it scored independent benign S7 traffic 0% NORMAL in the scoring plan's
pre-check (2026-09-28). The model team could not be reached, so this project trained the
replacement.

The contract is the delivered one, unchanged (spec
`docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md` section 3):
- the 16 raw features of `s7comm-feature-v1`;
- the `StableNumericTransformer` recipe;
- an LSTM autoencoder with hidden 32 and latent 16, over a 16-event window;
- the last event's weighted squared error, with weight 0 on the `s7_operation` columns;
- a per-group conformal decision.

Every feature comes from the platform's own Java code (`S7commFeatureExport`), so training and
live scoring cannot disagree about what a feature is.

## Release

- Release id: `v2_multisource_r1`. It sits under
  `models/S7v2/models/stage1_anomaly/v2_multisource_r1/` on the development machine and under
  `/root/s7data/v2/run-2/release/` on server3. `models/` is git-ignored: the release is data,
  pinned by its own `FROZEN_MANIFEST.json`.
- It is one column wider than the delivered model (22 against 21). The fitted operation categories
  are `FUNCTION_0x44`, `FUNCTION_0x84`, `READ_VAR`, `SETUP_COMMUNICATION` and `WRITE_VAR`, spelled
  exactly as the Java decoder writes them. The ROSCTR categories are 1, 3 and 7 (Plan A ruling A2).
- The chosen candidate is `id0.0-equal`: training weight 0 on the operation columns, every
  source weighted equally, and write requests raised to 10%.
- The five scoring files and their SHA-256:

| file | SHA-256 |
|---|---|
| `artifacts/model/s7comm_lstm_autoencoder.onnx` | `59992382178b1f367e6f189e41f69ceaee0b96d69f228b8a3d631bc5c2571002` |
| `artifacts/model/preprocessor_contract.json` | `0b2319cd48ca282959c2864158f293da52ccc2fd88fb9e049ec6453241ffde6f` |
| `artifacts/model/causal_online_shadow_policy.json` | `227ccc3841af889cfc55e59e152332428771478e8c1d207c502dd745aca68f73` |
| `artifacts/model/causal_online_conformal_calibration_scores.npz` | `b36a62590478e7c1eb48c1d2cedd6490ed72ec0e321a9bbc77ab9bd165a2a3ed` |
| `artifacts/model/shadow_deployment_manifest.json` | `0e8aa866112b0ff5d501ee66c91f8bed909ea7bf63a5025ca3ce5c5e94e024f0` |

## How it got here: run-1 failed, run-2 is the release

**Run-1 used the delivered recipe's training stride of 8.** It failed G1 on server3-benign (54.20%
NORMAL). The loss scores a window's last event only, and at stride 8 every training window of a
request/response poller ends on the same direction. That was true of 0% of the windows of 4SICS
HMI, server3 and libnodave: none ended on a request, so the model never learned their requests.
It reconstructed each one as a response, at one near-constant score (about 0.205). The threshold
fell inside that cluster, so pass or fail came down to the third decimal. The training data alone
showed it: training loss fell to 0.0005 while the validation score rose from the first epoch.

**The owner amended the recipe to stride 1**, the same day, and chose to judge run-2 on the same
test parts. Those test parts were therefore seen once before run-2: run-1's results were read,
and the stride fix was motivated by training evidence, not by test scores. Nothing else changed:
data, split, candidates, gates and seed are all the same. `training/tests/unit/s7comm/test_config.py`
pins the shipped config against the aliasing.

## Reproduce

On server3, from a `git archive` of this branch, with the online job's shaded JAR built:

```sh
bash training/s7comm/acquire.sh /root/s7data/v2 <bootstrap-online-job-*-all.jar>
bash training/s7comm/run-pipeline.sh /root/s7data/v2 <bootstrap-online-job-*-all.jar> configs/s7comm-v2.yaml run-N
```

- `training/s7comm/sources.sha256` pins every capture by SHA-256, and the source repositories by
  commit.
- `acquire.sh` gave 19 captures and 0 rejected records.
- Run-2's selection and final training took 55 minutes on 16 CPUs.

## Not yet measured

- Attack detection (G2) and the comparison with published models (spec section 8) are Plan B.
- Until then, v2 is known to be quiet on the normal traffic below, and nothing more.

## What the unseen and reported rows say

- **`s7comm-clean` is a client no training source contains**, and it scores 100% NORMAL.
- **`cyclic-1s` is S7 "cyclic data": the PLC pushes values on a subscription, with no polling.** No
  training source works that way, and it scores 0.74% NORMAL. A site running cyclic
  subscriptions would need its own traffic in training (limits, below).
- **The engineering sessions are all flagged, as the 4SICS non-HMI sessions are:** block
  downloads, block lists, PLC status and time, variable tables. Those are exactly the operator
  actions an S7 anomaly detector should surface.
- **Leave-one-source-out is where generalisation is weakest.** Held out, qut-control scores 0% and
  4SICS HMI about 14%: each is a polling style no other training source has. This is why v2 is
  trained on all four, and why a site should retrain on its own traffic.

## Limits (spec section 10)

- v2 is general only across the polling styles its data contains (four real training sources).
  Trust at a real site comes from re-running the pipeline on that site's own traffic.
- The QUT attacks are the only S7-level attack evidence; the server3 attack captures are
  TCP/ICMP-level and not usable for S7 detection.
- ICSNPP (icsnpp-s7comm 7ebeb03) logs only the first two PDUs of the public WinCC captures, so the
  sensor misses that HMI traffic entirely; recorded as a sensor finding, outside this unit.
- Attack connections shorter than the 16-event warm-up are never scored (a property of the
  contract, shared with v1).

---

## Generated card: S7comm Stage 1 detector v2_multisource_r1

Generated from `evaluation_summary.json` by `netsec_ml.s7comm.evaluate.render_card`.

### Gates

- **G1** (99% NORMAL past each connection's 64th event, every normal source's held-out rows): PASSED
- **G3** (ONNX vs PyTorch within 0.0001): PASSED, max difference 4.47e-07

### Held-out normal traffic (gated)

| source | scored | past 64th | NORMAL past 64th | NORMAL 16th-64th |
|---|---|---|---|---|
| 4sics-hmi | 26692 | 26692 | 100.00% | - |
| libnodave-bench | 1437 | 1437 | 100.00% | - |
| qut-attack-hmi | 12941 | 12892 | 99.96% | 87.76% |
| qut-control | 35662 | 35662 | 99.84% | - |
| server3-benign | 1715 | 1666 | 100.00% | 100.00% |

### Unseen clients (reported, not gated)

| source | scored | past 64th | NORMAL past 64th | NORMAL 16th-64th |
|---|---|---|---|---|
| cyclic-1s | 185 | 136 | 0.74% | 0.00% |
| s7comm-clean | 372 | 323 | 100.00% | 100.00% |

### Engineering and other sessions (reported, not gated)

| source | scored | past 64th | NORMAL past 64th | NORMAL 16th-64th |
|---|---|---|---|---|
| 4sics-other | 86 | 12 | 0.00% | 1.35% |
| engineering | 933 | 689 | 0.00% | 0.00% |

### Per score group, gated rows past the 64th event

| group | scored | NORMAL |
|---|---|---|
| RESPONSE | 39176 | 99.91% |
| READ_REQUEST | 38386 | 99.93% |
| WRITE_REQUEST | 787 | 100.00% |
| OTHER_REQUEST | 0 | - |

### Selection

Chosen candidate: `id0.0-equal`.

| candidate | mean leave-one-source-out NORMAL | per held-out source |
|---|---|---|
| id0.5-equal | 53.32% | qut-control 0.00%, 4sics-hmi 13.60%, server3-benign 100.00%, libnodave-bench 99.69% |
| id0.0-equal | 53.47% | qut-control 0.00%, 4sics-hmi 14.22%, server3-benign 100.00%, libnodave-bench 99.68% |
| id0.5-proportional | 46.04% | qut-control 0.00%, 4sics-hmi 13.16%, server3-benign 100.00%, libnodave-bench 70.98% |

### Data

| source | role | captures | rows by part |
|---|---|---|---|
| qut-control | normal | qut-control | train 166720, validation 35662, test 35662, gap 128 |
| 4sics-hmi | normal | 4sics-151020, 4sics-151021, 4sics-151022 | train 124857, test 26692, validation 26691, gap 128 |
| server3-benign | normal | server3-s7, server3-s701, server3-s702, server3-S7COMM | train 8372, validation 1730, test 1730, gap 128 |
| libnodave-bench | normal | libnodave-bench | train 7004, validation 1437, test 1437, gap 128 |
| qut-attack-hmi | normal-test | qut-attack | test 12956 |
| s7comm-clean | unseen | s7comm-clean | test 387 |
| cyclic-1s | unseen | cyclic-1s | test 200 |
| engineering | report | eng-readDiagData, eng-readVarTab, eng-plc-status, eng-blocklist, eng-download-db1, eng-plc-time, eng-snap7-everything | test 1085 |
| 4sics-other | report | 4sics-151020, 4sics-151021, 4sics-151022 | test 250 |

Inference: 110 us per window (ONNX Runtime, batch 1, one thread).
