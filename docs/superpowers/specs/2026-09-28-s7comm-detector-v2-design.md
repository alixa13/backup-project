# S7comm Stage 1 Detector v2 (our own model) — Design

**Status:** agreed in conversation 2026-09-28, approach and all four sections; written here for review.
**Branch:** `feat/s7comm-scoring`, after the scoring spec (`8fc9d54`) and plan (`662549c`).
**Scope in one line:** build, evaluate and release our own S7comm Stage 1 anomaly detector —
same contract as the delivered one, trained on normal traffic from several S7 polling styles —
compare it with every existing detector we can run on the same data, and hand it to the paused
S7 scoring plan as bundle `s7comm-stage1-detector/v2`.

## 1. Why

The delivered detector (`models/S7`, `v4_causal_final_r1`) scores independent benign S7 traffic
0.00% NORMAL (scoring plan, Task 1; memory `project_s7_scoring_precheck`). Upstream's own
training pipeline reproduces that result on the same capture, so the fault is the model, not the
platform: it learned "normal" from one connection. That training data is public —
[qut-infosec/2017QUT_S7comm](https://github.com/qut-infosec/2017QUT_S7comm) `control_set`
(238,172 records, one HMI → PLC connection, pipelined polling), matching the delivered
`dataset_manifest.json` row count and start time exactly. There is no model team to return to; we
are the model team. A detector is only as general as its normal data, so v2 learns normal from
several polling styles and must prove itself on normal traffic it never trained on.

## 2. What is in scope, and what is not

In scope: the data (public and server3 captures, downloaded to server3), a feature exporter that runs the
platform's own Java feature code, a training pipeline, evaluation against acceptance gates, a
comparison with existing detectors, the release in the delivered bundle format, a model card, and
one command that repeats all of it on new traffic (a real site's).

Not in scope: Stage 2 (the COMMAND/FLOODING router); S7comm-plus (the S7-1200/1500 protocol, which
the platform does not parse); wiring the model into the online job — that is the paused scoring
plan, revised afterwards (section 9).

## 3. Contract (unchanged from the delivered detector)

Input: the 16 `s7comm-feature-v1` values. Preprocessing: the delivered recipe
(`StableNumericTransformer` — median imputation, robust centre and 5-95 percentile span with a
0.1 floor, clipping at ±20, eight bounded ratios clipped to [0, 1] — plus binary pass-through and
one-hot categories), 16 → 21 values. Model: LSTM autoencoder (21 inputs, hidden 32, latent 16, one
layer), window of 16 events per connection, stride 1 online. Score: weighted squared error of the
last event, weight 0 on the `s7_operation` columns. Decision: one-sided conformal p-value per group
(RESPONSE, READ_REQUEST, WRITE_REQUEST, OTHER_REQUEST; a group without calibration scores uses all
pooled with the fallback alpha), ANOMALY iff p ≤ alpha. The released files have the delivered
layout and formats (`preprocessor_contract.json`, `causal_online_shadow_policy.json`, the
calibration `.npz`, `shadow_deployment_manifest.json`, `FROZEN_MANIFEST.json`), so the scoring
plan's loader, scorer and packaging take v2 with only the version changed.

What changes is what the preprocessing and model are fitted on, and two things the new data makes
unnecessary: the scoring spec's S1 and S2 input fixes existed only to match the delivered model's
pcap-parser training table. v2 trains on the platform's own features from ICSNPP records, so
there is nothing to match: v2's categories are fitted on `S7commCategories.decodeOperation`
strings as they are, and user-data PDUs keep the function codes Zeek writes. The scoring plan
drops both (section 9).

## 4. Data

Revised 2026-09-28, after scoring the delivered model on real public captures: several
independent real sources of normal S7 polling exist, so the synthetic traffic lab of the first
draft is dropped. Attack detection is measured on QUT's published, labelled attacks.

Everything lives on server3 under `/root/s7data/v2` (never in Git): downloads, Zeek logs, feature
exports, runs. Every file is pinned by SHA-256 (git sources also by commit) in
`training/s7comm/sources.sha256`.

| Logical source | Captures | Client(s) | What it is | Events | Role |
|---|---|---|---|---|---|
| `qut-control` | QUT `20161219132813_control_set/master.pcap` (pinned as its `hmi.pcap`: the same 238,172 S7 records seen from the HMI's side, only sub-ms timestamps differ; final review, 2026-09-28) | 10.10.10.20 | the delivered model's training connection: pipelined HMI, 2 PDU refs | 238,172 | normal: train/val/test |
| `4sics-hmi` | 4SICS Geek Lounge 151020, 151021, 151022 (Netresec) | 10.10.10.20 | a real lab HMI polling a real S7 PLC for ~25 h, one request at a time | 178,362 | normal: train/val/test |
| `server3-benign` | `s7`, `s701`, `s702`, `S7COMM` (another project's, read-only copies) | 192.168.10.100 | a lab poller, one request at a time, new PDU ref per request | 11,960 | normal: train/val/test |
| `libnodave-bench` | ITI `s7comm_varservice_libnodavedemo_bench.pcap` | 192.168.1.10 | libnodave benchmark against a real S7-300: fast reads and writes | 10,006 | normal: train/val/test |
| `qut-attack-hmi` | QUT `20161215163606_s7_process_attacks/master.pcap` | 10.10.10.20 | the same HMI during the attack run | 258,790 | normal: test only |
| `s7comm-clean` | ITI `S7Comm/s7comm_clean.pcap` | 192.168.0.21 | a read poller never seen in training | 387 | unseen client: test only |
| `cyclic-1s` | automayt `2-S7comm-VarService-CyclicData-1s.pcap` (LFS) | 192.168.1.20 | 1 s cyclic reads with user-data, never seen in training | 200 | unseen client: test only |
| `qut-attack` | QUT attack run + `master.csv` frame labels | 10.10.10.66 | process-command and flooding attacks | ~1.33 M | attack: test only |
| `engineering` | ITI STEP7/Snap7 sessions: diagnostics, variable tables, PLC status, block list, block download, PLC time, Snap7 "everything" | various | legitimate but rare engineering operations | ~1,100 | report only |
| `4sics-other` | the three 4SICS captures | 10.10.10.30, 192.168.1.10, 192.168.2.42 | operator writes and conference attendees' short sessions | ~250 | report only |

Excluded, with the reason recorded: the Mendeley medical-waste dataset (S7comm-plus, damaged
frames); the S7-1200/1500 HMI captures (S7comm-plus, which the platform does not parse); the
WinCC captures (ICSNPP logs only their first two PDUs, a sensor limitation noted in section 10).

## 5. Features from the platform's own code

`S7commFeatureExport`, a CLI in `bootstrap-online-job` next to `ZeekRecordCheck`, reads Zeek
`s7comm.log` JSON lines and runs every record through the real `JsonZeekS7commParser`,
`S7commEventMapper` and `S7commBuildFeaturesUseCase`, one `S7commConnectionState` per uid,
exactly as `S7commFeatureProcessFunction` does (including the fresh-state rule). It writes CSV:
source tag, uid, timestamp, event id, direction, function code, the 16 values, and the decoded
`s7_rosctr` and `s7_operation` strings. Rejected records are counted and written to a sidecar.
The Python side never computes a feature (the `training/` project's hard rule), so a trained model
and the live scorer cannot disagree about what a feature is. It runs on server3 with the Flink
image's Java, as `deploy.sh zeek-check` runs `ZeekRecordCheck`.

## 6. Training

In `training/src/netsec_ml/s7comm/` (the currently empty training project), run in a throwaway
`python:3.12-slim` container on server3 with CPU-only PyTorch, scikit-learn, onnx and onnxruntime.
CLAUDE.md's "no deep-learning frameworks" invariant becomes "none on the serving path; CPU-only
PyTorch in `training/` only" (the online job keeps only ONNX Runtime).

- **Split:** per normal logical source, chronological 70/15/15 train/validation/test by timestamp,
  with a gap of min(64, 1% of the source) events between parts (the delivered recipe's gap, shrunk
  for small sources). Test-only, attack and report sources are never trained or calibrated on.
- **Preprocessing:** fitted on the pooled training part only.
- **Sequences:** length 16 per connection, stride 1 for training and for scoring; weighted so
  every source contributes equally per epoch, and write endpoints are up-weighted to 10% of
  samples (the delivered recipe's write handling). *Amended 2026-09-28, the owner's decision after
  run-1:* the delivered recipe's training stride was 8. Under the last-event-only loss, stride 8
  ends every training window of a request/response poller on the same direction: 0% of the
  windows of 4SICS HMI, server3 and libnodave ended on a request. So the model never learned their
  requests, and it failed G1 on server3 (54.20%). Run-2 is judged on the same test parts that
  run-1 was, and the model card says so.
- **Model and loss:** the delivered `LSTMAutoencoder`; `WeightedLastTimestepMSE` with the
  `s7_operation` columns at training weight 0.5 (the delivered selection) and scoring weight 0.
- **Optimisation:** Adam, learning rate 1e-3, weight decay 1e-5, batch 256, gradient clip 1.0, up to
  40 epochs, early stopping after 7 without validation improvement, seed 42.
- **Calibration:** last-event scores on the validation part, per group; alpha 0.001 for
  RESPONSE and READ_REQUEST; a group with n < 1000 calibration scores takes the smallest attainable
  p-value, 1/(n+1), as the delivered WRITE_REQUEST did; a group with none uses the pooled fallback
  (0.001).
- **Export:** ONNX (`input` `[batch,16,21]` → `reconstruction`), parity-checked against PyTorch on
  validation windows, and the release files of section 3 in the delivered directory layout, with
  a `FROZEN_MANIFEST.json` of every file's SHA-256.
- **Candidates:** chosen on validation data and leave-one-source-out runs on the training sources
  only; the test parts are scored once, by the final model.

One script, `training/s7comm/run-pipeline.sh`, takes the source list (captures, clients, roles)
and runs Zeek (the deployed image), the exporter, training, evaluation, the comparison and the
release — so a real site's traffic, once the sensor sees it,
is one run away from a site model.

## 7. Acceptance

Gates (all must hold on the final model's test data):

- **G1** — every normal source's held-out test part scores ≥ 99% NORMAL after the connection's
  64th event.
- **G2** — QUT attack run: ≥ 99% of flooding-attack events flagged, and every command-attack
  episode detected (at least one ANOMALY among its scored events).
- **G3** — ONNX and PyTorch reconstructions agree within 1e-4.

Reported, not gated: the unseen clients' NORMAL rate (`s7comm-clean`, `cyclic-1s`) and the
leave-one-source-out NORMAL rate — a polling style the model never saw, the honest
generalisation numbers; the engineering and 4SICS-other sessions' results; the 16th-64th-event false-alarm rate; per-group results; inference
cost per window; event-level recall per attack kind.

If a gate fails, the causes go to the owner before anything is released; candidates are never
tuned on test data.

## 8. Published models: inspiration and comparison

The owner asked for this explicitly (2026-09-28): find the models others have published for S7 /
ICS network anomaly detection, learn from them, and compare v2 with them on the same data. Each
one below is reproduced from its paper or code, trained on the same normal training parts,
thresholded on the same validation parts, and scored on the same test data with the same
metrics (normal rate per normal test part, detection per attack episode, event-level recall).
The results table and a short review of each (what it models, what it needs, where it wins or
loses) go in `docs/models/s7comm-related-work.md` and the model card.

| # | Model | Published as | What it learns | Input here |
|---|---|---|---|---|
| C1 | Delivered v1 LSTM autoencoder (`v4_causal_final_r1`) | the model team's release | reconstruction of 16-event windows, one connection's style | our exported features |
| C2 | QUT / Digital Bond S7 rules (`S7Rules.txt`) | Rodofile et al., ACISP 2017; Digital Bond 2015 | allow-list: S7 read/write/setup from unauthorised hosts | the pcaps, via Suricata; allow-list per source |
| C3 | Package signatures + stacked LSTM next-signature classifier | Feng, Li & Chana, IEEE DSN 2017 | a database of normal message signatures (content level) and an LSTM predicting the next signature (time-series level); anomaly if unseen, or not in the top-k | signatures built from our exported records |
| C4 | Discrete-time Markov chain / statechart of DFAs | Kleinmann & Wool, arXiv:1607.07489 (Siemens S7 traffic) | the cyclic symbol patterns of each channel; anomaly on a transition normal traffic never makes | symbols (direction, ROSCTR, operation) from our records, per connection |
| C5 | Kitsune (AfterImage + KitNET autoencoder ensemble) | Mirsky et al., NDSS 2018; github.com/ymirsky/Kitsune-py | incremental per-channel packet statistics, reconstructed by an ensemble of small autoencoders | the pcaps; packet scores joined to S7 records by timestamp |
| C6 | USAD (adversarially trained twin autoencoders) | Audibert et al., KDD 2020 | reconstruction of multivariate windows | our preprocessed 16x21 windows |
| C7 | Classic baselines: isolation forest on single events; per-connection rate and inter-arrival mean +- 2 sigma | textbook | single-event outliers; rate shifts | our exported features |

**Inspiration.** What a published model does better is examined for what v2 can adopt without
changing its contract (a training choice, a calibration choice, a data choice): such a change is
tried as a v2 candidate, selected on validation data only. An idea that needs a different contract
(for example C3's content-level "never-seen signature" check, or C4's per-channel automaton as a
second opinion) is written up as a proposal for a later version, with the comparison numbers that
justify it, and is never folded silently into v2.

**Honesty rules.** A model that cannot be run (code that no longer builds, missing parts) is listed
with the reason and whatever its paper reports, clearly marked as not reproduced here. Numbers a
paper reports on other data or with other metrics are quoted as such and never mixed into our table.

## 9. After release

The model is packaged as `models/s7comm-stage1-detector/v2/` and committed as the scoring plan's
test fixture (the owner's decision for fixtures). The scoring plan is then revised before it
resumes: its Task 1 re-runs on v2 (it must pass now), Task 2 (S2) and the S1 upper-casing are
dropped, its fixture bundle, Python-computed test numbers and oracle are regenerated from v2, and
the packaging script takes the release directory and version as arguments. A model card,
`docs/models/s7comm-stage1-detector-v2.md`, records the data, splits, recipe, gates, comparison
and limits.

## 10. Limits stated up front

- v2 is general only across the polling styles its data contains (four real training sources).
  Trust at a real site comes from re-running the pipeline on that site's own traffic.
- The QUT attacks are the only S7-level attack evidence; the server3 attack captures are
  TCP/ICMP-level and not usable for S7 detection.
- ICSNPP (icsnpp-s7comm 7ebeb03) logs only the first two PDUs of the public WinCC captures, so the
  sensor misses that HMI traffic entirely; recorded as a sensor finding, outside this unit.
- Attack connections shorter than the 16-event warm-up are never scored (a property of the
  contract, shared with v1).
