# S7comm Stage 1 Scoring Implementation Plan

> **Revision (2026-09-28, detector v2).** Task 1's pre-check stopped this plan: the delivered
> model scores independent benign S7 traffic 0% NORMAL. It resumes against our own detector v2
> (`docs/models/s7comm-stage1-detector-v2.md`, release `v2_multisource_r1`). Its Task 1 re-runs on
> v2 first and must pass now. The bundle is packaged as `models/s7comm-stage1-detector/v2/` and
> committed as this plan's test fixture (spec section 9). Four changes are made when execution
> resumes, and ledgered there:
> 1. **Drop Task 2 (S2) and S1's upper-casing.** v2 was trained on `S7commFeatureExport`'s rows,
>    i.e. on exactly what the online job computes. Those fixes made Java's input look like the
>    delivered training table, and v2 has no such gap. v2's own categories spell operations as
>    the Java decoder does (`FUNCTION_0x44`, not `FUNCTION_0X44`).
> 2. **Packaging takes the release directory and file names as arguments** (Plan A ruling A4):
>    `artifacts/model/` and `s7comm_lstm_autoencoder.onnx`, not `artifacts/v4_causal_final_model/`
>    and `…_debiased.onnx`.
> 3. **The loader reads the one-hot width from the bundle** (Plan A ruling A2): v2's is 22, not 21.
> 4. **The fixture bundle and every number pinned against the delivered model are regenerated
>    from v2:** the oracle, the thresholds, and the live-check expectations.
>
> **Before resuming at all, the owner rules on v2's long-lived-connection limit.** Every event of a
> read-only connection is flagged once `s7_same_function_run_length` passes about 73,000, which is
> about 20 h at one read per second (the model card's "Measured after release").

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** First measure, on independent real traffic, whether the delivered S7comm Stage 1 detector calls benign S7 traffic normal; then make two S7comm inputs match its training data, score every S7comm feature vector with it in the online job, publish one prediction per event to Kafka, and archive the predictions to ClickHouse.

**Architecture:** A Python pre-check on server3's benign S7 captures runs first and can stop the plan (Task 1). Then S2 lands in the feature use case: a ROSCTR 7 PDU's function code is read as 0x00. `s7comm-features` gains a side output of `(key, vector, freshState, client, server)`. A new keyed operator, `s7comm-score`, keeps a 16-vector window of preprocessed vectors per connection and reconstructs it through a `ReconstructionScorer` port (ONNX Runtime). It turns the last event's weighted error into a group-conditional conformal p-value and emits an `S7commDetectorPrediction` (`WARMUP` / `NORMAL` / `ANOMALY` / `UNSCORABLE`) to `netsec.s7comm.prediction.v1`. The archive job's tenth chain writes `s7comm_detector_predictions`. The model travels as a SHA-pinned bundle under `models/`, byte-identical to the delivery.

**Tech Stack:** Java 21, Flink 2.2.1, ONNX Runtime Java 1.20.0, Jackson 2.17, JUnit 5, ClickHouse 25.8, and bash + jq for deploy. Python 3 with numpy, pandas, scikit-learn, joblib and onnxruntime is used only for the pre-check, the oracle generator and the reference numbers.

**Spec:** `docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md` (read it; this plan argues from it).

## Global Constraints

- Branch `feat/s7comm-scoring` (already created from `feat/deploy-mvp` at `1cbbc04`; holds the spec at `8fc9d54`). Java package root `io.netsecml.platform`.
- Hexagonal: `domain` imports no Jackson, Flink, Kafka, ONNX Runtime or ClickHouse; `ports` depends only on `domain`; `application` on `domain` + `ports`; adapters never import each other, except the recorded `adapter-flink` → `adapter-kafka`. Anything that needs two adapters (bundle loader + ONNX) lives in a bootstrap module.
- Records with array components copy defensively in the compact constructor **and** the accessor.
- Every code block carries a short comment saying why (the codebase's convention and the user's preference).
- Test commands: `./mvnw test -pl <module> -am -Dtest='<Class>' -Dsurefire.failIfNoSpecifiedTests=false`, then **read the actual `Tests run:` line** — a green build can run zero tests. Never `-pl` without `-am`. Never run Testcontainers tests or start the deploy stack on this machine.
- Frozen detector values come from the bundle, never from code:
  - input `input` `[?,16,21]`; output `reconstruction` `[?,?,21]`;
  - weight 0 on the four `categorical__s7_operation_*` columns (Σw = 17);
  - alphas `RESPONSE` 0.001, `READ_REQUEST` 0.001, `WRITE_REQUEST` 0.015625; `OTHER_REQUEST` has no calibration and falls back to all 35,647 scores pooled with alpha 0.001.

  Tests may assert these numbers; main code must read them.
- State names are checkpoint identity. `s7comm-connection-state` is unchanged (S2 changes the inputs, not the layout). `s7comm-score-window` is new. Never rename either.
- Operator uids: online `s7comm-score`, `s7comm-prediction-sink`; archive `s7comm-prediction-source`, `s7comm-prediction-row`, `s7comm-predictions-clickhouse-sink`. Every existing uid is unchanged.
- Topic `netsec.s7comm.prediction.v1` (1 partition, 168 h). Env vars:
  - `S7COMM_PREDICTION_TOPIC`;
  - `S7COMM_DETECTOR_BUNDLE`: deploy default `s7comm-stage1-detector/v1`; an empty value disables scoring;
  - `NETSEC_MODELS_DIR`: `/opt/netsec/models`.
- Contracts in `contracts/` are immutable: add files, never edit existing ones.
- Never edit:
  - `models/S7/`, `models/modbus/` or `two-models-info/` (the deliveries);
  - the `feature/reference` test package;
  - `ModbusScoreWindow` or any other class stored as Flink state.
- The test fixture bundle `tests/fixtures/models/s7comm-stage1-detector/v1/` is produced only by the packaging script (Task 7) and committed. This applies the owner's 2026-09-28 decision for the Modbus fixture.
- server3 (`ssh server3`; the repo is `/root/mvp-project/backup-project`):
  - the other project's captures under `/root/1405-06-15/` are read-only inputs;
  - write only under `/root/s7check` (removed after use) and the repo;
  - never touch other projects' containers and never reboot;
  - never print `deploy/.env`'s password.
- `SP` below means `/tmp/claude-1000/-home-dark-Downloads-ml-platform-step1-skeleton-ml-platform/4d31708c-970b-4ced-a8a2-4b8599cc4b44/scratchpad`. Its Python venv is `$SP/venv`.
- Commit messages end with these two lines; where a task's commit step shows `<attribution lines>`, it means exactly these:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01WXKAFPjfNQKQ6biy659oN3
  ```

## Rulings this plan makes

- **P1 — The real-traffic check runs twice, both times under spec §11's stop rule.** First it runs in Python, before any code (Task 1), and can stop the plan. Then it runs through the Java production path before merge (Task 17). Throwaway probes run while planning (in `$SP/probe_s7*.py`) scored every synthetic strictly-alternating read poll `ANOMALY` (scores ~0.5, against calibration medians of 0.11 and 2e-5). Only a pipelined poller, with two requests and two PDU references in flight, scored `NORMAL`: the training capture's shape. Measuring first costs an hour and may save sixteen tasks.
- **P2 — `UNSCORABLE` marks a non-finite score, not non-finite preprocessed input.** `S7commPreprocessing`'s output is always finite: NaN is imputed and every continuous value is clipped. The spec's input guard could therefore never fire. A non-finite reconstruction is the one non-finite value possible, and calling it `NORMAL` would hide a broken model.
- **P3 — The scoring oracle checks p-values within a tolerance band and verdicts exactly.**
  - A p-value must fall within `[p_low, p_high]`, the band a score tolerance allows, rather than match bit for bit. The generator computes the band with upstream's own function.
  - Verdicts must match exactly, and the generator refuses any verdict that could flip within the tolerance.
  - Why: ONNX Runtime 1.20 (Java) and 1.30 (Python) may differ in the last bits of a reconstruction, and thousands of calibration scores sit near the bulk of real scores.
- **P4 — Deploy's `detector_bundle_setting` takes the pin's variable name as an argument** (there are two pins now), and `is_bundle_failure` recognises both loaders.

## Review Focus

1. An older `deploy/.env` without `S7COMM_DETECTOR_BUNDLE` must score with the default bundle, and an empty value must not score. A `restart` with a missing or corrupt S7 bundle must stop nothing. Pinned in Task 16.
2. A connection whose feature state expired while its score window survived must re-warm, and must never score a window spanning the gap. Pinned in Task 12 (`aConnectionWhoseStateExpiredIsFreshAgain`) and Task 6 (`freshStateStartsTheWindowAndTheCountAgain`).
3. A ROSCTR 7 record with a function name but no code, or no function at all, must still read as code 0x00. Pinned in Task 2 (`aUserDataPduWithoutACodeIsReadAsZeroToo`).
4. A window restored under a different bundle must be emptied, never scored. Pinned in Task 6 (`aWindowFromAnotherBundleIsEmptiedFirst`).
5. An `.npz` entry that is not a 1-D little-endian float64 array, or a calibration file missing a group, must be refused at load, never misread. Pinned in Task 8.

---

### Task 1: Real-traffic pre-check (Python, throwaway, can stop the plan)

**Files:** nothing in the repository. Scratch only:
- `$SP/s7pkg/`: a copy of upstream's code;
- `$SP/s7precheck.py`: the harness;
- `$SP/s7check/`: Zeek's output and the report.

**Interfaces:**
- Consumes: the delivery `models/S7/`; server3's captures; the deployed Zeek image `netsec-ml/zeek:1` on server3.
- Produces: `$SP/s7check/<capture>/s7comm.log` (reused by Task 17) and `$SP/s7check/precheck.txt`.

- [ ] **Step 1: Prepare the Python environment and upstream's package**

```bash
SP=/tmp/claude-1000/-home-dark-Downloads-ml-platform-step1-skeleton-ml-platform/4d31708c-970b-4ced-a8a2-4b8599cc4b44/scratchpad
[ -x "$SP/venv/bin/python" ] || python3 -m venv "$SP/venv"
"$SP/venv/bin/pip" install -q numpy pandas scikit-learn joblib onnxruntime
# Upstream's feature path, plus the module the pickled preprocessor's class lives in.
rm -rf "$SP/s7pkg" && mkdir -p "$SP/s7pkg/s7zeek/"{domain,features,adapters,modeling}
for d in . domain features adapters modeling; do : > "$SP/s7pkg/s7zeek/$d/__init__.py"; done
cp models/S7/src/s7zeek/domain/events.py "$SP/s7pkg/s7zeek/domain/"
cp models/S7/src/s7zeek/features/{s7_parser.py,customer_icsnpp_enriched_builder.py,customer_icsnpp_time_normalized_builder.py} "$SP/s7pkg/s7zeek/features/"
cp models/S7/src/s7zeek/adapters/kafka_source.py "$SP/s7pkg/s7zeek/adapters/"
cp models/S7/src/s7zeek/modeling/preprocessing.py "$SP/s7pkg/s7zeek/modeling/"
find "$SP/s7pkg" -name '*.py' | wc -l
```
Expected: `11`.

- [ ] **Step 2: Run Zeek over the eight captures on server3**

```bash
ssh server3 'set -e
out=/root/s7check; rm -rf "$out"; mkdir -p "$out"
for f in /root/1405-06-15/Models/data/raw/benign/pcap/s7comm/*.pcap /root/1405-06-15/Models/data/raw/attack/pcap/s7comm/*.pcap; do
  n="$(basename "$f" .pcap)"; mkdir -p "$out/$n"
  # The deployed image and policy, as deploy.sh zeek-check runs them; captures mounted read-only.
  docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint zeek \
    -v "$(dirname "$f"):/pcap:ro" -v "$out/$n:/work" -w /work netsec-ml/zeek:1 \
    -C -r "/pcap/$n.pcap" /opt/netsec/offline-json.zeek
  printf "%s %s\n" "$n" "$(cat "$out/$n/s7comm.log" 2>/dev/null | wc -l)"
done
cp /root/1405-06-15/Models/data/raw/attack/labels/s7comm/*.csv "$out/"
tar -C /root -czf /root/s7check.tgz s7check'
```
Expected: one line per capture (`s7 …`, `s701 …`, `s702 …`, `S7COMM …`, `aS700 …` to `aS703 …`) with a record count. The four benign counts must be above 0.

- [ ] **Step 3: Copy the output back and clean server3**

```bash
scp server3:/root/s7check.tgz "$SP/s7check.tgz" && rm -rf "$SP/s7check" && tar -C "$SP" -xzf "$SP/s7check.tgz"
ssh server3 'rm -rf /root/s7check /root/s7check.tgz'
ls "$SP/s7check"
```
Expected: eight capture folders and four `Siemens_*.csv` files.

- [ ] **Step 4: Write the harness `$SP/s7precheck.py`**

```python
#!/usr/bin/env python3
"""Throwaway (plan Task 1): score S7 captures with the delivered Python runtime --
as delivered, and with spec section 2.1's S1+S2 -- and print spec section 11's
report: % NORMAL per capture, split at each connection's 64th event, per group
and per ROSCTR/operation."""
import ast
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import joblib
import numpy as np
import onnxruntime as ort
import pandas as pd

PKG, DELIVERY, CHECK = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
sys.path.insert(0, str(PKG))
from s7zeek.adapters.kafka_source import normalize_zeek_message  # noqa: E402
from s7zeek.features.customer_icsnpp_time_normalized_builder import (  # noqa: E402
    CustomerICSNPPTimeNormalizedFeatureBuilder,
)

ART = DELIVERY / "models/stage1_anomaly/v4_causal_final_r1/artifacts/v4_causal_final_model"
# upstream's operation_groups, conformal_pvalues and GROUP_NAMES, exec'd from
# debiased.py itself -- only those nodes, so its torch import is never run.
tree = ast.parse((DELIVERY / "src/s7zeek/modeling/debiased.py").read_text())
keep = [n for n in tree.body
        if (isinstance(n, ast.FunctionDef) and n.name in ("operation_groups", "conformal_pvalues"))
        or (isinstance(n, ast.Assign) and any(getattr(t, "id", "").startswith("GROUP") for t in n.targets))]
up = {"np": np}
exec(compile(ast.Module(body=keep, type_ignores=[]), "debiased.py", "exec"), up)

pre = joblib.load(ART / "preprocessor.joblib")
session = ort.InferenceSession(str(ART / "s7comm_lstm_autoencoder_debiased.onnx"), providers=["CPUExecutionProvider"])
calibration = np.load(ART / "causal_online_conformal_calibration_scores.npz")
policy = json.loads((ART / "causal_online_shadow_policy.json").read_text())
FEATURES = list(pre.feature_names_in_)
# causal_shadow.py's weights: 0 on the s7_operation columns, 1 elsewhere.
weights = np.array([0.0 if n.startswith("categorical__s7_operation_") else 1.0
                    for n in pre.get_feature_names_out()], np.float32)
pooled = np.concatenate([calibration[g] for g in up["GROUP_NAMES"].values() if len(calibration[g])])


def score_capture(records, amended):
    """Per uid: upstream's builder and a 16-row window, as the online job keys them."""
    builders, windows, counts, out = {}, defaultdict(list), Counter(), []
    for record in records:
        record = dict(record)
        if amended and str(record.get("rosctr_code")) == "7":  # S2
            record["function_code"] = 0
        event = normalize_zeek_message(record)
        builder = builders.setdefault(event.uid, CustomerICSNPPTimeNormalizedFeatureBuilder())
        row = builder.process_event(event)
        counts[event.uid] += 1
        frame = pd.DataFrame([[row[f] for f in FEATURES]], columns=FEATURES)
        if amended:  # S1
            frame["s7_operation"] = frame["s7_operation"].str.upper()
        x = np.asarray(pre.transform(frame), np.float32)[0]
        window = windows[event.uid]
        window.append(x)
        del window[:-16]
        code = event.function_code
        group = up["GROUP_NAMES"][int(up["operation_groups"](
            [row["is_request_direction"]], [int(code == 4)], [int(code == 5)])[0])]
        base = {"n": counts[event.uid], "group": group, "rosctr": row["s7_rosctr"], "operation": row["s7_operation"]}
        if len(window) < 16:
            out.append(dict(base, verdict="WARMUP"))
            continue
        seq = np.stack(window)[None]
        recon = session.run(None, {"input": seq})[0]
        score = float(((((recon[:, -1, :] - seq[:, -1, :]) ** 2) * weights).sum(axis=1) / weights.sum())[0])
        cal = calibration[group]
        alpha = policy["alpha_by_group"].get(group, policy["fallback_alpha"])
        if len(cal) == 0:  # apply_group_conformal_policy's fallback
            cal, alpha = pooled, policy["fallback_alpha"]
        p = float(up["conformal_pvalues"](cal, np.array([score]))[0])
        out.append(dict(base, verdict="ANOMALY" if np.isfinite(p) and p <= alpha else "NORMAL"))
    return out


def pct(rows):
    scored = [r for r in rows if r["verdict"] != "WARMUP"]
    if not scored:
        return "none scored"
    return f"{100.0 * sum(r['verdict'] == 'NORMAL' for r in scored) / len(scored):.2f}% NORMAL of {len(scored)}"


for capture in sorted(p for p in CHECK.iterdir() if p.is_dir()):
    log = capture / "s7comm.log"
    records = [json.loads(line) for line in log.read_text().splitlines() if line.strip()] if log.exists() else []
    print(f"== {capture.name}: {len(records)} records, {len({r.get('uid') for r in records})} connections")
    for amended in (False, True):
        rows = score_capture(records, amended)
        tag = "S1+S2" if amended else "as delivered"
        print(f"  [{tag}] all: {pct(rows)} | past 64th: {pct([r for r in rows if r['n'] > 64])}"
              f" | 16th-64th: {pct([r for r in rows if r['n'] <= 64])}")
        for g in sorted({r["group"] for r in rows}):
            print(f"    group {g}: {pct([r for r in rows if r['group'] == g])}")
        for key in sorted({(r["rosctr"], r["operation"]) for r in rows}):
            print(f"    rosctr {key[0]} {key[1]}: {pct([r for r in rows if (r['rosctr'], r['operation']) == key])}")
```

- [ ] **Step 5: Run it**

Run: `"$SP/venv/bin/python" "$SP/s7precheck.py" "$SP/s7pkg" models/S7 "$SP/s7check" | tee "$SP/s7check/precheck.txt"`
Expected: an `==` block per capture, each with an `[as delivered]` and an `[S1+S2]` line and their group and ROSCTR/operation breakdowns. The benign captures are `s7`, `s701`, `s702` and `S7COMM`. For the attack captures (`aS700`–`aS703`), `% NORMAL` is the miss rate.

- [ ] **Step 6: Record the label format**

Run: `head -3 "$SP"/s7check/Siemens_*.csv`
Expected: the column header of each label file. Write it to the ledger: Task 17 joins labels to records only if a row maps to an S7 record (a timestamp and endpoints).

- [ ] **Step 7: Apply the stop rule (spec §11)**

For each benign capture, read its `[S1+S2] past 64th` figure. **If any is below 99% NORMAL, or any benign capture has no event past its connection's 64th: STOP.** Do not start Task 2. Report `$SP/s7check/precheck.txt` to the owner with:
- both variants;
- the groups and ROSCTR/operation keys that carry the anomalies;
- the probe finding in ruling P1.

The owner decides whether to continue, return to the model team, or change the design. If every benign capture is at or above 99%, write the four figures to the ledger and continue.

---

### Task 2: A USERDATA PDU's function code is read as 0x00 (spec 2.1, S2)

**Files:**
- Modify: `modules/application/src/main/java/io/netsecml/platform/application/usecase/S7commBuildFeaturesUseCase.java` (`build(...)`, plus a new static method)
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/S7commBuildFeaturesUseCaseTest.java`
- Modify: `tests/fixtures/s7comm/generate_upstream_oracle.py`
- Regenerate: `tests/fixtures/s7comm/upstream_oracle_v1.jsonl`
- Modify (comment only): `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/S7commUpstreamOracleTest.java`

**Interfaces:**
- Produces: `static S7commEvent S7commBuildFeaturesUseCase.asTrainingParserReadIt(S7commEvent)`. Every vector on `netsec.s7comm.feature-vector.v1` for a ROSCTR 7 record now carries operation code 0.

- [ ] **Step 1: Write the failing tests**

Append to `S7commBuildFeaturesUseCaseTest` (its `event(ts, request, pdu, rosctr, function, name)` helper already exists):

```java
    // Spec 2.1, S2: a USERDATA (ROSCTR 7) PDU is read with function code 0x00,
    // as the detector's training parser read it, whatever code ICSNPP wrote.
    @Test
    void aUserDataPdusFunctionCodeIsReadAsZero() {
        float[] v = useCase.build(event(1000.0, true, 1, 7, 0x44, "Request: CPU Functions"),
            S7commConnectionState.empty()).vector().values();
        assertEquals(0f, v[15], "s7_operation is code 0x00, which decodes to FUNCTION_0x00");
    }

    // The training parser always had a parameter byte to read, so a user-data
    // record with only a name, or no function at all, is 0x00 too.
    @Test
    void aUserDataPduWithoutACodeIsReadAsZeroToo() {
        float[] named = useCase.build(event(1000.0, true, 1, 7, null, "Request: CPU Functions"),
            S7commConnectionState.empty()).vector().values();
        float[] bare = useCase.build(event(1000.0, true, 1, 7, null, null),
            S7commConnectionState.empty()).vector().values();
        assertEquals(0f, named[15]);
        assertEquals(0f, bare[15]);
    }

    // Zeek's different user-data codes are one repeating function to the engine.
    @Test
    void userDataRequestsWithDifferentZeekCodesAreOneRepeatingFunction() {
        S7commConnectionState state = S7commConnectionState.empty();
        useCase.build(event(1000.0, true, 1, 7, 0x44, null), state);
        float[] second = useCase.build(event(1000.1, true, 2, 7, 0x47, null), state).vector().values();
        assertEquals(0f, second[13], "s7_function_changed: both read as 0x00");
        assertEquals(2f, second[3], "s7_same_function_run_length");
    }

    // Every other ROSCTR keeps the code Zeek wrote.
    @Test
    void aJobPduKeepsItsFunctionCode() {
        float[] v = useCase.build(event(1000.0, true, 1, 1, 0x04, null), S7commConnectionState.empty())
            .vector().values();
        assertEquals(4f, v[15]);
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/application -am -Dtest='S7commBuildFeaturesUseCaseTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 11, Failures: 3`. The failures are `aUserDataPdusFunctionCodeIsReadAsZero` (got 68), `aUserDataPduWithoutACodeIsReadAsZeroToo` (got -2 / -1) and `userDataRequestsWithDifferentZeekCodesAreOneRepeatingFunction`. `aJobPduKeepsItsFunctionCode` already passes; it is the guard that S2 touches only ROSCTR 7.

- [ ] **Step 3: Implement S2**

In `S7commBuildFeaturesUseCase.build(...)`, replace these lines:

```java
        // Apply the event; every feature is read after it, as upstream does.
        S7commConnectionState.Step step = currentState.advance(event.tsSeconds(), event.isRequest(),
            event.pduReference(), event.rosctrCode(), event.functionCode());
        float[] values = extractor.extract(event, currentState, step);
```
with:
```java
        // The engine reads the event as the detector's training parser did (spec 2.1, S2).
        S7commEvent read = asTrainingParserReadIt(event);
        // Apply the event; every feature is read after it, as upstream does.
        S7commConnectionState.Step step = currentState.advance(read.tsSeconds(), read.isRequest(),
            read.pduReference(), read.rosctrCode(), read.functionCode());
        float[] values = extractor.extract(read, currentState, step);
```
Then add to the class, after `build(...)`:
```java
    // ROSCTR 7: an S7 USERDATA PDU.
    private static final int USERDATA = 7;

    // Spec 2.1, S2 (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md):
    // the Stage 1 detector's training table was built by upstream's own pcap
    // parser, which reads a PDU's function code as its parameter's first byte.
    // A USERDATA parameter always starts with the head 00 01 12, so every
    // user-data PDU the detector trained on had function code 0x00; ICSNPP
    // writes the type/group byte instead (0x44, 0x84, ...). The engine reads a
    // ROSCTR 7 event's function code as 0x00 -- its name stays, but a code
    // always wins over a name (S7commCategories). Every other event is unchanged.
    static S7commEvent asTrainingParserReadIt(S7commEvent event) {
        if (event.rosctrCode() == null || event.rosctrCode() != USERDATA
                || Integer.valueOf(0).equals(event.functionCode())) {
            return event;
        }
        return new S7commEvent(event.envelope(), event.tsSeconds(), event.sourceIp(), event.sourcePort(),
            event.destinationIp(), event.destinationPort(), event.pduReference(), event.rosctrCode(), 0,
            event.functionName());
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `Tests run: 11, Failures: 0, Errors: 0`.

- [ ] **Step 5: Watch the parity oracle fail on user-data records**

Run: `./mvnw test -pl modules/adapter-flink -am -Dtest='S7commUpstreamOracleTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL in `everyRecordMatchesUpstreamBitForBit` with `s7_operation` (and some function-history) mismatches, all on ROSCTR 7 records. The fixture still holds Zeek's codes as upstream read them. This is the evidence that the fixture needs S2 too.

- [ ] **Step 6: Apply S2 in the oracle generator**

In `tests/fixtures/s7comm/generate_upstream_oracle.py`:

1. In `main()`'s inner loop, replace:
```python
            raw, upstream = render(shape_rng, conn, ts, is_request, pdu, rosctr, function, name, shape)
            row = builder.process_event(normalize_zeek_message(upstream))
```
with:
```python
            raw, upstream = render(shape_rng, conn, ts, is_request, pdu, rosctr, function, name, shape)
            # Spec 2.1 of docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md,
            # S2: the detector's training parser read every USERDATA (ROSCTR 7)
            # PDU's function code as 0x00, and S7commBuildFeaturesUseCase now reads
            # it the same way, so upstream's builder is handed that code. The raw
            # record, which the Java side parses, stays exactly as Zeek wrote it.
            if rosctr == 7:
                upstream.pop("function", None)
                upstream["function_code"] = 0
            row = builder.process_event(normalize_zeek_message(upstream))
```
2. In the meta dictionary, add after `"features": FEATURES,`:
```python
        "input_amendments": ["S2: a ROSCTR 7 record's function code is handed to upstream as 0x00"],
```
3. In the module docstring, after its first paragraph, add:
```
Upstream is handed each ROSCTR 7 record's function code as 0x00 (the scoring
design's S2), exactly as the Java feature use case reads it.
```

- [ ] **Step 7: Regenerate the fixture, twice, and confirm it is deterministic**

```bash
python3 tests/fixtures/s7comm/generate_upstream_oracle.py --upstream two-models-info/S7___ --out tests/fixtures/s7comm/upstream_oracle_v1.jsonl
sha256sum tests/fixtures/s7comm/upstream_oracle_v1.jsonl
python3 tests/fixtures/s7comm/generate_upstream_oracle.py --upstream two-models-info/S7___ --out tests/fixtures/s7comm/upstream_oracle_v1.jsonl
sha256sum tests/fixtures/s7comm/upstream_oracle_v1.jsonl
git diff --stat tests/fixtures/s7comm/upstream_oracle_v1.jsonl
```
Expected: `wrote 2121 records from 13 streams` both times; the two hashes are identical; the diff touches the meta line and ROSCTR 7 records only.

- [ ] **Step 8: The oracle passes again, and a planted bug breaks it**

Add this sentence to `S7commUpstreamOracleTest`'s class comment, after "…upstream's exact strings":
```java
// The generator hands upstream each ROSCTR 7 record's function code as 0x00
// (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md section
// 2.1, S2), as S7commBuildFeaturesUseCase reads it.
```
Run: the Step 5 command. Expected: `Tests run: 2, Failures: 0`.
Then plant the bug: in `build(...)`, temporarily change `S7commEvent read = asTrainingParserReadIt(event);` to `S7commEvent read = event;`. Run the Step 5 command. Expected: FAIL, with mismatches on ROSCTR 7 records. Restore the line, rerun, and expect `Tests run: 2, Failures: 0`.

- [ ] **Step 9: Nothing else moved**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='S7commBuildFeaturesUseCaseTest,S7commFloodTest,S7commFeatureProcessFunctionTest,S7commUpstreamOracleTest,ZeekRecordCheckTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: every class passes. `ZeekRecordCheckTest` still has 8 tests, 84/84 S7 accepted.

- [ ] **Step 10: Commit**

```bash
git add modules/application/src/main/java/io/netsecml/platform/application/usecase/S7commBuildFeaturesUseCase.java \
  modules/application/src/test/java/io/netsecml/platform/application/usecase/S7commBuildFeaturesUseCaseTest.java \
  modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/S7commUpstreamOracleTest.java \
  tests/fixtures/s7comm/generate_upstream_oracle.py tests/fixtures/s7comm/upstream_oracle_v1.jsonl
git commit -m "fix(s7comm): read a USERDATA PDU's function code as 0x00, as training did

<attribution lines>"
```

---

### Task 3: The frozen preprocessing, in the domain

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/S7commPreprocessing.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/model/S7commPreprocessingTest.java`

**Interfaces:**
- Consumes: `S7commCategories.decodeRosctr(int)`, `decodeOperation(int)`, `MISSING`, `UNSEEN_NAME` (domain.feature).
- Produces: `S7commPreprocessing`:
  - `S7commPreprocessing(List<Continuous>, List<String> binary, double transformedClip, List<String> rosctrCategories, List<String> operationCategories)`;
  - `record Continuous(String name, double median, double center, double scale, double low, double high)` with `bounded()`;
  - `int width()`;
  - `List<String> rawFeatureOrder()`;
  - `List<String> transformedFeatureOrder()`;
  - `float[] scoreWeights(Collection<String> zeroWeightFeatures)`;
  - `float[] apply(float[] raw16)`;
  - constants `RAW_WIDTH` (16), `ROSCTR_FEATURE` ("s7_rosctr"), `OPERATION_FEATURE` ("s7_operation").

- [ ] **Step 1: Write the failing tests**

```java
package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.feature.S7commCategories;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// preprocessor_contract.json's transform on a hand-built contract whose outputs
// are worked out by hand. The real contract's numbers are pinned against the
// delivered Python preprocessor in S7commDetectorBundleLoaderTest.
class S7commPreprocessingTest {

    // c0: unbounded (median 1, center 1, scale 2); c1: bounded to [0, 1]
    // (median 0.5); c2..c11: unbounded identity. The real categories.
    private static S7commPreprocessing contract() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        cs.add(new S7commPreprocessing.Continuous("c0", 1.0, 1.0, 2.0, Double.NaN, Double.NaN));
        cs.add(new S7commPreprocessing.Continuous("c1", 0.5, 0.0, 1.0, 0.0, 1.0));
        for (int i = 2; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("c" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        return new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"),
            List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
    }

    // A raw vector with one value set.
    private static float[] raw(int index, float value) {
        float[] raw = new float[16];
        raw[index] = value;
        return raw;
    }

    @Test
    void anUnboundedFeatureIsRobustScaled() {
        assertEquals(2f, contract().apply(raw(0, 5f))[0], "(5 - 1) / 2");
    }

    @Test
    void anUnboundedFeatureIsClippedToTheTransformedClip() {
        assertEquals(20f, contract().apply(raw(0, 1000f))[0]);
        assertEquals(-20f, contract().apply(raw(2, -50f))[2]);
        assertEquals(20f, contract().apply(raw(2, Float.POSITIVE_INFINITY))[2]);
    }

    @Test
    void aBoundedFeatureIsClippedToItsRangeAndNeverScaled() {
        assertEquals(1f, contract().apply(raw(1, 1.7f))[1]);
        assertEquals(0f, contract().apply(raw(1, -0.2f))[1]);
        assertEquals(0.25f, contract().apply(raw(1, 0.25f))[1]);
    }

    @Test
    void aMissingContinuousValueTakesItsMedian() {
        assertEquals(0f, contract().apply(raw(0, Float.NaN))[0], "(median 1 - 1) / 2");
        assertEquals(0.5f, contract().apply(raw(1, Float.NaN))[1]);
    }

    @Test
    void binaryValuesPassThroughAndAMissingOneIsZero() {
        assertEquals(1f, contract().apply(raw(12, 1f))[12]);
        assertEquals(0f, contract().apply(raw(13, Float.NaN))[13]);
    }

    // Columns 14-16: s7_rosctr 1, 3, 7; anything else is all zeros.
    @Test
    void theRosctrCodeIsOneHot() {
        float[] three = contract().apply(raw(14, 3f));
        assertEquals(List.of(0f, 1f, 0f), List.of(three[14], three[15], three[16]));
        float[] ack = contract().apply(raw(14, 2f));
        assertEquals(List.of(0f, 0f, 0f), List.of(ack[14], ack[15], ack[16]), "ROSCTR 2 was never trained on");
        float[] missing = contract().apply(raw(14, S7commCategories.MISSING));
        assertEquals(List.of(0f, 0f, 0f), List.of(missing[14], missing[15], missing[16]));
    }

    // Columns 17-20: FUNCTION_0X00, READ_VAR, SETUP_COMMUNICATION, WRITE_VAR.
    // Code 0 decodes to FUNCTION_0x00 and matches FUNCTION_0X00 only because the
    // decoded name is upper-cased (spec 2.1, S1).
    @Test
    void theOperationIsOneHotAfterUpperCasing() {
        assertEquals(1f, contract().apply(raw(15, 4f))[18], "READ_VAR");
        assertEquals(1f, contract().apply(raw(15, 0f))[17], "FUNCTION_0x00 -> FUNCTION_0X00");
        for (float code : new float[]{0x29, S7commCategories.UNSEEN_NAME, S7commCategories.MISSING}) {
            float[] out = contract().apply(raw(15, code));
            assertEquals(List.of(0f, 0f, 0f, 0f), List.of(out[17], out[18], out[19], out[20]), "code " + code);
        }
    }

    @Test
    void theWidthAndTheColumnNamesFollowTheContract() {
        S7commPreprocessing p = contract();
        assertEquals(21, p.width());
        List<String> names = p.transformedFeatureOrder();
        assertEquals("continuous__c0", names.get(0));
        assertEquals("binary__b0", names.get(12));
        assertEquals("categorical__s7_rosctr_1", names.get(14));
        assertEquals("categorical__s7_operation_FUNCTION_0X00", names.get(17));
        assertEquals("categorical__s7_operation_WRITE_VAR", names.get(20));
        assertEquals(List.of("s7_rosctr", "s7_operation"), p.rawFeatureOrder().subList(14, 16));
    }

    // upstream's transformed_columns_for_raw_features: every column a listed
    // raw feature produces weighs 0.
    @Test
    void theScoreWeightsZeroEveryColumnOfAListedFeature() {
        float[] weights = contract().scoreWeights(List.of("s7_operation"));
        for (int i = 0; i < 17; i++) {
            assertEquals(1f, weights[i], "column " + i);
        }
        for (int i = 17; i < 21; i++) {
            assertEquals(0f, weights[i], "column " + i);
        }
        assertEquals(0f, contract().scoreWeights(List.of("c1"))[1]);
        assertThrows(IllegalArgumentException.class, () -> contract().scoreWeights(List.of("no_such_feature")));
    }

    @Test
    void theShapeIsChecked() {
        assertThrows(IllegalArgumentException.class, () -> contract().apply(new float[15]));
        List<S7commPreprocessing.Continuous> eleven = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            eleven.add(new S7commPreprocessing.Continuous("c" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commPreprocessing(eleven, List.of("b0", "b1"),
            20.0, List.of("1"), List.of("READ_VAR")));
        assertThrows(IllegalArgumentException.class,
            () -> new S7commPreprocessing.Continuous("c", 0.0, 0.0, 0.0, Double.NaN, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> new S7commPreprocessing.Continuous("c", 0.0, 0.0, 1.0,
            1.0, 0.0));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/domain -am -Dtest='S7commPreprocessingTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commPreprocessing`.

- [ ] **Step 3: Implement**

```java
package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.feature.S7commCategories;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

// The S7comm Stage 1 detector's frozen preprocessing (preprocessor_contract.json;
// docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md section 3.2):
// the 16 raw s7comm-feature-v1 values in, 21 out. Twelve continuous values are
// imputed, then clipped to a physical range or robust-scaled and clipped; two
// binary values are imputed; the two categorical codes are decoded to
// upstream's strings and one-hot encoded, an unknown category as all zeros.
// Computed in double and carried as float32, as upstream does. The output is
// always finite: NaN is imputed and every continuous value is clipped.
public final class S7commPreprocessing {

    // s7comm-feature-v1's layout: 12 continuous, 2 binary, then the two codes.
    public static final int RAW_WIDTH = 16;
    public static final String ROSCTR_FEATURE = "s7_rosctr";
    public static final String OPERATION_FEATURE = "s7_operation";
    private static final int CONTINUOUS_COUNT = 12;
    private static final int BINARY_END = 14;
    private static final int ROSCTR = 14;
    private static final int OPERATION = 15;

    // One continuous feature: the imputer's median, and either a physical range
    // (bounded: clip only) or a robust center and scale (unbounded).
    public record Continuous(String name, double median, double center, double scale, double low, double high) {

        public Continuous {
            Objects.requireNonNull(name, "name");
            // A bounded feature has both ends of its range; an unbounded one neither.
            if (Double.isNaN(low) != Double.isNaN(high)) {
                throw new IllegalArgumentException(name + ": a range needs both ends");
            }
            if (!Double.isNaN(low) && !(low < high)) {
                throw new IllegalArgumentException(name + ": range [" + low + ", " + high + "] is empty");
            }
            if (Double.isNaN(low) && !(scale > 0 && Double.isFinite(scale) && Double.isFinite(center))) {
                throw new IllegalArgumentException(name + ": an unbounded feature needs a finite center and a "
                    + "positive scale");
            }
            if (!Double.isFinite(median)) {
                throw new IllegalArgumentException(name + ": the imputer's median must be finite");
            }
        }

        public boolean bounded() {
            return !Double.isNaN(low);
        }
    }

    private final List<Continuous> continuous;
    private final List<String> binary;
    private final double transformedClip;
    private final List<String> rosctrCategories;
    private final List<String> operationCategories;

    public S7commPreprocessing(List<Continuous> continuous, List<String> binary, double transformedClip,
                               List<String> rosctrCategories, List<String> operationCategories) {
        this.continuous = List.copyOf(continuous);
        this.binary = List.copyOf(binary);
        this.transformedClip = transformedClip;
        this.rosctrCategories = List.copyOf(rosctrCategories);
        this.operationCategories = List.copyOf(operationCategories);
        // The contract's shape is s7comm-feature-v1's: 12 continuous, 2 binary.
        if (this.continuous.size() != CONTINUOUS_COUNT || this.binary.size() != BINARY_END - CONTINUOUS_COUNT) {
            throw new IllegalArgumentException("expected 12 continuous and 2 binary features, got "
                + this.continuous.size() + " and " + this.binary.size());
        }
        if (!(transformedClip > 0 && Double.isFinite(transformedClip))) {
            throw new IllegalArgumentException("transformedClip must be positive and finite, was " + transformedClip);
        }
        // A category listed twice would make its one-hot column ambiguous.
        requireDistinct(this.rosctrCategories, ROSCTR_FEATURE);
        requireDistinct(this.operationCategories, OPERATION_FEATURE);
    }

    private static void requireDistinct(List<String> categories, String feature) {
        if (categories.isEmpty() || new HashSet<>(categories).size() != categories.size()) {
            throw new IllegalArgumentException(feature + " needs distinct categories, got " + categories);
        }
    }

    public int width() {
        return BINARY_END + rosctrCategories.size() + operationCategories.size();
    }

    // The raw features, in the contract's raw_feature_order.
    public List<String> rawFeatureOrder() {
        List<String> names = new ArrayList<>();
        continuous.forEach(c -> names.add(c.name()));
        names.addAll(binary);
        names.add(ROSCTR_FEATURE);
        names.add(OPERATION_FEATURE);
        return List.copyOf(names);
    }

    // The names upstream's ColumnTransformer gives its 21 outputs, in order; the
    // loader checks the contract's transformed_feature_order against them.
    public List<String> transformedFeatureOrder() {
        List<String> names = new ArrayList<>();
        continuous.forEach(c -> names.add("continuous__" + c.name()));
        binary.forEach(b -> names.add("binary__" + b));
        rosctrCategories.forEach(c -> names.add("categorical__" + ROSCTR_FEATURE + "_" + c));
        operationCategories.forEach(c -> names.add("categorical__" + OPERATION_FEATURE + "_" + c));
        return List.copyOf(names);
    }

    // The score's per-column weights: 0 for every column a listed raw feature
    // produces, 1 elsewhere -- upstream's transformed_columns_for_raw_features
    // rule (reliability.py). A name that is not a raw feature is refused.
    public float[] scoreWeights(Collection<String> zeroWeightFeatures) {
        List<String> raw = rawFeatureOrder();
        for (String feature : zeroWeightFeatures) {
            if (!raw.contains(feature)) {
                throw new IllegalArgumentException("not a raw feature of this contract: " + feature);
            }
        }
        List<String> names = transformedFeatureOrder();
        float[] weights = new float[names.size()];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            boolean zero = zeroWeightFeatures.stream().anyMatch(f -> name.equals("continuous__" + f)
                || name.equals("binary__" + f) || name.startsWith("categorical__" + f + "_"));
            weights[i] = zero ? 0f : 1f;
        }
        return weights;
    }

    // The transformed vector, per the contract's continuous, binary and
    // categorical rules.
    public float[] apply(float[] raw) {
        if (raw.length != RAW_WIDTH) {
            throw new IllegalArgumentException("expected " + RAW_WIDTH + " values, got " + raw.length);
        }
        float[] out = new float[width()];
        // Continuous: impute NaN, then clip to the range, or robust-scale and clip.
        for (int i = 0; i < CONTINUOUS_COUNT; i++) {
            Continuous c = continuous.get(i);
            double x = Float.isNaN(raw[i]) ? c.median() : raw[i];
            double y = c.bounded()
                ? clip(x, c.low(), c.high())
                : clip((x - c.center()) / c.scale(), -transformedClip, transformedClip);
            out[i] = (float) y;
        }
        // Binary: NaN is the imputer's 0; anything else passes through.
        for (int i = CONTINUOUS_COUNT; i < BINARY_END; i++) {
            out[i] = Float.isNaN(raw[i]) ? 0f : raw[i];
        }
        // Categorical: one-hot of the decoded string; an unknown one is all zeros.
        oneHot(out, BINARY_END, rosctrCategories, category(raw[ROSCTR], false));
        oneHot(out, BINARY_END + rosctrCategories.size(), operationCategories, category(raw[OPERATION], true));
        return out;
    }

    // upstream's category string for a code, via S7commCategories; an
    // operation is upper-cased (spec 2.1, S1). A code that is not a whole number
    // is no category at all (all zeros).
    private static String category(float code, boolean operation) {
        if (!Float.isFinite(code) || code != Math.rint(code)) {
            return null;
        }
        int c = (int) code;
        return operation
            ? S7commCategories.decodeOperation(c).toUpperCase(Locale.ROOT)
            : S7commCategories.decodeRosctr(c);
    }

    private static void oneHot(float[] out, int at, List<String> categories, String value) {
        for (int j = 0; j < categories.size(); j++) {
            out[at + j] = categories.get(j).equals(value) ? 1f : 0f;
        }
    }

    // np.clip: an infinite input lands on the bound it passed.
    private static double clip(double x, double low, double high) {
        return Math.max(low, Math.min(high, x));
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `Tests run: 10, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/model/S7commPreprocessing.java \
  modules/domain/src/test/java/io/netsecml/platform/domain/model/S7commPreprocessingTest.java
git commit -m "feat(s7comm): the Stage 1 detector's frozen preprocessing

<attribution lines>"
```

---

### Task 4: The score groups and the conformal policy

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/S7commScoreGroup.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/S7commConformalPolicy.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/inference/S7commScoreGroupTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/model/S7commConformalPolicyTest.java`

**Interfaces:**
- Produces:
  - `enum S7commScoreGroup { RESPONSE, READ_REQUEST, WRITE_REQUEST, OTHER_REQUEST; static S7commScoreGroup of(float[] vector16) }`;
  - `S7commConformalPolicy(Map<S7commScoreGroup, double[]> scores, Map<S7commScoreGroup, Double> alphaByGroup, double fallbackAlpha)`, with methods `double alpha(g)`, `int calibrationSize(g)`, `double pValue(g, double score)` and `boolean anomalous(g, double score)`.

- [ ] **Step 1: Write the failing tests**

`S7commScoreGroupTest`:
```java
package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

// upstream's operation_groups, read from the vector: is_request_direction
// (index 12) and the s7_operation code (index 15).
class S7commScoreGroupTest {

    private static float[] vector(boolean request, float operation) {
        float[] v = new float[16];
        v[12] = request ? 1f : 0f;
        v[15] = operation;
        return v;
    }

    @Test
    void aResponseIsResponseWhateverItsFunction() {
        assertEquals(S7commScoreGroup.RESPONSE, S7commScoreGroup.of(vector(false, 4f)));
        assertEquals(S7commScoreGroup.RESPONSE, S7commScoreGroup.of(vector(false, 5f)));
    }

    @Test
    void aRequestIsGroupedByItsFunction() {
        assertEquals(S7commScoreGroup.READ_REQUEST, S7commScoreGroup.of(vector(true, 4f)));
        assertEquals(S7commScoreGroup.WRITE_REQUEST, S7commScoreGroup.of(vector(true, 5f)));
        assertEquals(S7commScoreGroup.OTHER_REQUEST, S7commScoreGroup.of(vector(true, 0xF0)));
        assertEquals(S7commScoreGroup.OTHER_REQUEST, S7commScoreGroup.of(vector(true, 0f)), "user data (S2)");
        assertEquals(S7commScoreGroup.OTHER_REQUEST, S7commScoreGroup.of(vector(true, -1f)), "no function");
    }
}
```

`S7commConformalPolicyTest`:
```java
package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.IntStream;

import static io.netsecml.platform.domain.inference.S7commScoreGroup.OTHER_REQUEST;
import static io.netsecml.platform.domain.inference.S7commScoreGroup.READ_REQUEST;
import static io.netsecml.platform.domain.inference.S7commScoreGroup.RESPONSE;
import static io.netsecml.platform.domain.inference.S7commScoreGroup.WRITE_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// upstream's conformal_pvalues and apply_group_conformal_policy
// (debiased.py, causal_shadow.py) on a hand-built policy.
class S7commConformalPolicyTest {

    // RESPONSE: four scores (unsorted on purpose), alpha 0.2. READ_REQUEST: one
    // score, no alpha of its own. WRITE_REQUEST: 0.01..0.63, alpha 1/64.
    // OTHER_REQUEST: no scores, a configured alpha that must not apply.
    private static S7commConformalPolicy policy() {
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        scores.put(RESPONSE, new double[]{0.4, 0.1, 0.3, 0.2});
        scores.put(READ_REQUEST, new double[]{0.05});
        scores.put(WRITE_REQUEST, IntStream.rangeClosed(1, 63).mapToDouble(i -> i / 100.0).toArray());
        scores.put(OTHER_REQUEST, new double[0]);
        Map<S7commScoreGroup, Double> alphas = new EnumMap<>(S7commScoreGroup.class);
        alphas.put(RESPONSE, 0.2);
        alphas.put(WRITE_REQUEST, 0.015625);
        alphas.put(OTHER_REQUEST, 0.01);
        return new S7commConformalPolicy(scores, alphas, 0.001);
    }

    @Test
    void thePValueCountsTheCalibrationScoresAtLeastTheScore() {
        S7commConformalPolicy p = policy();
        assertEquals(0.4, p.pValue(RESPONSE, 0.35), 1e-12, "0.4 only: (1 + 1) / 5");
        assertEquals(0.6, p.pValue(RESPONSE, 0.3), 1e-12, "0.3 counts itself: (2 + 1) / 5");
        assertEquals(0.2, p.pValue(RESPONSE, 0.5), 1e-12, "none: 1 / 5");
        assertEquals(1.0, p.pValue(RESPONSE, 0.0), 1e-12, "all four: 5 / 5");
    }

    @Test
    void anAnomalyIsAPValueAtMostTheAlpha() {
        assertTrue(policy().anomalous(RESPONSE, 0.5), "p 0.2 <= 0.2");
        assertFalse(policy().anomalous(RESPONSE, 0.4), "p 0.4");
    }

    // 63 calibration scores: 1/64 is the smallest p-value there is, so a write
    // is an anomaly only above every one of them.
    @Test
    void aWriteIsAnAnomalyOnlyAboveEveryCalibrationScore() {
        assertTrue(policy().anomalous(WRITE_REQUEST, 0.64));
        assertEquals(1.0 / 64, policy().pValue(WRITE_REQUEST, 0.64), 1e-12);
        assertFalse(policy().anomalous(WRITE_REQUEST, 0.63), "equal to the maximum: p = 2/64");
    }

    @Test
    void aGroupWithoutAnAlphaUsesTheFallback() {
        assertEquals(0.001, policy().alpha(READ_REQUEST));
    }

    // apply_group_conformal_policy: a group with no calibration scores is judged
    // against every group's scores pooled, with the fallback alpha -- its own
    // configured alpha never applies.
    @Test
    void aGroupWithoutCalibrationUsesEveryScorePooledAndTheFallbackAlpha() {
        assertEquals(4 + 1 + 63, policy().calibrationSize(OTHER_REQUEST));
        assertEquals(0.001, policy().alpha(OTHER_REQUEST));
        assertEquals(1.0 / 69, policy().pValue(OTHER_REQUEST, 1.0), 1e-12);
    }

    @Test
    void aNonFiniteScoreIsNeverAnAnomaly() {
        assertTrue(Double.isNaN(policy().pValue(RESPONSE, Double.NaN)));
        assertFalse(policy().anomalous(RESPONSE, Double.NaN));
    }

    @Test
    void anUnusablePolicyIsRefused() {
        Map<S7commScoreGroup, double[]> missing = new EnumMap<>(S7commScoreGroup.class);
        missing.put(RESPONSE, new double[]{0.1});
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(missing, Map.of(), 0.001),
            "every group must be given, even empty");
        Map<S7commScoreGroup, double[]> nan = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            nan.put(g, new double[]{Double.NaN});
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(nan, Map.of(), 0.001));
        Map<S7commScoreGroup, double[]> none = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            none.put(g, new double[0]);
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(none, Map.of(), 0.001));
        Map<S7commScoreGroup, double[]> fine = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            fine.put(g, new double[]{0.1});
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(fine, Map.of(), 0.0));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/domain -am -Dtest='S7commScoreGroupTest,S7commConformalPolicyTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commScoreGroup`.

- [ ] **Step 3: Implement**

`S7commScoreGroup`:
```java
package io.netsecml.platform.domain.inference;

// The S7comm Stage 1 detector's calibration groups (upstream's operation_groups,
// debiased.py): a response, or a request by its function -- READ_VAR (4),
// WRITE_VAR (5) or anything else. Read from the vector: is_request_direction
// (index 12) and the s7_operation code (index 15), which equals the record's
// function code whenever it carried one (scoring design section 9, D5).
public enum S7commScoreGroup {
    RESPONSE, READ_REQUEST, WRITE_REQUEST, OTHER_REQUEST;

    private static final int IS_REQUEST = 12;
    private static final int OPERATION = 15;

    public static S7commScoreGroup of(float[] vector) {
        // upstream: is_request_direction.astype(int64) == 1.
        if (vector[IS_REQUEST] != 1f) {
            return RESPONSE;
        }
        float operation = vector[OPERATION];
        if (operation == 4f) {
            return READ_REQUEST;
        }
        if (operation == 5f) {
            return WRITE_REQUEST;
        }
        return OTHER_REQUEST;
    }
}
```

`S7commConformalPolicy`:
```java
package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.inference.S7commScoreGroup;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.DoubleStream;

// The S7comm Stage 1 detector's decision (scoring design section 3.4):
// upstream's one-sided conformal p-value against each group's normal-validation
// scores, p = (#{calibration >= score} + 1) / (n + 1), and ANOMALY iff
// p <= that group's alpha (conformal_pvalues, apply_group_conformal_policy).
// A group with no scores is judged against every group's scores pooled, with
// the fallback alpha. Immutable; built once per subtask.
public final class S7commConformalPolicy {

    private final EnumMap<S7commScoreGroup, double[]> calibration = new EnumMap<>(S7commScoreGroup.class);
    private final EnumMap<S7commScoreGroup, Double> alpha = new EnumMap<>(S7commScoreGroup.class);

    public S7commConformalPolicy(Map<S7commScoreGroup, double[]> scores, Map<S7commScoreGroup, Double> alphaByGroup,
                                 double fallbackAlpha) {
        requireAlpha(fallbackAlpha, "fallback");
        // Every group must be given, possibly empty, and every score finite.
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            double[] s = scores.get(g);
            if (s == null) {
                throw new IllegalArgumentException("no calibration scores given for " + g);
            }
            if (DoubleStream.of(s).anyMatch(v -> !Double.isFinite(v))) {
                throw new IllegalArgumentException(g + "'s calibration scores must all be finite");
            }
        }
        // The fallback reference: every non-empty group's scores, together.
        double[] pooled = Arrays.stream(S7commScoreGroup.values()).flatMapToDouble(g -> DoubleStream.of(scores.get(g)))
            .sorted().toArray();
        if (pooled.length == 0) {
            throw new IllegalArgumentException("a policy needs calibration scores");
        }
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            double[] own = scores.get(g).clone();
            Arrays.sort(own);
            if (own.length == 0) {
                calibration.put(g, pooled);
                alpha.put(g, fallbackAlpha);
            } else {
                calibration.put(g, own);
                double a = alphaByGroup.getOrDefault(g, fallbackAlpha);
                requireAlpha(a, g.name());
                alpha.put(g, a);
            }
        }
    }

    private static void requireAlpha(double a, String name) {
        if (!(a > 0 && a <= 1)) {
            throw new IllegalArgumentException(name + "'s alpha must be in (0, 1], was " + a);
        }
    }

    // The alpha this group's decision uses: its own, or the fallback when it
    // had no alpha or no calibration scores.
    public double alpha(S7commScoreGroup group) {
        return alpha.get(group);
    }

    // How many calibration scores judge this group (the pooled count for a
    // group that had none).
    public int calibrationSize(S7commScoreGroup group) {
        return calibration.get(group).length;
    }

    // conformal_pvalues: NaN for a non-finite score.
    public double pValue(S7commScoreGroup group, double score) {
        if (!Double.isFinite(score)) {
            return Double.NaN;
        }
        double[] ordered = calibration.get(group);
        int atLeast = ordered.length - firstAtLeast(ordered, score);
        return (atLeast + 1.0) / (ordered.length + 1.0);
    }

    // apply_group_conformal_policy: isfinite(p) & (p <= alpha).
    public boolean anomalous(S7commScoreGroup group, double score) {
        double p = pValue(group, score);
        return !Double.isNaN(p) && p <= alpha(group);
    }

    // np.searchsorted(side="left"): the first index whose value is >= key.
    private static int firstAtLeast(double[] ordered, double key) {
        int low = 0;
        int high = ordered.length;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (ordered[mid] < key) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `S7commScoreGroupTest` 2, `S7commConformalPolicyTest` 7; `Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/inference/S7commScoreGroup.java \
  modules/domain/src/main/java/io/netsecml/platform/domain/model/S7commConformalPolicy.java \
  modules/domain/src/test/java/io/netsecml/platform/domain/inference/S7commScoreGroupTest.java \
  modules/domain/src/test/java/io/netsecml/platform/domain/model/S7commConformalPolicyTest.java
git commit -m "feat(s7comm): the Stage 1 score groups and conformal decision

<attribution lines>"
```

---
### Task 5: The bundle, the window and the prediction

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/S7commDetectorBundle.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/S7commScoreWindow.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/S7commDetectorPrediction.java`
- Modify (comment only): `modules/domain/src/main/java/io/netsecml/platform/domain/inference/DetectorVerdict.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/model/S7commDetectorBundleTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/inference/S7commScoreWindowTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/inference/S7commDetectorPredictionTest.java`

**Interfaces:**
- Consumes: `S7commPreprocessing`, `S7commConformalPolicy`, `S7commScoreGroup` (Tasks 3-4); `DetectorVerdict`, `SensorId`.
- Produces:
  - `record S7commDetectorBundle(String name, String version, String modelSha, String schemaId, String schemaHash, int sequenceLength, int featureCount, String inputName, String outputName, S7commPreprocessing preprocessing, float[] scoreWeights, S7commConformalPolicy policy)` with `bundleId()`;
  - `final class S7commScoreWindow`, with `static empty(String bundleId, int capacity, int width)`, `bundleId()`, `size()`, `isFull()`, `reset()`, `long countEvent()`, `long eventsSinceReset()`, `append(float[] row, int flags)`, `float[][] sequence()` and `int flagsOr()`;
  - `record S7commDetectorPrediction(String predictionId, String eventId, Instant eventTime, SensorId sensor, String connectionUid, String clientIp, String serverIp, String modelName, String modelVersion, String modelSha, String schemaId, String schemaHash, DetectorVerdict verdict, Float score, Double pValue, S7commScoreGroup scoreGroup, double alpha, long eventsSinceReset, int qualityFlags, long inferenceMicros, Instant producedAt)`.

- [ ] **Step 1: Write the failing tests**

`S7commDetectorBundleTest`:
```java
package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// A bundle's parts must agree: the graph's width is the preprocessing's, and
// the score has one non-negative weight per column, not all zero.
class S7commDetectorBundleTest {

    private static S7commPreprocessing preprocessing() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("c" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        return new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"),
            List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
    }

    private static S7commConformalPolicy policy() {
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            scores.put(g, new double[]{0.1});
        }
        return new S7commConformalPolicy(scores, Map.of(), 0.001);
    }

    private static float[] filled(int n, float value) {
        float[] w = new float[n];
        Arrays.fill(w, value);
        return w;
    }

    private static S7commDetectorBundle bundle(int sequenceLength, int featureCount, float[] weights) {
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1",
            "c".repeat(64), sequenceLength, featureCount, "input", "reconstruction", preprocessing(), weights,
            policy());
    }

    @Test
    void theBundleIdIsNameSlashVersion() {
        assertEquals("s7comm-stage1-detector/v1", bundle(16, 21, filled(21, 1f)).bundleId());
    }

    @Test
    void theWeightsAreCopiedInAndOut() {
        float[] weights = filled(21, 1f);
        S7commDetectorBundle b = bundle(16, 21, weights);
        weights[0] = 5f;
        assertEquals(1f, b.scoreWeights()[0]);
        b.scoreWeights()[1] = 5f;
        assertEquals(1f, b.scoreWeights()[1]);
    }

    @Test
    void thePartsMustAgree() {
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 20, filled(20, 1f)), "21 columns");
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 21, filled(20, 1f)), "one weight per column");
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 21, filled(21, 0f)), "not all zero");
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 21, filled(21, -1f)), "not negative");
        assertThrows(IllegalArgumentException.class, () -> bundle(0, 21, filled(21, 1f)), "a window of events");
    }
}
```

`S7commScoreWindowTest`:
```java
package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// A connection's last rows, oldest first, their flags, and its event count
// since the last reset.
class S7commScoreWindowTest {

    @Test
    void itKeepsTheLastCapacityRowsOldestFirst() {
        S7commScoreWindow w = S7commScoreWindow.empty("b/v1", 3, 2);
        for (int i = 0; i < 5; i++) {
            w.append(new float[]{i, -i}, 0);
        }
        assertTrue(w.isFull());
        assertEquals(3, w.size());
        float[][] rows = w.sequence();
        assertEquals(2f, rows[0][0]);
        assertEquals(4f, rows[2][0]);
    }

    @Test
    void itCountsEventsSinceItsLastReset() {
        S7commScoreWindow w = S7commScoreWindow.empty("b/v1", 3, 2);
        assertEquals(1, w.countEvent());
        assertEquals(2, w.countEvent());
        w.append(new float[2], 8);
        w.reset();
        assertEquals(0, w.size());
        assertEquals(0, w.eventsSinceReset());
        assertEquals(0, w.flagsOr());
        assertEquals(1, w.countEvent());
    }

    @Test
    void itOrsTheFlagsOfTheRowsItHolds() {
        S7commScoreWindow w = S7commScoreWindow.empty("b/v1", 2, 1);
        w.append(new float[1], 16);
        w.append(new float[1], 0);
        assertEquals(16, w.flagsOr());
        w.append(new float[1], 0);
        assertEquals(0, w.flagsOr(), "the flagged row left");
    }

    @Test
    void aRowOfTheWrongWidthIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> S7commScoreWindow.empty("b/v1", 2, 3).append(new float[2], 0));
    }
}
```

`S7commDetectorPredictionTest`:
```java
package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

// A score and a p-value exist exactly when the detector judged the event.
class S7commDetectorPredictionTest {

    private static S7commDetectorPrediction prediction(DetectorVerdict verdict, Float score, Double p, double alpha,
                                                       long events) {
        return new S7commDetectorPrediction("a".repeat(64), "s:C1:1:REQUEST:1", Instant.EPOCH, new SensorId("s"),
            "C1", "10.0.0.5", "10.0.0.9", "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1",
            "c".repeat(64), verdict, score, p, S7commScoreGroup.READ_REQUEST, alpha, events, 0, 0L, Instant.EPOCH);
    }

    @Test
    void judgedVerdictsCarryAScoreAndAPValue() {
        assertDoesNotThrow(() -> prediction(DetectorVerdict.NORMAL, 0.1f, 0.5, 0.001, 16));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.ANOMALY, null, 0.5, 0.001, 16));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.ANOMALY, 0.1f, null, 0.001, 16));
    }

    @Test
    void otherVerdictsCarryNeither() {
        assertDoesNotThrow(() -> prediction(DetectorVerdict.WARMUP, null, null, 0.001, 3));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.UNSCORABLE, 0.1f, null, 0.001, 16));
    }

    @Test
    void theEventCountStartsAtOneAndAlphaIsAProbability() {
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.WARMUP, null, null, 0.001, 0));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.WARMUP, null, null, 0.0, 1));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/domain -am -Dtest='S7commDetectorBundleTest,S7commScoreWindowTest,S7commDetectorPredictionTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commDetectorBundle`.

- [ ] **Step 3: Implement**

`S7commDetectorBundle`:
```java
package io.netsecml.platform.domain.model;

import java.util.Arrays;
import java.util.Objects;

// A loaded, verified S7comm Stage 1 detector: its identity, the feature schema
// it was trained on, its graph's input and output names, its frozen
// preprocessing, the score's per-column weights and its conformal policy.
// Built only by the bundle loader, after every SHA-256 and every contract
// check has passed.
public record S7commDetectorBundle(String name, String version, String modelSha, String schemaId,
                                   String schemaHash, int sequenceLength, int featureCount, String inputName,
                                   String outputName, S7commPreprocessing preprocessing, float[] scoreWeights,
                                   S7commConformalPolicy policy) {

    public S7commDetectorBundle {
        // Identity and graph names are looked up by string; none may be missing.
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(modelSha, "modelSha");
        Objects.requireNonNull(schemaId, "schemaId");
        Objects.requireNonNull(schemaHash, "schemaHash");
        Objects.requireNonNull(inputName, "inputName");
        Objects.requireNonNull(outputName, "outputName");
        Objects.requireNonNull(preprocessing, "preprocessing");
        Objects.requireNonNull(policy, "policy");
        if (sequenceLength < 1) {
            throw new IllegalArgumentException("sequenceLength must be at least 1, was " + sequenceLength);
        }
        // The preprocessing produces exactly the vector the graph reads.
        if (featureCount != preprocessing.width()) {
            throw new IllegalArgumentException("featureCount " + featureCount + " but preprocessing produces "
                + preprocessing.width());
        }
        // One finite, non-negative weight per column, not all zero (debiased.py's checks).
        scoreWeights = Arrays.copyOf(Objects.requireNonNull(scoreWeights, "scoreWeights"), scoreWeights.length);
        if (scoreWeights.length != featureCount) {
            throw new IllegalArgumentException(scoreWeights.length + " weights for " + featureCount + " columns");
        }
        float sum = 0f;
        for (float w : scoreWeights) {
            if (!(w >= 0 && Float.isFinite(w))) {
                throw new IllegalArgumentException("weights must be finite and >= 0, found " + w);
            }
            sum += w;
        }
        if (sum <= 0f) {
            throw new IllegalArgumentException("at least one weight must be > 0");
        }
    }

    @Override
    public float[] scoreWeights() {
        return Arrays.copyOf(scoreWeights, scoreWeights.length);
    }

    // How a window records which bundle filled it: name/version.
    public String bundleId() {
        return name + "/" + version;
    }
}
```

`S7commScoreWindow`:
```java
package io.netsecml.platform.domain.inference;

import java.util.Arrays;

// One S7 connection's last `capacity` preprocessed vectors, a ring, with each
// row's quality flags, the bundle that filled it, and the connection's event
// count since the window last reset (scoring design section 5). Mutable and
// updated in place, like ModbusScoreWindow: keyed Flink state, read through
// value() on every call, never cached. Its own class, not ModbusScoreWindow's,
// because that one is live Flink state whose layout must not change.
public final class S7commScoreWindow {

    private final String bundleId;
    private final int capacity;
    private final int width;
    private final float[][] rows;
    private final int[] flags;
    // Index of the oldest row, how many rows are held, and events since reset.
    private int start;
    private int size;
    private long eventsSinceReset;

    private S7commScoreWindow(String bundleId, int capacity, int width) {
        this.bundleId = bundleId;
        this.capacity = capacity;
        this.width = width;
        this.rows = new float[capacity][width];
        this.flags = new int[capacity];
    }

    public static S7commScoreWindow empty(String bundleId, int capacity, int width) {
        if (bundleId == null || capacity < 1 || width < 1) {
            throw new IllegalArgumentException("a window needs a bundle id, a capacity and a width");
        }
        return new S7commScoreWindow(bundleId, capacity, width);
    }

    public String bundleId() {
        return bundleId;
    }

    public int size() {
        return size;
    }

    public boolean isFull() {
        return size == capacity;
    }

    // The connection's history starts again: no row, no flag, no event.
    public void reset() {
        start = 0;
        size = 0;
        Arrays.fill(flags, 0);
        eventsSinceReset = 0;
    }

    // One more event since the last reset; returns the count, 1-based.
    public long countEvent() {
        return ++eventsSinceReset;
    }

    public long eventsSinceReset() {
        return eventsSinceReset;
    }

    // Appends a row; once full, the oldest row leaves.
    public void append(float[] row, int qualityFlags) {
        if (row.length != width) {
            throw new IllegalArgumentException("expected a row of " + width + " values, got " + row.length);
        }
        int slot;
        if (size < capacity) {
            slot = (start + size) % capacity;
            size++;
        } else {
            slot = start;
            start = (start + 1) % capacity;
        }
        System.arraycopy(row, 0, rows[slot], 0, width);
        flags[slot] = qualityFlags;
    }

    // The held rows, oldest first, as a fresh array.
    public float[][] sequence() {
        float[][] out = new float[size][];
        for (int i = 0; i < size; i++) {
            out[i] = Arrays.copyOf(rows[(start + i) % capacity], width);
        }
        return out;
    }

    // The OR of the held rows' quality flags.
    public int flagsOr() {
        int or = 0;
        for (int i = 0; i < size; i++) {
            or |= flags[(start + i) % capacity];
        }
        return or;
    }
}
```

`S7commDetectorPrediction`:
```java
package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;

import java.time.Instant;
import java.util.Objects;

// One S7comm event's prediction (contracts/stream/s7comm-detector-prediction-v1.json):
// which connection, which model, the verdict, the group and alpha that judged
// it, and -- when the detector judged it -- its score and p-value.
public record S7commDetectorPrediction(String predictionId, String eventId, Instant eventTime, SensorId sensor,
                                       String connectionUid, String clientIp, String serverIp, String modelName,
                                       String modelVersion, String modelSha, String schemaId, String schemaHash,
                                       DetectorVerdict verdict, Float score, Double pValue,
                                       S7commScoreGroup scoreGroup, double alpha, long eventsSinceReset,
                                       int qualityFlags, long inferenceMicros, Instant producedAt) {

    public S7commDetectorPrediction {
        // The join keys and the model identity are required on every prediction.
        Objects.requireNonNull(predictionId, "predictionId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(sensor, "sensor");
        Objects.requireNonNull(verdict, "verdict");
        Objects.requireNonNull(scoreGroup, "scoreGroup");
        Objects.requireNonNull(producedAt, "producedAt");
        // A score and a p-value exist exactly when the detector judged the event.
        boolean judged = score != null && pValue != null;
        boolean unjudged = score == null && pValue == null;
        if (verdict.scored() ? !judged : !unjudged) {
            throw new IllegalArgumentException(verdict + " must " + (verdict.scored() ? "" : "not ")
                + "carry a score and a p-value");
        }
        // The count includes this event; alpha is a probability.
        if (eventsSinceReset < 1) {
            throw new IllegalArgumentException("eventsSinceReset counts this event, so is at least 1");
        }
        if (!(alpha > 0 && alpha <= 1)) {
            throw new IllegalArgumentException("alpha must be in (0, 1], was " + alpha);
        }
    }
}
```

In `DetectorVerdict`, replace its comment (the enum is unchanged):
```java
// One scored event's outcome, for either detector. WARMUP: fewer than a full
// window in the stream's (Modbus) or connection's (S7comm) history.
// UNSCORABLE: the detector could not judge it -- non-finite preprocessed input
// (Modbus) or a non-finite score (S7comm). NORMAL and ANOMALY: the detector
// ran, so its scores exist.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `S7commDetectorBundleTest` 3, `S7commScoreWindowTest` 4, `S7commDetectorPredictionTest` 3; `Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/model/S7commDetectorBundle.java \
  modules/domain/src/main/java/io/netsecml/platform/domain/inference/S7commScoreWindow.java \
  modules/domain/src/main/java/io/netsecml/platform/domain/inference/S7commDetectorPrediction.java \
  modules/domain/src/main/java/io/netsecml/platform/domain/inference/DetectorVerdict.java \
  modules/domain/src/test/java/io/netsecml/platform/domain/model/S7commDetectorBundleTest.java \
  modules/domain/src/test/java/io/netsecml/platform/domain/inference/S7commScoreWindowTest.java \
  modules/domain/src/test/java/io/netsecml/platform/domain/inference/S7commDetectorPredictionTest.java
git commit -m "feat(s7comm): the detector bundle, score window and prediction types

<attribution lines>"
```

---

### Task 6: The scorer port and `ScoreS7commSequenceUseCase`

**Files:**
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/out/ReconstructionScorer.java`
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/out/ReconstructionScorerFactory.java`
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/ScoreS7commSequenceUseCase.java`
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/S7commScoringResult.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/ScoreS7commSequenceUseCaseTest.java`

**Interfaces:**
- Consumes: Tasks 3-5; `FeatureVector`, `Prediction.deriveId`, `S7commFeatureSchemaV1`.
- Produces:
  - `interface ReconstructionScorer extends AutoCloseable { S7commDetectorBundle bundle(); float[] reconstructLast(float[][] window); void close(); }`;
  - `interface ReconstructionScorerFactory extends Serializable { ReconstructionScorer create(); }`;
  - `ScoreS7commSequenceUseCase(ReconstructionScorer, Clock)`, with `S7commScoringResult score(FeatureVector vector, boolean freshState, String clientIp, String serverIp, S7commScoreWindow window)`;
  - `record S7commScoringResult(S7commDetectorPrediction prediction, S7commScoreWindow window)`.

- [ ] **Step 1: Write the failing tests**

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;
import io.netsecml.platform.port.out.ReconstructionScorer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The use case with a stub detector: the warm-up, resets, the weighted
// last-event score, the group-conditional decision, the flags and the
// bundle-id rule (scoring design sections 3-5).
class ScoreS7commSequenceUseCaseTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);

    // Identity continuous transforms, so a raw continuous value is its preprocessed value.
    private static S7commPreprocessing identity() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("s" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        return new S7commPreprocessing(cs, List.of("is_request_direction", "s7_function_changed"), 20.0,
            List.of("1", "3", "7"), List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
    }

    // RESPONSE: four scores, alpha 0.2. READ_REQUEST: one score, no alpha of its
    // own (the fallback). WRITE_REQUEST: one score, alpha 0.5. OTHER_REQUEST: no
    // scores (pooled, fallback alpha).
    private static S7commConformalPolicy policy() {
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        scores.put(S7commScoreGroup.RESPONSE, new double[]{0.1, 0.2, 0.3, 0.4});
        scores.put(S7commScoreGroup.READ_REQUEST, new double[]{0.05});
        scores.put(S7commScoreGroup.WRITE_REQUEST, new double[]{0.5});
        scores.put(S7commScoreGroup.OTHER_REQUEST, new double[0]);
        return new S7commConformalPolicy(scores,
            Map.of(S7commScoreGroup.RESPONSE, 0.2, S7commScoreGroup.WRITE_REQUEST, 0.5), 0.001);
    }

    private static S7commDetectorBundle bundle(String schemaId) {
        S7commPreprocessing p = identity();
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64), schemaId,
            S7commFeatureSchemaV1.CONTENT_HASH, 16, 21, "input", "reconstruction", p,
            p.scoreWeights(List.of("s7_operation")), policy());
    }

    // A stub detector: it reconstructs the last row with `offset` added to
    // column 0 (weight 1) and 100 added to column 20 (an s7_operation column,
    // weight 0), so the score is offset^2 / 17 whatever column 20's error.
    private static final class StubScorer implements ReconstructionScorer {
        private final S7commDetectorBundle bundle;
        float offset = 0f;
        int calls = 0;

        StubScorer(S7commDetectorBundle bundle) {
            this.bundle = bundle;
        }

        @Override
        public S7commDetectorBundle bundle() {
            return bundle;
        }

        @Override
        public float[] reconstructLast(float[][] window) {
            calls++;
            float[] row = window[window.length - 1].clone();
            row[0] += offset;
            row[20] += 100f;
            return row;
        }

        @Override
        public void close() {
        }
    }

    private final StubScorer scorer = new StubScorer(bundle(S7commFeatureSchemaV1.SCHEMA.id()));
    private final ScoreS7commSequenceUseCase useCase = new ScoreS7commSequenceUseCase(scorer, CLOCK);

    // One s7comm-feature-v1 vector: a request with this operation code, or a
    // response; feature 0 is i / 100.
    private static FeatureVector vector(int i, boolean request, int operation, int flags) {
        float[] v = new float[16];
        v[0] = i / 100f;
        v[12] = request ? 1f : 0f;
        v[14] = request ? 1f : 3f;
        v[15] = operation;
        return new FeatureVector("s:C1:" + i + ":" + (request ? "REQUEST" : "RESPONSE") + ":" + i,
            Instant.ofEpochSecond(1000 + i), new SensorId("s"), LogType.S7COMM, "C1",
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, v, flags,
            Instant.ofEpochSecond(1000 + i));
    }

    // `n` response events on one connection, the first fresh; the window after them.
    private S7commScoreWindow feed(int n) {
        S7commScoreWindow window = null;
        for (int i = 0; i < n; i++) {
            window = useCase.score(vector(i, false, 4, 0), i == 0, "10.0.0.5", "10.0.0.9", window).window();
        }
        return window;
    }

    @Test
    void theFirstFifteenEventsWarmUpAndTheSixteenthIsScored() {
        S7commScoreWindow window = null;
        List<S7commDetectorPrediction> out = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            S7commScoringResult r = useCase.score(vector(i, false, 4, 0), i == 0, "10.0.0.5", "10.0.0.9", window);
            window = r.window();
            out.add(r.prediction());
        }
        for (int i = 0; i < 15; i++) {
            assertEquals(DetectorVerdict.WARMUP, out.get(i).verdict(), "event " + i);
            assertNull(out.get(i).score());
            assertEquals(i + 1, out.get(i).eventsSinceReset());
        }
        assertEquals(DetectorVerdict.NORMAL, out.get(15).verdict());
        assertEquals(0f, out.get(15).score());
        assertEquals(1.0, out.get(15).pValue(), "every calibration score is >= 0: p = 5/5");
        assertEquals(16, out.get(15).eventsSinceReset());
        assertEquals(1, scorer.calls, "the detector runs only on a full window");
    }

    // Spec section 5 (D4): the feature engine started the connection again, so
    // the window and its count start again too.
    @Test
    void freshStateStartsTheWindowAndTheCountAgain() {
        S7commScoreWindow window = feed(16);
        S7commScoringResult again = useCase.score(vector(16, false, 4, 0), true, "a", "b", window);
        assertEquals(DetectorVerdict.WARMUP, again.prediction().verdict());
        assertEquals(1, again.prediction().eventsSinceReset());
        S7commScoringResult next = useCase.score(vector(17, false, 4, 0), false, "a", "b", again.window());
        assertEquals(2, next.prediction().eventsSinceReset());
    }

    // causal_shadow.py: only the last event, weighted; column 20 weighs 0.
    @Test
    void theScoreIsTheWeightedErrorOfTheLastEventOnly() {
        S7commScoreWindow window = feed(15);
        scorer.offset = 3f;
        S7commDetectorPrediction p = useCase.score(vector(15, false, 4, 0), false, "a", "b", window).prediction();
        assertEquals(9f / 17f, p.score(), 1e-6f, "offset^2 / 17");
    }

    @Test
    void anAnomalyIsAPValueAtMostItsGroupsAlpha() {
        S7commScoreWindow window = feed(15);
        scorer.offset = 3f;
        S7commDetectorPrediction p = useCase.score(vector(15, false, 4, 0), false, "a", "b", window).prediction();
        assertEquals(S7commScoreGroup.RESPONSE, p.scoreGroup());
        assertEquals(0.2, p.pValue(), 1e-12, "0.53 is above all four RESPONSE scores: 1/5");
        assertEquals(0.2, p.alpha());
        assertEquals(DetectorVerdict.ANOMALY, p.verdict());
    }

    @Test
    void theGroupAndItsAlphaFollowTheVector() {
        assertGroup(true, 4, S7commScoreGroup.READ_REQUEST, 0.001);
        assertGroup(true, 5, S7commScoreGroup.WRITE_REQUEST, 0.5);
        assertGroup(true, 0, S7commScoreGroup.OTHER_REQUEST, 0.001);
        assertGroup(false, 5, S7commScoreGroup.RESPONSE, 0.2);
    }

    private void assertGroup(boolean request, int operation, S7commScoreGroup group, double alpha) {
        S7commDetectorPrediction p = useCase.score(vector(0, request, operation, 0), true, "a", "b", null)
            .prediction();
        assertEquals(group, p.scoreGroup());
        assertEquals(alpha, p.alpha());
    }

    @Test
    void qualityFlagsAreOrdOverTheWindow() {
        S7commScoreWindow window = null;
        S7commDetectorPrediction p = null;
        for (int i = 0; i < 16; i++) {
            S7commScoringResult r = useCase.score(vector(i, false, 4, i == 3 ? 16 : 0), i == 0, "a", "b", window);
            window = r.window();
            p = r.prediction();
        }
        assertEquals(16, p.qualityFlags(), "event 3's S7COMM_OUT_OF_ORDER is still in the window");
        for (int i = 16; i < 20; i++) {
            S7commScoringResult r = useCase.score(vector(i, false, 4, 0), false, "a", "b", window);
            window = r.window();
            p = r.prediction();
        }
        assertEquals(0, p.qualityFlags(), "and gone once it left");
    }

    // A window filled under another bundle is never scored by this one.
    @Test
    void aWindowFromAnotherBundleIsEmptiedFirst() {
        S7commScoreWindow other = S7commScoreWindow.empty("s7comm-stage1-detector/v0", 16, 21);
        for (int i = 0; i < 16; i++) {
            other.append(new float[21], 0);
        }
        S7commDetectorPrediction p = useCase.score(vector(0, false, 4, 0), false, "a", "b", other).prediction();
        assertEquals(DetectorVerdict.WARMUP, p.verdict());
        assertEquals(1, p.eventsSinceReset());
    }

    // Plan ruling P2: a non-finite score is UNSCORABLE, never NORMAL; the row stays.
    @Test
    void aNonFiniteReconstructionIsUnscorable() {
        S7commScoreWindow window = feed(15);
        scorer.offset = Float.NaN;
        S7commScoringResult r = useCase.score(vector(15, false, 4, 0), false, "a", "b", window);
        assertEquals(DetectorVerdict.UNSCORABLE, r.prediction().verdict());
        assertNull(r.prediction().score());
        assertNull(r.prediction().pValue());
        scorer.offset = 0f;
        assertEquals(DetectorVerdict.NORMAL,
            useCase.score(vector(16, false, 4, 0), false, "a", "b", r.window()).prediction().verdict(),
            "the window was kept");
    }

    @Test
    void aBundleForAnotherSchemaIsRefused() {
        assertThrows(IllegalStateException.class,
            () -> new ScoreS7commSequenceUseCase(new StubScorer(bundle("modbus-feature-v1")), CLOCK));
    }

    @Test
    void thePredictionCarriesTheEventsAndTheBundlesIdentity() {
        FeatureVector v = vector(0, true, 4, 0);
        S7commDetectorPrediction p = useCase.score(v, true, "10.0.0.5", "10.0.0.9", null).prediction();
        assertEquals(Prediction.deriveId(v.eventId(), "s7comm-stage1-detector", "v1"), p.predictionId());
        assertEquals(v.eventId(), p.eventId());
        assertEquals("C1", p.connectionUid());
        assertEquals("10.0.0.5", p.clientIp());
        assertEquals("10.0.0.9", p.serverIp());
        assertEquals("b".repeat(64), p.modelSha());
        assertEquals(S7commFeatureSchemaV1.SCHEMA.id(), p.schemaId());
        assertEquals(Instant.parse("2026-09-28T12:00:00Z"), p.producedAt());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/application -am -Dtest='ScoreS7commSequenceUseCaseTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class ReconstructionScorer`.

- [ ] **Step 3: Implement**

`ReconstructionScorer`:
```java
package io.netsecml.platform.port.out;

import io.netsecml.platform.domain.model.S7commDetectorBundle;

// Reconstructs one window of preprocessed vectors with a loaded S7comm
// detector and returns the reconstruction of its LAST row -- the only row the
// detector's score reads (causal_shadow.py). AutoCloseable because the real
// one holds an ONNX Runtime session; close() declares no checked exception, so
// an implementation rethrows its own close failure unchecked.
public interface ReconstructionScorer extends AutoCloseable {

    // The bundle this scorer was built from.
    S7commDetectorBundle bundle();

    // window: bundle().sequenceLength() rows of bundle().featureCount()
    // preprocessed values, oldest first.
    float[] reconstructLast(float[][] window);

    @Override
    void close();
}
```

`ReconstructionScorerFactory`:
```java
package io.netsecml.platform.port.out;

import java.io.Serializable;

// Creates one ReconstructionScorer per Flink subtask, inside that subtask's
// open(): an ONNX Runtime session is not shared across subtasks. Serializable
// so the factory (a bundle path) travels with the operator to every TaskManager.
public interface ReconstructionScorerFactory extends Serializable {
    ReconstructionScorer create();
}
```

`S7commScoringResult`:
```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;

// One event's prediction and its connection's window after it.
public record S7commScoringResult(S7commDetectorPrediction prediction, S7commScoreWindow window) {
}
```

`ScoreS7commSequenceUseCase`:
```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.port.out.ReconstructionScorer;

import java.time.Clock;

// Scores one S7comm feature vector
// (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md sections
// 3-5): keeps its connection's window of preprocessed vectors, reconstructs the
// window once full, turns the last event's weighted error into a
// group-conditional conformal p-value, and that into a verdict. The window is
// updated in place and handed back, like ModbusScoreWindow.
public final class ScoreS7commSequenceUseCase {

    private final ReconstructionScorer scorer;
    private final S7commDetectorBundle bundle;
    private final Clock clock;
    private final float[] weights;
    private final float weightSum;

    public ScoreS7commSequenceUseCase(ReconstructionScorer scorer, Clock clock) {
        this.scorer = scorer;
        this.bundle = scorer.bundle();
        this.clock = clock;
        // The detector must be the one trained on the vectors this job builds.
        if (!S7commFeatureSchemaV1.SCHEMA.id().equals(bundle.schemaId())) {
            throw new IllegalStateException("bundle " + bundle.bundleId() + " was trained on " + bundle.schemaId()
                + ", not " + S7commFeatureSchemaV1.SCHEMA.id());
        }
        // The score's weights, once: they are frozen with the bundle.
        this.weights = bundle.scoreWeights();
        float sum = 0f;
        for (float w : weights) {
            sum += w;
        }
        this.weightSum = sum;
    }

    public S7commScoringResult score(FeatureVector vector, boolean freshState, String clientIp, String serverIp,
                                     S7commScoreWindow window) {
        // A window filled under another bundle, or none yet: start empty.
        if (window == null || !window.bundleId().equals(bundle.bundleId())) {
            window = S7commScoreWindow.empty(bundle.bundleId(), bundle.sequenceLength(), bundle.featureCount());
        }
        // The feature engine started this connection from empty state: so does
        // the window (spec section 5, D4).
        if (freshState) {
            window.reset();
        }
        long eventsSinceReset = window.countEvent();
        float[] raw = vector.values();
        S7commScoreGroup group = S7commScoreGroup.of(raw);
        double alpha = bundle.policy().alpha(group);
        // Always finite (the preprocessing imputes and clips), so every vector
        // enters the window (plan ruling P2).
        float[] preprocessed = bundle.preprocessing().apply(raw);
        window.append(preprocessed, vector.qualityFlags());
        // Fewer than a full window: WARMUP (spec section 3.5).
        if (!window.isFull()) {
            return new S7commScoringResult(prediction(vector, clientIp, serverIp, DetectorVerdict.WARMUP, null,
                null, group, alpha, eventsSinceReset, window.flagsOr(), 0L), window);
        }
        // A full window: reconstruct it and judge the last event.
        long started = System.nanoTime();
        float[] reconstruction = scorer.reconstructLast(window.sequence());
        long micros = (System.nanoTime() - started) / 1_000;
        float score = lastEventScore(reconstruction, preprocessed);
        // A non-finite score cannot be judged (plan ruling P2).
        if (!Float.isFinite(score)) {
            return new S7commScoringResult(prediction(vector, clientIp, serverIp, DetectorVerdict.UNSCORABLE,
                null, null, group, alpha, eventsSinceReset, window.flagsOr(), micros), window);
        }
        double p = bundle.policy().pValue(group, score);
        DetectorVerdict verdict = bundle.policy().anomalous(group, score) ? DetectorVerdict.ANOMALY
            : DetectorVerdict.NORMAL;
        return new S7commScoringResult(prediction(vector, clientIp, serverIp, verdict, score, p, group, alpha,
            eventsSinceReset, window.flagsOr(), micros), window);
    }

    // upstream's causal score (causal_shadow.py) in its own float32
    // arithmetic: sum_j w_j * (reconstruction_j - x_j)^2 / sum_j w_j.
    private float lastEventScore(float[] reconstruction, float[] actual) {
        if (reconstruction.length != actual.length) {
            throw new IllegalStateException("the detector returned " + reconstruction.length + " values for a row of "
                + actual.length);
        }
        float sum = 0f;
        for (int j = 0; j < actual.length; j++) {
            float d = reconstruction[j] - actual[j];
            sum += weights[j] * (d * d);
        }
        return sum / weightSum;
    }

    // The prediction's fields: the vector's identity, the connection's
    // endpoints, the bundle's identity, and this event's outcome.
    private S7commDetectorPrediction prediction(FeatureVector vector, String clientIp, String serverIp,
                                                DetectorVerdict verdict, Float score, Double p,
                                                S7commScoreGroup group, double alpha, long eventsSinceReset,
                                                int flags, long micros) {
        return new S7commDetectorPrediction(
            Prediction.deriveId(vector.eventId(), bundle.name(), bundle.version()),
            vector.eventId(), vector.eventTime(), vector.sensor(), vector.connectionUid(), clientIp, serverIp,
            bundle.name(), bundle.version(), bundle.modelSha(), bundle.schemaId(), bundle.schemaHash(),
            verdict, score, p, group, alpha, eventsSinceReset, flags, micros, clock.instant());
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `Tests run: 10, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/ports/src/main/java/io/netsecml/platform/port/out/ReconstructionScorer.java \
  modules/ports/src/main/java/io/netsecml/platform/port/out/ReconstructionScorerFactory.java \
  modules/application/src/main/java/io/netsecml/platform/application/usecase/ScoreS7commSequenceUseCase.java \
  modules/application/src/main/java/io/netsecml/platform/application/usecase/S7commScoringResult.java \
  modules/application/src/test/java/io/netsecml/platform/application/usecase/ScoreS7commSequenceUseCaseTest.java
git commit -m "feat(s7comm): the reconstruction scorer port and the scoring use case

<attribution lines>"
```

---

### Task 7: The bundle contract, the packaging script and the fixture bundle

**Files:**
- Create: `contracts/model/s7comm-detector-bundle-v1.json`
- Create: `deploy/models/package-s7comm-detector.sh`
- Modify: `deploy/tests/test_models.sh` (a second section)
- Create (generated by the script): `tests/fixtures/models/s7comm-stage1-detector/v1/{bundle.json,model.onnx,preprocessing.json,policy.json,calibration.npz}`

**Interfaces:**
- Produces: `bundle.json`, with the fields `name`, `version`, `schemaId`, `sequenceLength`, `featureCount`, `inputName`, `outputName`, `zeroWeightFeatures`, `modelSha`, `preprocessingSha`, `policySha` and `calibrationSha`. Task 8 reads them.

- [ ] **Step 1: Write the failing test**

Append to `deploy/tests/test_models.sh`, before its final `finish`, and extend its header comment with "and deploy/models/package-s7comm-detector.sh the same way, against the release's FROZEN_MANIFEST.json":
```bash
# --- package-s7comm-detector.sh (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md section 7) ---
s7script="$HERE/../models/package-s7comm-detector.sh"

# A fake S7 delivery in the real release's layout, its FROZEN_MANIFEST.json
# recording every file's SHA-256 as the model team's does.
fake_s7_delivery() {
  local rel="$1/models/stage1_anomaly/v4_causal_final_r1" art="artifacts/v4_causal_final_model" files='{}' f
  mkdir -p "$rel/$art"
  printf 'not really onnx' > "$rel/$art/s7comm_lstm_autoencoder_debiased.onnx"
  chmod 775 "$rel/$art/s7comm_lstm_autoencoder_debiased.onnx"
  printf '{"transformed_dimension":21}' > "$rel/$art/preprocessor_contract.json"
  printf '{"score_semantics":{"sequence_length":16}}' > "$rel/$art/causal_online_shadow_policy.json"
  printf 'not really npz' > "$rel/$art/causal_online_conformal_calibration_scores.npz"
  printf '{"identity_features_excluded_from_event_score":["s7_operation"]}' > "$rel/$art/shadow_deployment_manifest.json"
  for f in s7comm_lstm_autoencoder_debiased.onnx preprocessor_contract.json causal_online_shadow_policy.json \
           causal_online_conformal_calibration_scores.npz shadow_deployment_manifest.json; do
    files="$(jq --arg k "$art/$f" --arg s "$(sha256sum "$rel/$art/$f" | cut -c1-64)" '. + {($k): {sha256: $s}}' <<< "$files")"
  done
  jq -n --argjson files "$files" '{files: $files}' > "$rel/FROZEN_MANIFEST.json"
}

fake_s7_delivery "$tmp/s7"
out="$(bash "$s7script" "$tmp/s7" "$tmp/s7out" 2>&1)"; status=$?
s="$tmp/s7out/s7comm-stage1-detector/v1"
assert_eq 0 "$status" "packaging a sound S7 delivery succeeds"
assert_eq "bundle.json calibration.npz model.onnx policy.json preprocessing.json" "$(cd "$s" && printf '%s\n' * | sort | tr '\n' ' ' | sed 's/ $//')" "the S7 bundle's five files"
assert_eq s7comm-stage1-detector "$(jq -r .name "$s/bundle.json")" "S7 name"
assert_eq s7comm-feature-v1 "$(jq -r .schemaId "$s/bundle.json")" "S7 schemaId"
assert_eq 16 "$(jq -r .sequenceLength "$s/bundle.json")" "sequenceLength from the policy"
assert_eq 21 "$(jq -r .featureCount "$s/bundle.json")" "featureCount from the preprocessing contract"
assert_eq input "$(jq -r .inputName "$s/bundle.json")" "inputName"
assert_eq reconstruction "$(jq -r .outputName "$s/bundle.json")" "outputName"
assert_eq '["s7_operation"]' "$(jq -c .zeroWeightFeatures "$s/bundle.json")" "zeroWeightFeatures from the deployment manifest"
for pair in model.onnx:modelSha preprocessing.json:preprocessingSha policy.json:policySha calibration.npz:calibrationSha; do
  assert_eq "$(sha256sum "$s/${pair%%:*}" | cut -c1-64)" "$(jq -r ".${pair#*:}" "$s/bundle.json")" "${pair#*:}"
done
assert_eq "$(sha256sum "$tmp/s7/models/stage1_anomaly/v4_causal_final_r1/artifacts/v4_causal_final_model/causal_online_conformal_calibration_scores.npz" | cut -c1-64)" \
  "$(sha256sum "$s/calibration.npz" | cut -c1-64)" "the calibration scores are copied byte for byte"
assert_eq "644 644 644 644 644" "$(cd "$s" && stat -c %a bundle.json calibration.npz model.onnx policy.json preprocessing.json | tr '\n' ' ' | sed 's/ $//')" "S7 bundle files are plain data"

# Immutable, and a tampered file never gets packaged.
out="$(bash "$s7script" "$tmp/s7" "$tmp/s7out" 2>&1)"; status=$?
assert_eq 1 "$status" "an existing S7 bundle is never overwritten"
fake_s7_delivery "$tmp/s7bad"
printf 'tampered' >> "$tmp/s7bad/models/stage1_anomaly/v4_causal_final_r1/artifacts/v4_causal_final_model/causal_online_shadow_policy.json"
out="$(bash "$s7script" "$tmp/s7bad" "$tmp/s7out2" 2>&1)"; status=$?
assert_eq 1 "$status" "a file that does not match the release's manifest is refused"
assert_eq 1 "$(grep -c "causal_online_shadow_policy.json does not match the release's own manifest" <<< "$out")" "naming it"
assert_eq no "$([ -e "$tmp/s7out2/s7comm-stage1-detector" ] && echo yes || echo no)" "and writes nothing"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash deploy/tests/test_models.sh`
Expected: the Modbus assertions pass, then the S7 ones fail (`packaging a sound S7 delivery succeeds` gets status 127: no such script). The final line reports failures.

- [ ] **Step 3: Write the contract and the script**

`contracts/model/s7comm-detector-bundle-v1.json`:
```json
{
  "id": "s7comm-detector-bundle-v1",
  "semanticVersion": "1.0.0",
  "encoding": "application/json",
  "fields": [
    {"name": "name", "type": "string", "required": true, "description": "Model family, e.g. s7comm-stage1-detector; with version, the bundle's identity in every prediction"},
    {"name": "version", "type": "string", "required": true, "description": "Version within the family, e.g. v1"},
    {"name": "schemaId", "type": "string", "required": true, "description": "Feature schema the detector was trained on, s7comm-feature-v1; must resolve in FeatureSchemaRegistry, whose feature order must equal preprocessing.json's raw_feature_order"},
    {"name": "sequenceLength", "type": "integer", "required": true, "description": "Events per window, 16; must equal policy.json's score_semantics.sequence_length"},
    {"name": "featureCount", "type": "integer", "required": true, "description": "Preprocessed values per event, 21; must equal preprocessing.json's transformed_dimension"},
    {"name": "inputName", "type": "string", "required": true, "description": "The ONNX graph's input, shape [?, sequenceLength, featureCount]"},
    {"name": "outputName", "type": "string", "required": true, "description": "The reconstruction output, shape [?, sequenceLength or symbolic, featureCount]"},
    {"name": "zeroWeightFeatures", "type": "array", "items": {"type": "string"}, "required": true, "description": "Raw features whose preprocessed columns weigh 0 in the score: the delivery's identity_features_excluded_from_event_score"},
    {"name": "modelSha", "type": "string", "required": true, "description": "SHA-256 of model.onnx, 64 lowercase hex"},
    {"name": "preprocessingSha", "type": "string", "required": true, "description": "SHA-256 of preprocessing.json, the delivered preprocessor_contract.json byte for byte"},
    {"name": "policySha", "type": "string", "required": true, "description": "SHA-256 of policy.json, the delivered causal_online_shadow_policy.json byte for byte"},
    {"name": "calibrationSha", "type": "string", "required": true, "description": "SHA-256 of calibration.npz, the delivered causal_online_conformal_calibration_scores.npz byte for byte"}
  ]
}
```

`deploy/models/package-s7comm-detector.sh` (then `chmod +x`):
```bash
#!/usr/bin/env bash
# Packages the model team's S7comm Stage 1 delivery into the SHA-pinned bundle
# the online job loads (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
# section 7): models/s7comm-stage1-detector/v1/ with model.onnx,
# preprocessing.json, policy.json and calibration.npz byte for byte as
# delivered, plus bundle.json. Refuses any file that does not match the
# release's own FROZEN_MANIFEST.json, and never overwrites a bundle.
# Usage: package-s7comm-detector.sh <delivery-dir> [out-root, default: models]
set -euo pipefail

die() { printf 'package-s7comm-detector: %s\n' "$*" >&2; exit 1; }

delivery="${1:?usage: package-s7comm-detector.sh <delivery-dir> [out-root]}"
out_root="${2:-models}"
name=s7comm-stage1-detector
version=v1

# The release, where the model team's layout puts it, and its own manifest.
release="$delivery/models/stage1_anomaly/v4_causal_final_r1"
artifacts="artifacts/v4_causal_final_model"
manifest="$release/FROZEN_MANIFEST.json"
[ -f "$manifest" ] || die "missing $manifest"

# Each file must be the one the release's manifest records.
verify() {
  local file="$artifacts/$1" expected actual
  [ -f "$release/$file" ] || die "missing $release/$file"
  expected="$(jq -r --arg f "$file" '.files[$f].sha256 // empty' "$manifest")"
  actual="$(sha256sum "$release/$file" | cut -c1-64)"
  if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
    die "$release/$file does not match the release's own manifest (expected ${expected:-none}, got $actual)"
  fi
}
for f in s7comm_lstm_autoencoder_debiased.onnx preprocessor_contract.json causal_online_shadow_policy.json \
         causal_online_conformal_calibration_scores.npz shadow_deployment_manifest.json; do
  verify "$f"
done

# Bundles are immutable: a new model is a new version, never an overwrite.
out="$out_root/$name/$version"
[ ! -e "$out" ] || die "$out already exists; bundles are immutable -- package a new version instead"

# The four runtime files, byte for byte, then the manifest that pins them.
mkdir -p "$out"
cp "$release/$artifacts/s7comm_lstm_autoencoder_debiased.onnx" "$out/model.onnx"
cp "$release/$artifacts/preprocessor_contract.json" "$out/preprocessing.json"
cp "$release/$artifacts/causal_online_shadow_policy.json" "$out/policy.json"
cp "$release/$artifacts/causal_online_conformal_calibration_scores.npz" "$out/calibration.npz"
sha() { sha256sum "$1" | cut -c1-64; }
# The graph's names are the delivery README's (section 8); the ONNX scorer
# checks them against the graph itself when it opens it.
jq -n --arg name "$name" --arg version "$version" \
  --argjson length "$(jq '.score_semantics.sequence_length' "$out/policy.json")" \
  --argjson width "$(jq '.transformed_dimension' "$out/preprocessing.json")" \
  --argjson zero "$(jq -c '.identity_features_excluded_from_event_score' "$release/$artifacts/shadow_deployment_manifest.json")" \
  --arg model "$(sha "$out/model.onnx")" --arg prep "$(sha "$out/preprocessing.json")" \
  --arg policy "$(sha "$out/policy.json")" --arg cal "$(sha "$out/calibration.npz")" \
  '{name: $name, version: $version, schemaId: "s7comm-feature-v1", sequenceLength: $length, featureCount: $width,
    inputName: "input", outputName: "reconstruction", zeroWeightFeatures: $zero,
    modelSha: $model, preprocessingSha: $prep, policySha: $policy, calibrationSha: $cal}' > "$out/bundle.json"
# Plain data, whatever mode the delivery's files had.
chmod 644 "$out/model.onnx" "$out/preprocessing.json" "$out/policy.json" "$out/calibration.npz" "$out/bundle.json"
printf 'packaged %s\n' "$out"
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `chmod +x deploy/models/package-s7comm-detector.sh && bash deploy/tests/test_models.sh`
Expected: every assertion passes, `0 failed`.

- [ ] **Step 5: Package the real delivery as the test fixture**

```bash
bash deploy/models/package-s7comm-detector.sh models/S7 tests/fixtures/models
cat tests/fixtures/models/s7comm-stage1-detector/v1/bundle.json
git check-ignore -v tests/fixtures/models/s7comm-stage1-detector/v1/* || echo "none ignored"
```
Expected: `packaged tests/fixtures/models/s7comm-stage1-detector/v1`. `modelSha` is `2a2e5fe233ee37f22e8e7fe29e2438c1145ca7c799719091eaa0a309b9716858`, `sequenceLength` 16, `featureCount` 21, and `none ignored`.

- [ ] **Step 6: Commit**

```bash
git add contracts/model/s7comm-detector-bundle-v1.json deploy/models/package-s7comm-detector.sh \
  deploy/tests/test_models.sh tests/fixtures/models/s7comm-stage1-detector
git commit -m "feat(s7comm): the detector bundle contract, packaging script and fixture

<attribution lines>"
```

---

### Task 8: The bundle loader and the `.npz` reader

**Files:**
- Create: `modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/NpzReader.java`
- Create: `modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/S7commDetectorBundleLoader.java`
- Create: `modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/LoadedS7commDetector.java`
- Test: `modules/adapter-registry-filesystem/src/test/java/io/netsecml/platform/adapter/registry/NpzReaderTest.java`
- Test: `modules/adapter-registry-filesystem/src/test/java/io/netsecml/platform/adapter/registry/S7commDetectorBundleLoaderTest.java`

**Interfaces:**
- Consumes: the Task 7 bundle; Tasks 3-5 types; `FeatureSchemaRegistry`, `S7commFeatureSchemaV1`.
- Produces:
  - `static LoadedS7commDetector S7commDetectorBundleLoader.load(Path bundleDir) throws IOException`;
  - `record LoadedS7commDetector(S7commDetectorBundle bundle, byte[] model)`;
  - package-private `NpzReader.read(byte[])` → `Map<String, double[]>` and `NpzReader.npy(String, byte[])` → `double[]`.

- [ ] **Step 1: Write the failing tests**

`NpzReaderTest`:
```java
package io.netsecml.platform.adapter.registry;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The delivered calibration scores read exactly as NumPy wrote them, and
// anything that is not a 1-D little-endian float64 array is refused.
class NpzReaderTest {

    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v1", "calibration.npz");

    // One .npy file: magic, version, header length, the header padded with
    // spaces to a multiple of 64 bytes and ending in a newline, then the data.
    private static byte[] npy(int major, String header, double... values) {
        int lengthBytes = major == 1 ? 2 : 4;
        int unpadded = 8 + lengthBytes + header.length() + 1;
        String padded = header + " ".repeat((unpadded + 63) / 64 * 64 - unpadded) + "\n";
        ByteBuffer b = ByteBuffer.allocate(8 + lengthBytes + padded.length() + values.length * 8)
            .order(ByteOrder.LITTLE_ENDIAN);
        b.put(new byte[]{(byte) 0x93, 'N', 'U', 'M', 'P', 'Y'}).put((byte) major).put((byte) 0);
        if (major == 1) {
            b.putShort((short) padded.length());
        } else {
            b.putInt(padded.length());
        }
        b.put(padded.getBytes(StandardCharsets.ISO_8859_1));
        for (double v : values) {
            b.putDouble(v);
        }
        return b.array();
    }

    private static String header(String descr, String fortran, String shape) {
        return "{'descr': '" + descr + "', 'fortran_order': " + fortran + ", 'shape': " + shape + ", }";
    }

    @Test
    void theDeliveredFileReadsAsNumPyWroteIt() throws Exception {
        Map<String, double[]> arrays = NpzReader.read(Files.readAllBytes(FIXTURE));
        assertEquals(List.of("RESPONSE", "READ_REQUEST", "WRITE_REQUEST", "OTHER_REQUEST"),
            new ArrayList<>(arrays.keySet()));
        assertEquals(17824, arrays.get("RESPONSE").length);
        assertEquals(17760, arrays.get("READ_REQUEST").length);
        assertEquals(63, arrays.get("WRITE_REQUEST").length);
        assertEquals(0, arrays.get("OTHER_REQUEST").length);
        assertEquals(5.776698799309088e-06, arrays.get("RESPONSE")[0]);
        assertEquals(0.12811486423015594, arrays.get("RESPONSE")[1]);
        assertEquals(1.2200343007862102e-05, arrays.get("READ_REQUEST")[1]);
        assertEquals(0.0015446325996890664, arrays.get("WRITE_REQUEST")[2]);
    }

    @Test
    void aVersionTwoHeaderReads() {
        assertArrayEquals(new double[]{1.5, -2.0},
            NpzReader.npy("a.npy", npy(2, header("<f8", "False", "(2,)"), 1.5, -2.0)));
    }

    @Test
    void anythingButA1DLittleEndianFloat64ArrayIsRefused() {
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header(">f8", "False", "(1,)"), 1.0)), "big-endian");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f4", "False", "(1,)"), 1.0)), "float32");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f8", "False", "(1, 2)"), 1.0, 2.0)), "2-D");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f8", "True", "(1,)"), 1.0)), "Fortran order");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", npy(1, header("<f8", "False", "(3,)"), 1.0, 2.0)), "truncated");
        assertThrows(IllegalStateException.class,
            () -> NpzReader.npy("a.npy", "not numpy".getBytes(StandardCharsets.US_ASCII)), "no magic");
    }

    @Test
    void anEntryThatIsNotAnNpyArrayIsRefused() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("notes.txt"));
            zip.write("hello".getBytes(StandardCharsets.US_ASCII));
            zip.closeEntry();
        }
        assertThrows(IllegalStateException.class, () -> NpzReader.read(bytes.toByteArray()));
    }
}
```

`S7commDetectorBundleLoaderTest`:
```java
package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The fixture bundle (the real delivery, packaged) loads with the delivered
// values, its preprocessing reproduces the delivered Python preprocessor, and
// every check of spec section 7 refuses a bundle that fails it, naming it.
class S7commDetectorBundleLoaderTest {

    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v1");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Three raw s7comm-feature-v1 vectors and the delivered preprocessor.joblib's
    // output for them, computed in Python (scikit-learn 1.9.1) with S1 applied
    // (the operation upper-cased): a WRITE_VAR request, a user-data response
    // (operation code 0 -> FUNCTION_0X00, spec 2.1) and an ACK carrying PLC_STOP
    // (ROSCTR 2 and PLC_STOP are both unseen categories: all zeros).
    private static final float[][] RAW = {
        {1, 0.5f, 1, 1617, 1, 0.5f, 1, 0, 0, 0, 1, 1, 1, 0, 1, 5},
        {3, 2.5f, 0.25f, 2, 4, 0.75f, 0.25f, 0.5f, 0.625f, 0.375f, 0.5f, 0.875f, 0, 1, 7, 0},
        {0, 0, 0, 50000, 40, 0, 0, 1, 1, 1, 0, 0.03125f, 1, 1, 2, 0x29},
    };
    private static final float[][] PREPROCESSED = {
        {0.0f, -6.0f, 1.0f, 0.5665310621261597f, 0.0f, 0.5f, 1.0f, 0.0f, 0.0f, 0.0f, 1.0f, 1.0f, 1.0f, 0.0f, 1.0f,
            0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f},
        {1.0f, 10.0f, 0.25f, -0.3718768060207367f, 3.0f, 0.75f, 0.25f, 0.5f, 0.625f, 0.375f, 0.5f, 0.875f, 0.0f,
            1.0f, 0.0f, 0.0f, 1.0f, 1.0f, 0.0f, 0.0f, 0.0f},
        {-0.5f, -10.0f, 0.0f, 20.0f, 20.0f, 0.0f, 0.0f, 1.0f, 1.0f, 1.0f, 0.0f, 0.03125f, 1.0f, 1.0f, 0.0f, 0.0f,
            0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
    };

    @Test
    void theFixtureBundleLoadsWithTheDeliveredValues() throws IOException {
        LoadedS7commDetector loaded = S7commDetectorBundleLoader.load(FIXTURE);
        S7commDetectorBundle b = loaded.bundle();
        assertEquals("s7comm-stage1-detector/v1", b.bundleId());
        assertEquals("2a2e5fe233ee37f22e8e7fe29e2438c1145ca7c799719091eaa0a309b9716858", b.modelSha());
        assertEquals(S7commFeatureSchemaV1.CONTENT_HASH, b.schemaHash(), "the registered schema's hash");
        assertEquals(16, b.sequenceLength());
        assertEquals(21, b.featureCount());
        assertEquals("input", b.inputName());
        assertEquals("reconstruction", b.outputName());
        assertEquals(Files.size(FIXTURE.resolve("model.onnx")), loaded.model().length);
        // Σw = 17: the four s7_operation columns weigh nothing.
        float sum = 0f;
        for (float w : b.scoreWeights()) {
            sum += w;
        }
        assertEquals(17f, sum);
        assertEquals(0f, b.scoreWeights()[17]);
    }

    @Test
    void thePolicyIsTheDeliveredOne() throws IOException {
        S7commConformalPolicy p = S7commDetectorBundleLoader.load(FIXTURE).bundle().policy();
        assertEquals(17824, p.calibrationSize(S7commScoreGroup.RESPONSE));
        assertEquals(17760, p.calibrationSize(S7commScoreGroup.READ_REQUEST));
        assertEquals(63, p.calibrationSize(S7commScoreGroup.WRITE_REQUEST));
        assertEquals(35647, p.calibrationSize(S7commScoreGroup.OTHER_REQUEST), "no scores: every group pooled");
        assertEquals(0.001, p.alpha(S7commScoreGroup.RESPONSE));
        assertEquals(0.001, p.alpha(S7commScoreGroup.READ_REQUEST));
        assertEquals(0.015625, p.alpha(S7commScoreGroup.WRITE_REQUEST), "the policy file's, not the YAML's 0.05");
        assertEquals(0.001, p.alpha(S7commScoreGroup.OTHER_REQUEST), "the fallback, not its configured 0.01");
        // The largest WRITE calibration score is 0.034061819314956665.
        assertFalse(p.anomalous(S7commScoreGroup.WRITE_REQUEST, 0.034061819314956665));
        assertTrue(p.anomalous(S7commScoreGroup.WRITE_REQUEST, 0.0341));
    }

    // The loader's preprocessing reproduces the delivered Python preprocessor.
    @Test
    void thePreprocessingMatchesPython() throws IOException {
        S7commDetectorBundle b = S7commDetectorBundleLoader.load(FIXTURE).bundle();
        for (int r = 0; r < RAW.length; r++) {
            float[] out = b.preprocessing().apply(RAW[r]);
            for (int i = 0; i < out.length; i++) {
                assertEquals(PREPROCESSED[r][i], out[i], 1e-6f, "row " + r + " column " + i);
            }
        }
    }

    @Test
    void eachFileThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        for (String file : new String[]{"model.onnx", "preprocessing.json", "policy.json", "calibration.npz"}) {
            Path b = copyFixture(dir.resolve(file));
            Files.write(b.resolve(file), new byte[]{1, 2, 3});
            assertRefused(b, file);
        }
    }

    // raw_feature_order with two features swapped, its SHA updated so only the order is wrong.
    @Test
    void aRawFeatureOrderThatDiffersFromTheSchemaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode prep = (ObjectNode) MAPPER.readTree(b.resolve("preprocessing.json").toFile());
        ArrayNode order = (ArrayNode) prep.get("raw_feature_order");
        String first = order.get(0).asText();
        order.set(0, order.get(1));
        order.set(1, MAPPER.getNodeFactory().textNode(first));
        Files.writeString(b.resolve("preprocessing.json"), prep.toString());
        rewriteSha(b, "preprocessingSha", "preprocessing.json");
        assertRefused(b, "raw_feature_order");
    }

    @Test
    void aPolicyForAnotherWindowLengthIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode policy = (ObjectNode) MAPPER.readTree(b.resolve("policy.json").toFile());
        ((ObjectNode) policy.get("score_semantics")).put("sequence_length", 20);
        Files.writeString(b.resolve("policy.json"), policy.toString());
        rewriteSha(b, "policySha", "policy.json");
        assertRefused(b, "sequence_length");
    }

    // calibration.npz without OTHER_REQUEST.npy, its SHA updated.
    @Test
    void aCalibrationFileMissingAGroupIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(b.resolve("calibration.npz")));
             ZipOutputStream out = new ZipOutputStream(bytes)) {
            for (ZipEntry e = in.getNextEntry(); e != null; e = in.getNextEntry()) {
                if (!e.getName().equals("OTHER_REQUEST.npy")) {
                    out.putNextEntry(new ZipEntry(e.getName()));
                    out.write(in.readAllBytes());
                    out.closeEntry();
                }
            }
        }
        Files.write(b.resolve("calibration.npz"), bytes.toByteArray());
        rewriteSha(b, "calibrationSha", "calibration.npz");
        assertRefused(b, "four groups");
    }

    @Test
    void aZeroWeightFeatureThatIsNotARawFeatureIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode bundle = (ObjectNode) MAPPER.readTree(b.resolve("bundle.json").toFile());
        bundle.putArray("zeroWeightFeatures").add("no_such_feature");
        Files.writeString(b.resolve("bundle.json"), bundle.toString());
        assertRefused(b, "zeroWeightFeatures");
    }

    @Test
    void aBundleForAnotherSchemaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode bundle = (ObjectNode) MAPPER.readTree(b.resolve("bundle.json").toFile());
        bundle.put("schemaId", "modbus-feature-v1");
        Files.writeString(b.resolve("bundle.json"), bundle.toString());
        assertRefused(b, "s7comm-feature-v1");
        bundle.put("schemaId", "no-such-schema");
        Files.writeString(b.resolve("bundle.json"), bundle.toString());
        assertThrows(IllegalArgumentException.class, () -> S7commDetectorBundleLoader.load(b));
    }

    @Test
    void aMissingFileIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.delete(b.resolve("policy.json"));
        assertThrows(IOException.class, () -> S7commDetectorBundleLoader.load(b));
    }

    private static void assertRefused(Path bundle, String named) {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> S7commDetectorBundleLoader.load(bundle));
        assertTrue(e.getMessage().contains(named), e.getMessage());
    }

    private static Path copyFixture(Path dir) throws IOException {
        Path b = dir.resolve("v1");
        Files.createDirectories(b);
        try (Stream<Path> files = Files.list(FIXTURE)) {
            for (Path f : files.toList()) {
                Files.copy(f, b.resolve(f.getFileName().toString()));
            }
        }
        return b;
    }

    private static void rewriteSha(Path bundle, String field, String file) throws Exception {
        ObjectNode json = (ObjectNode) MAPPER.readTree(bundle.resolve("bundle.json").toFile());
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(bundle.resolve(file)));
        json.put(field, HexFormat.of().formatHex(digest));
        Files.writeString(bundle.resolve("bundle.json"), json.toString());
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-registry-filesystem -am -Dtest='NpzReaderTest,S7commDetectorBundleLoaderTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class NpzReader`.

- [ ] **Step 3: Implement**

`NpzReader`:
```java
package io.netsecml.platform.adapter.registry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

// Reads a NumPy .npz archive of 1-D little-endian float64 arrays -- the
// delivered calibration scores, read as delivered (spec section 9, D7). An
// .npz is a zip of .npy files; each is the magic "\x93NUMPY", a version, a
// header length, a Python dict literal header, then the raw values. Anything
// else is refused, never guessed at.
final class NpzReader {

    private static final byte[] MAGIC = {(byte) 0x93, 'N', 'U', 'M', 'P', 'Y'};
    private static final Pattern DESCR = Pattern.compile("'descr':\\s*'([^']*)'");
    private static final Pattern FORTRAN = Pattern.compile("'fortran_order':\\s*(True|False)");
    private static final Pattern SHAPE = Pattern.compile("'shape':\\s*\\(\\s*(\\d+)\\s*,\\s*\\)");

    private NpzReader() {
    }

    // Every array in the archive, by entry name without ".npy", in zip order.
    static Map<String, double[]> read(byte[] npz) throws IOException {
        Map<String, double[]> arrays = new LinkedHashMap<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(npz))) {
            for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                String name = entry.getName();
                if (!name.endsWith(".npy")) {
                    throw new IllegalStateException("calibration.npz holds " + name + ", not an .npy array");
                }
                arrays.put(name.substring(0, name.length() - ".npy".length()), npy(name, zip.readAllBytes()));
            }
        }
        if (arrays.isEmpty()) {
            throw new IllegalStateException("calibration.npz holds no arrays");
        }
        return arrays;
    }

    // One .npy file: version 1.0 (a 2-byte header length) or 2.0/3.0 (4-byte).
    static double[] npy(String name, byte[] bytes) {
        if (bytes.length < 10 || !Arrays.equals(Arrays.copyOf(bytes, MAGIC.length), MAGIC)) {
            throw new IllegalStateException(name + " is not an .npy array");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int major = bytes[6];
        int offset = major == 1 ? 10 : 12;
        if (major < 1 || major > 3 || bytes.length < offset) {
            throw new IllegalStateException(name + ": unsupported .npy version " + major);
        }
        int headerLength = major == 1 ? buffer.getShort(8) & 0xFFFF : buffer.getInt(8);
        if (headerLength < 0 || offset + headerLength > bytes.length) {
            throw new IllegalStateException(name + ": its header runs past the end of the file");
        }
        String header = new String(bytes, offset, headerLength, StandardCharsets.ISO_8859_1).trim();
        // Exactly one layout: little-endian float64, C order, one dimension.
        Matcher descr = DESCR.matcher(header);
        Matcher fortran = FORTRAN.matcher(header);
        Matcher shape = SHAPE.matcher(header);
        if (!descr.find() || !descr.group(1).equals("<f8") || !fortran.find() || !fortran.group(1).equals("False")
                || !shape.find()) {
            throw new IllegalStateException(name + " is not a 1-D little-endian float64 array: " + header);
        }
        long count = Long.parseLong(shape.group(1));
        int data = offset + headerLength;
        if (bytes.length - data != count * Double.BYTES) {
            throw new IllegalStateException(name + ": its header says " + count + " values, but "
                + (bytes.length - data) + " bytes follow");
        }
        double[] values = new double[(int) count];
        buffer.position(data);
        buffer.asDoubleBuffer().get(values);
        return values;
    }
}
```

`LoadedS7commDetector`:
```java
package io.netsecml.platform.adapter.registry;

import io.netsecml.platform.domain.model.S7commDetectorBundle;

import java.util.Arrays;

// A verified S7comm bundle and its model bytes, for the ONNX scorer to open.
public record LoadedS7commDetector(S7commDetectorBundle bundle, byte[] model) {

    public LoadedS7commDetector {
        model = Arrays.copyOf(model, model.length);
    }

    @Override
    public byte[] model() {
        return Arrays.copyOf(model, model.length);
    }
}
```

`S7commDetectorBundleLoader`:
```java
package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

// Loads an S7comm detector bundle (contracts/model/s7comm-detector-bundle-v1.json)
// and refuses it unless every check of spec section 7 passes: each file's
// SHA-256, the feature schema, the preprocessing contract's features, order and
// transform, the policy's window, and the calibration file's four groups. The
// graph's names and shapes are checked by the ONNX scorer that opens it.
public final class S7commDetectorBundleLoader {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private S7commDetectorBundleLoader() {
    }

    public static LoadedS7commDetector load(Path bundleDir) throws IOException {
        BundleJson bundle = MAPPER.readValue(bundleDir.resolve("bundle.json").toFile(), BundleJson.class);
        byte[] model = Files.readAllBytes(bundleDir.resolve("model.onnx"));
        byte[] preprocessing = Files.readAllBytes(bundleDir.resolve("preprocessing.json"));
        byte[] policy = Files.readAllBytes(bundleDir.resolve("policy.json"));
        byte[] calibration = Files.readAllBytes(bundleDir.resolve("calibration.npz"));

        // Every file must be exactly the one bundle.json pins.
        requireSha("model.onnx", bundle.modelSha, model);
        requireSha("preprocessing.json", bundle.preprocessingSha, preprocessing);
        requireSha("policy.json", bundle.policySha, policy);
        requireSha("calibration.npz", bundle.calibrationSha, calibration);

        // The preprocessing hard-codes s7comm-feature-v1's layout, so the bundle
        // must be for exactly that schema.
        FeatureSchema schema = FeatureSchemaRegistry.byId(bundle.schemaId);
        require(schema.id().equals(S7commFeatureSchemaV1.SCHEMA.id()), "bundle.json's schemaId is " + schema.id()
            + ", but this detector reads " + S7commFeatureSchemaV1.SCHEMA.id());
        S7commPreprocessing prep = preprocessing(MAPPER.readTree(preprocessing), schema);
        require(prep.width() == bundle.featureCount, "bundle.json's featureCount " + bundle.featureCount
            + " but preprocessing.json produces " + prep.width());

        // The score's weights, from the raw features bundle.json names.
        float[] weights;
        try {
            weights = prep.scoreWeights(bundle.zeroWeightFeatures);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("bundle.json's zeroWeightFeatures: " + e.getMessage(), e);
        }

        return new LoadedS7commDetector(new S7commDetectorBundle(bundle.name, bundle.version, bundle.modelSha,
            schema.id(), schema.contentHash(), bundle.sequenceLength, bundle.featureCount, bundle.inputName,
            bundle.outputName, prep, weights,
            policy(MAPPER.readTree(policy), NpzReader.read(calibration), bundle.sequenceLength)), model);
    }

    // preprocessor_contract.json -> S7commPreprocessing (spec section 3.2),
    // refusing a contract whose features, order or transform differ.
    private static S7commPreprocessing preprocessing(JsonNode c, FeatureSchema schema) {
        List<String> order = schema.definitions().stream().map(FeatureDefinition::name).toList();
        require(texts(field(c, "raw_feature_order")).equals(order),
            "preprocessing.json's raw_feature_order differs from " + schema.id() + "'s feature order");
        JsonNode continuous = field(c, "continuous");
        List<String> names = texts(field(continuous, "features"));
        require(names.equals(order.subList(0, 12)), "preprocessing.json's continuous features are not "
            + schema.id() + "'s first 12");
        require("StableNumericTransformer".equals(field(continuous, "transformer").asText()),
            "preprocessing.json's continuous transformer is not StableNumericTransformer");
        JsonNode binary = field(c, "binary");
        require(texts(field(binary, "features")).equals(order.subList(12, 14)),
            "preprocessing.json's binary features are not " + schema.id() + "'s 13th and 14th");
        require(field(binary, "imputer_statistics").get(0).asDouble() == 0.0
            && field(binary, "imputer_statistics").get(1).asDouble() == 0.0,
            "preprocessing.json's binary imputer is not 0");
        JsonNode categorical = field(c, "categorical");
        require(texts(field(categorical, "features"))
                .equals(List.of(S7commPreprocessing.ROSCTR_FEATURE, S7commPreprocessing.OPERATION_FEATURE)),
            "preprocessing.json's categorical features are not s7_rosctr, s7_operation");

        // Each continuous feature: its median, and its range or its center and scale.
        JsonNode medians = field(continuous, "imputer_statistics");
        JsonNode centers = field(continuous, "robust_center");
        JsonNode scales = field(continuous, "robust_scale");
        JsonNode bounded = field(continuous, "bounded_ranges");
        List<S7commPreprocessing.Continuous> features = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            JsonNode range = bounded.get(names.get(i));
            features.add(range != null
                ? new S7commPreprocessing.Continuous(names.get(i), medians.get(i).asDouble(), 0.0, 1.0,
                    range.get(0).asDouble(), range.get(1).asDouble())
                : new S7commPreprocessing.Continuous(names.get(i), medians.get(i).asDouble(),
                    centers.get(i).asDouble(), scales.get(i).asDouble(), Double.NaN, Double.NaN));
        }
        JsonNode categories = field(categorical, "categories");
        S7commPreprocessing prep = new S7commPreprocessing(features, order.subList(12, 14),
            field(continuous, "transformed_clip").asDouble(),
            texts(field(categories, S7commPreprocessing.ROSCTR_FEATURE)),
            texts(field(categories, S7commPreprocessing.OPERATION_FEATURE)));

        // The contract's own column list must be the one this transform produces.
        require(prep.transformedFeatureOrder().equals(texts(field(c, "transformed_feature_order"))),
            "preprocessing.json's transformed_feature_order differs from the columns its transform produces: "
                + prep.transformedFeatureOrder());
        require(field(c, "transformed_dimension").asInt() == prep.width(),
            "preprocessing.json's transformed_dimension is not " + prep.width());
        return prep;
    }

    // causal_online_shadow_policy.json and the calibration scores ->
    // S7commConformalPolicy (spec section 3.4; alphas from alpha_by_group, D6).
    private static S7commConformalPolicy policy(JsonNode policy, Map<String, double[]> arrays, int sequenceLength) {
        JsonNode semantics = field(policy, "score_semantics");
        require(semantics.path("sequence_length").asInt() == sequenceLength, "policy.json's sequence_length "
            + semantics.path("sequence_length").asInt() + " is not bundle.json's " + sequenceLength);
        require("LAST_ONLY".equals(semantics.path("scored_timestep").asText()),
            "policy.json's scored_timestep is not LAST_ONLY");
        require(semantics.path("minimum_events_before_score").asInt() == sequenceLength,
            "policy.json's minimum_events_before_score is not the window length");
        Set<String> groups = new TreeSet<>(Arrays.stream(S7commScoreGroup.values()).map(Enum::name).toList());
        require(new TreeSet<>(arrays.keySet()).equals(groups),
            "calibration.npz holds " + arrays.keySet() + ", not the four groups " + groups);

        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        arrays.forEach((group, values) -> scores.put(S7commScoreGroup.valueOf(group), values));
        Map<S7commScoreGroup, Double> alphas = new EnumMap<>(S7commScoreGroup.class);
        field(policy, "alpha_by_group").fields().forEachRemaining(e -> {
            require(groups.contains(e.getKey()), "policy.json's alpha_by_group names an unknown group " + e.getKey());
            alphas.put(S7commScoreGroup.valueOf(e.getKey()), e.getValue().asDouble());
        });
        return new S7commConformalPolicy(scores, alphas, field(policy, "fallback_alpha").asDouble());
    }

    private static JsonNode field(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            throw new IllegalStateException("the bundle's contract has no '" + name + "'");
        }
        return value;
    }

    private static List<String> texts(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireSha(String file, String expected, byte[] bytes) {
        String actual = sha256Hex(bytes);
        if (!actual.equals(expected)) {
            throw new IllegalStateException(file + " does not match bundle.json's recorded SHA-256: expected "
                + expected + " but computed " + actual);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }

    // bundle.json, every field required.
    @JsonIgnoreProperties(ignoreUnknown = false)
    private record BundleJson(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "version", required = true) String version,
        @JsonProperty(value = "schemaId", required = true) String schemaId,
        @JsonProperty(value = "sequenceLength", required = true) int sequenceLength,
        @JsonProperty(value = "featureCount", required = true) int featureCount,
        @JsonProperty(value = "inputName", required = true) String inputName,
        @JsonProperty(value = "outputName", required = true) String outputName,
        @JsonProperty(value = "zeroWeightFeatures", required = true) List<String> zeroWeightFeatures,
        @JsonProperty(value = "modelSha", required = true) String modelSha,
        @JsonProperty(value = "preprocessingSha", required = true) String preprocessingSha,
        @JsonProperty(value = "policySha", required = true) String policySha,
        @JsonProperty(value = "calibrationSha", required = true) String calibrationSha) {
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `NpzReaderTest` 4, `S7commDetectorBundleLoaderTest` 10; `Failures: 0, Errors: 0`. If `thePreprocessingMatchesPython` fails on one column, compare that column's rule with section 3.2 before touching the tolerance: the Python values are float32-exact outputs of the same arithmetic.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/NpzReader.java \
  modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/S7commDetectorBundleLoader.java \
  modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/LoadedS7commDetector.java \
  modules/adapter-registry-filesystem/src/test/java/io/netsecml/platform/adapter/registry/NpzReaderTest.java \
  modules/adapter-registry-filesystem/src/test/java/io/netsecml/platform/adapter/registry/S7commDetectorBundleLoaderTest.java
git commit -m "feat(s7comm): load and verify the detector bundle, calibration read as delivered

<attribution lines>"
```

---

### Task 9: The ONNX reconstruction scorer

**Files:**
- Create: `modules/adapter-onnx/src/main/java/io/netsecml/platform/adapter/onnx/runtime/OnnxReconstructionScorer.java`
- Test: `modules/adapter-onnx/src/test/java/io/netsecml/platform/adapter/onnx/runtime/OnnxReconstructionScorerTest.java`

**Interfaces:**
- Consumes: `ReconstructionScorer` (Task 6), `S7commDetectorBundle` (Task 5), the fixture `model.onnx` (Task 7).
- Produces: `OnnxReconstructionScorer(byte[] model, S7commDetectorBundle bundle) implements ReconstructionScorer`.

- [ ] **Step 1: Write the failing tests**

```java
package io.netsecml.platform.adapter.onnx.runtime;

import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The delivered graph, run from Java: its last-row reconstruction equals Python
// ONNX Runtime's (1.30) for the same windows, to 1e-5, and a bundle whose names
// or window length do not fit the graph is refused.
class OnnxReconstructionScorerTest {

    private static final Path MODEL = Path.of("..", "..", "tests", "fixtures", "models", "s7comm-stage1-detector",
        "v1", "model.onnx");

    // Python ONNX Runtime 1.30: the last row's reconstruction of an all-zero window.
    private static final float[] ZERO_LAST = {-0.47339922189712524f, -0.334583044052124f, 0.9121827483177185f,
        -0.6606706380844116f, 0.5798348188400269f, 0.4874168634414673f, 0.6117779612541199f,
        -0.05447503924369812f, 0.00972612202167511f, -0.03914304077625275f, 0.647331953048706f,
        0.11115028709173203f, 0.08977775275707245f, 0.07123750448226929f, 0.08253837376832962f,
        0.8266689777374268f, -0.016102299094200134f, 0.057957109063863754f, 0.5017139315605164f,
        0.019532809033989906f, 0.4741319417953491f};
    // A user-data response's preprocessed row (S7commDetectorBundleLoaderTest's
    // second row), repeated 16 times, and the last row's reconstruction.
    private static final float[] ROW = {1.0f, 10.0f, 0.25f, -0.3718768060207367f, 3.0f, 0.75f, 0.25f, 0.5f, 0.625f,
        0.375f, 0.5f, 0.875f, 0.0f, 1.0f, 0.0f, 0.0f, 1.0f, 1.0f, 0.0f, 0.0f, 0.0f};
    private static final float[] REPEAT_LAST = {-0.4190230965614319f, 0.9907174110412598f, 0.9591835141181946f,
        -0.34542468190193176f, 1.2952054738998413f, 0.6435911059379578f, 0.7296352982521057f,
        -0.1422126740217209f, -0.08790476620197296f, -0.18834571540355682f, 0.9411546587944031f,
        0.10126174241304398f, 0.07165340334177017f, -0.008129667490720749f, 0.1426275670528412f,
        0.8517481088638306f, -0.05910259485244751f, 0.043602608144283295f, 0.8704980611801147f,
        0.015470843762159348f, -0.05655679106712341f};

    private static byte[] model() throws IOException {
        return Files.readAllBytes(MODEL);
    }

    // The graph's contract only: its names and the window length. The
    // preprocessing and policy never reach the graph, so simple ones do.
    private static S7commDetectorBundle bundle(int sequenceLength, String input, String output) {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("s" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        S7commPreprocessing p = new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"),
            List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            scores.put(g, new double[]{0.1});
        }
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64),
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, sequenceLength, 21, input, output,
            p, p.scoreWeights(List.of("s7_operation")), new S7commConformalPolicy(scores, Map.of(), 0.001));
    }

    private static S7commDetectorBundle delivered() {
        return bundle(16, "input", "reconstruction");
    }

    private static void assertRow(float[] expected, float[] actual) {
        assertEquals(expected.length, actual.length);
        for (int j = 0; j < expected.length; j++) {
            assertEquals(expected[j], actual[j], 1e-5f, "column " + j);
        }
    }

    @Test
    void anAllZeroWindowReconstructsAsPythonDoes() throws IOException {
        try (OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), delivered())) {
            assertRow(ZERO_LAST, scorer.reconstructLast(new float[16][21]));
        }
    }

    @Test
    void aRepeatedRowReconstructsAsPythonDoes() throws IOException {
        float[][] window = new float[16][];
        for (int i = 0; i < 16; i++) {
            window[i] = ROW.clone();
        }
        try (OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), delivered())) {
            assertRow(REPEAT_LAST, scorer.reconstructLast(window));
        }
    }

    @Test
    void aBundleThatDoesNotFitTheGraphIsRefused() throws IOException {
        byte[] model = model();
        IllegalStateException input = assertThrows(IllegalStateException.class,
            () -> new OnnxReconstructionScorer(model, bundle(16, "sequence", "reconstruction")));
        assertTrue(input.getMessage().contains("'sequence'"), input.getMessage());
        assertThrows(IllegalStateException.class,
            () -> new OnnxReconstructionScorer(model, bundle(16, "input", "decoded")));
        assertThrows(IllegalStateException.class,
            () -> new OnnxReconstructionScorer(model, bundle(20, "input", "reconstruction")));
    }

    @Test
    void aWindowOfTheWrongShapeIsRejected() throws IOException {
        try (OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), delivered())) {
            assertThrows(IllegalArgumentException.class, () -> scorer.reconstructLast(new float[15][21]));
            assertThrows(IllegalArgumentException.class, () -> scorer.reconstructLast(new float[16][20]));
        }
    }

    @Test
    void closeIsIdempotent() throws IOException {
        OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), delivered());
        scorer.close();
        assertDoesNotThrow(scorer::close);
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-onnx -am -Dtest='OnnxReconstructionScorerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class OnnxReconstructionScorer`.

- [ ] **Step 3: Implement**

```java
package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.port.out.ReconstructionScorer;

import java.util.Arrays;
import java.util.Map;

// Runs the S7comm Stage 1 LSTM autoencoder (scoring design section 3.3): one
// window in, the reconstruction of its last row out -- the only row the score
// reads. One session per subtask, one thread each way (CLAUDE.md's CPU rule).
public final class OnnxReconstructionScorer implements ReconstructionScorer {

    private static final OrtEnvironment ENVIRONMENT = OrtEnvironment.getEnvironment();

    private final S7commDetectorBundle bundle;
    private final OrtSession session;
    private boolean closed = false;

    public OnnxReconstructionScorer(byte[] model, S7commDetectorBundle bundle) {
        this.bundle = bundle;
        try {
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            options.setIntraOpNumThreads(1);
            options.setInterOpNumThreads(1);
            this.session = ENVIRONMENT.createSession(model, options);
        } catch (OrtException e) {
            throw new IllegalStateException("failed to open the ONNX graph of " + bundle.bundleId(), e);
        }
        // The graph must have the names and shapes the bundle declares. The
        // delivered graph leaves its output's row count symbolic (-1).
        try {
            requireShape("input", bundle.inputName(), session.getInputInfo().get(bundle.inputName()), false);
            requireShape("output", bundle.outputName(), session.getOutputInfo().get(bundle.outputName()), true);
        } catch (OrtException | RuntimeException e) {
            IllegalStateException failure = e instanceof IllegalStateException ise ? ise
                : new IllegalStateException("failed to read the graph of " + bundle.bundleId(), e);
            try {
                session.close();
            } catch (OrtException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    // A [batch, rows, width] float tensor with this exact name.
    private void requireShape(String role, String name, NodeInfo info, boolean symbolicRowsAllowed) {
        if (info == null || !(info.getInfo() instanceof TensorInfo tensor)) {
            throw new IllegalStateException("the graph has no " + role + " tensor named '" + name + "'");
        }
        long[] shape = tensor.getShape();
        int rows = bundle.sequenceLength();
        boolean rowsFit = shape.length == 3 && (shape[1] == rows || (symbolicRowsAllowed && shape[1] == -1));
        if (!rowsFit || shape[2] != bundle.featureCount()) {
            throw new IllegalStateException("the graph's " + role + " '" + name + "' has shape "
                + Arrays.toString(shape) + ", not [?, " + rows + ", " + bundle.featureCount() + "]");
        }
    }

    @Override
    public S7commDetectorBundle bundle() {
        return bundle;
    }

    @Override
    public float[] reconstructLast(float[][] window) {
        int length = bundle.sequenceLength();
        int width = bundle.featureCount();
        // Exactly one window of the bundle's shape.
        if (window.length != length) {
            throw new IllegalArgumentException("expected " + length + " rows, got " + window.length);
        }
        for (float[] row : window) {
            if (row.length != width) {
                throw new IllegalArgumentException("expected rows of " + width + " values, got " + row.length);
            }
        }
        try (OnnxTensor input = OnnxTensor.createTensor(ENVIRONMENT, new float[][][]{window});
             OrtSession.Result result = session.run(Map.of(bundle.inputName(), input))) {
            OnnxValue value = result.get(bundle.outputName()).orElseThrow(
                () -> new IllegalStateException("the graph returned no output named '" + bundle.outputName() + "'"));
            float[][][] reconstruction = (float[][][]) value.getValue();
            if (reconstruction.length != 1 || reconstruction[0].length != length
                    || reconstruction[0][length - 1].length != width) {
                throw new IllegalStateException("the graph returned a reconstruction that is not [1, " + length
                    + ", " + width + "]");
            }
            return reconstruction[0][length - 1].clone();
        } catch (OrtException e) {
            throw new IllegalStateException("ONNX Runtime failed reconstructing with " + bundle.bundleId(), e);
        }
    }

    // Idempotent; a close failure is rethrown unchecked, never dropped.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            session.close();
        } catch (OrtException e) {
            throw new IllegalStateException("failed to close the ONNX session of " + bundle.bundleId(), e);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `Tests run: 5, Failures: 0, Errors: 0`. The reconstructions match Python's to 1e-5. If they differ by more, report the largest difference in the ledger. Do not widen the tolerance without a ruling: the scoring oracle (Task 15) relies on it.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-onnx/src/main/java/io/netsecml/platform/adapter/onnx/runtime/OnnxReconstructionScorer.java \
  modules/adapter-onnx/src/test/java/io/netsecml/platform/adapter/onnx/runtime/OnnxReconstructionScorerTest.java
git commit -m "feat(s7comm): the ONNX reconstruction scorer

<attribution lines>"
```

---
### Task 10: The prediction stream contract and its serializer

**Files:**
- Create: `contracts/stream/s7comm-detector-prediction-v1.json`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/S7commDetectorPredictionSerializer.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/S7commDetectorPredictionDeserializer.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/S7commDetectorPredictionSerializerTest.java`
- Modify: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/StreamContractDriftTest.java` (one test)

**Interfaces:**
- Consumes: `S7commDetectorPrediction` (Task 5).
- Produces: `S7commDetectorPredictionSerializer implements Serializer<S7commDetectorPrediction>, Serializable` and `S7commDetectorPredictionDeserializer implements Deserializer<S7commDetectorPrediction>`.

- [ ] **Step 1: Write the failing tests**

`S7commDetectorPredictionSerializerTest`:
```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Round trips, and a WARMUP prediction's score and p-value are JSON nulls, not absent keys.
class S7commDetectorPredictionSerializerTest {

    static S7commDetectorPrediction prediction(DetectorVerdict verdict, Float score, Double p) {
        return new S7commDetectorPrediction("a".repeat(64), "s:C1:25:REQUEST:1790000000123",
            Instant.parse("2026-09-28T12:00:00.123Z"), new SensorId("s"), "C1", "10.0.0.5", "10.0.0.9",
            "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1", "c".repeat(64), verdict, score, p,
            S7commScoreGroup.READ_REQUEST, 0.001, 40, 16, 150L, Instant.parse("2026-09-28T12:00:00.456Z"));
    }

    @Test
    void aWarmupsScoreAndPValueAreJsonNulls() throws Exception {
        JsonNode node = new ObjectMapper().readTree(new S7commDetectorPredictionSerializer()
            .serialize("t", prediction(DetectorVerdict.WARMUP, null, null)));
        assertTrue(node.has("score") && node.get("score").isNull());
        assertTrue(node.has("pValue") && node.get("pValue").isNull());
        assertEquals("READ_REQUEST", node.get("scoreGroup").asText());
    }

    @Test
    void everyFieldSurvivesARoundTrip() {
        for (S7commDetectorPrediction original : new S7commDetectorPrediction[]{
            prediction(DetectorVerdict.WARMUP, null, null), prediction(DetectorVerdict.ANOMALY, 0.53f, 0.2)}) {
            S7commDetectorPrediction restored = new S7commDetectorPredictionDeserializer()
                .deserialize("t", new S7commDetectorPredictionSerializer().serialize("t", original));
            assertEquals(original, restored);
        }
    }
}
```
Append to `StreamContractDriftTest` (its `messageFields` and `contractFields` helpers exist; add the imports for `S7commDetectorPrediction` and `S7commScoreGroup`):
```java
    @Test
    void s7commDetectorPredictionSerializerEmitsExactlyTheContractFields() throws Exception {
        S7commDetectorPrediction scored = new S7commDetectorPrediction("a".repeat(64), "sensor-eu-1:C1:25:REQUEST:1",
            Instant.parse("2026-09-28T12:00:00Z"), new SensorId("sensor-eu-1"), "C1", "10.0.0.5", "10.0.0.9",
            "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1", "c".repeat(64),
            DetectorVerdict.NORMAL, 0.01f, 0.5, S7commScoreGroup.RESPONSE, 0.001, 20, 0, 90L,
            Instant.parse("2026-09-28T12:00:00.004Z"));
        Set<String> emitted = messageFields(new S7commDetectorPredictionSerializer()
            .serialize("netsec.s7comm.prediction.v1", scored));
        assertEquals(contractFields("s7comm-detector-prediction-v1.json"), emitted,
            "the serializer and contracts/stream/s7comm-detector-prediction-v1.json must describe the same message");
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-kafka -am -Dtest='S7commDetectorPredictionSerializerTest,StreamContractDriftTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commDetectorPredictionSerializer`.

- [ ] **Step 3: Implement**

`contracts/stream/s7comm-detector-prediction-v1.json`:
```json
{
  "id": "s7comm-detector-prediction-v1",
  "semanticVersion": "1.0.0",
  "topic": "netsec.s7comm.prediction.v1",
  "encoding": "application/json",
  "fields": [
    {"name": "predictionId", "type": "string", "required": true, "description": "Prediction.deriveId(eventId, modelName, modelVersion): 64 lowercase hex, stable on replay"},
    {"name": "eventId", "type": "string", "required": true, "description": "The scored feature vector's eventId: the join back to it"},
    {"name": "eventTime", "type": "string", "format": "date-time", "required": true, "description": "The event's time, copied from the feature vector, ISO-8601 UTC"},
    {"name": "sensor", "type": "string", "required": true, "description": "The sensor that captured the event"},
    {"name": "connectionUid", "type": "string", "required": true, "description": "Zeek's connection uid: the scorer's key with the sensor"},
    {"name": "clientIp", "type": "string", "required": true, "description": "The connection's client: the sender of a request to port 102"},
    {"name": "serverIp", "type": "string", "required": true, "description": "The connection's server: the PLC on port 102"},
    {"name": "modelName", "type": "string", "required": true, "description": "The detector bundle's name"},
    {"name": "modelVersion", "type": "string", "required": true, "description": "The detector bundle's version"},
    {"name": "modelSha", "type": "string", "required": true, "description": "SHA-256 of the detector's model.onnx"},
    {"name": "schemaId", "type": "string", "required": true, "description": "The feature schema scored, s7comm-feature-v1"},
    {"name": "schemaHash", "type": "string", "required": true, "description": "That schema's content hash"},
    {"name": "verdict", "type": "string", "required": true, "description": "WARMUP (fewer than 16 events since the connection's last reset), NORMAL, ANOMALY, or UNSCORABLE (a non-finite score)"},
    {"name": "score", "type": "number", "required": true, "nullable": true, "description": "The last event's weighted reconstruction error; null unless NORMAL or ANOMALY"},
    {"name": "pValue", "type": "number", "required": true, "nullable": true, "description": "Its conformal p-value against its group's calibration scores; null unless NORMAL or ANOMALY"},
    {"name": "scoreGroup", "type": "string", "required": true, "description": "RESPONSE, READ_REQUEST, WRITE_REQUEST or OTHER_REQUEST: the calibration group that judges the event"},
    {"name": "alpha", "type": "number", "required": true, "description": "The alpha that applies to the group: ANOMALY iff pValue <= alpha"},
    {"name": "eventsSinceReset", "type": "integer", "required": true, "description": "The connection's events since the scorer last reset, this one included; decisions before about the 64th carry the model's post-reset false-alarm rate"},
    {"name": "qualityFlags", "type": "integer", "required": true, "description": "OR of this vector's quality flags and those of every vector in the window"},
    {"name": "inferenceMicros", "type": "integer", "required": true, "description": "Inference time in microseconds; 0 unless the detector ran"},
    {"name": "producedAt", "type": "string", "format": "date-time", "required": true, "description": "When the prediction was emitted, ISO-8601 UTC"}
  ]
}
```

`S7commDetectorPredictionSerializer`:
```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;

// contracts/stream/s7comm-detector-prediction-v1.json, field for field. The
// score and the p-value are always written -- as JSON null when the detector
// did not judge the event -- so every message carries the same keys.
public final class S7commDetectorPredictionSerializer implements Serializer<S7commDetectorPrediction>, Serializable {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, S7commDetectorPrediction p) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            // Which event this prediction is for, and where it came from.
            node.put("predictionId", p.predictionId());
            node.put("eventId", p.eventId());
            node.put("eventTime", p.eventTime().toString());
            node.put("sensor", p.sensor().value());
            node.put("connectionUid", p.connectionUid());
            node.put("clientIp", p.clientIp());
            node.put("serverIp", p.serverIp());
            // The bundle and the feature schema that produced it.
            node.put("modelName", p.modelName());
            node.put("modelVersion", p.modelVersion());
            node.put("modelSha", p.modelSha());
            node.put("schemaId", p.schemaId());
            node.put("schemaHash", p.schemaHash());
            // The verdict and the evidence behind it; null when the detector did not judge.
            node.put("verdict", p.verdict().name());
            node.put("score", p.score());
            node.put("pValue", p.pValue());
            node.put("scoreGroup", p.scoreGroup().name());
            node.put("alpha", p.alpha());
            node.put("eventsSinceReset", p.eventsSinceReset());
            node.put("qualityFlags", p.qualityFlags());
            // Cost and provenance.
            node.put("inferenceMicros", p.inferenceMicros());
            node.put("producedAt", p.producedAt().toString());
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize S7commDetectorPrediction for topic " + topic, e);
        }
    }
}
```

`S7commDetectorPredictionDeserializer`:
```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Instant;

// The archive job's side of contracts/stream/s7comm-detector-prediction-v1.json.
public final class S7commDetectorPredictionDeserializer implements Deserializer<S7commDetectorPrediction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public S7commDetectorPrediction deserialize(String topic, byte[] data) {
        try {
            JsonNode n = objectMapper.readTree(data);
            return new S7commDetectorPrediction(
                n.get("predictionId").asText(), n.get("eventId").asText(),
                Instant.parse(n.get("eventTime").asText()), new SensorId(n.get("sensor").asText()),
                n.get("connectionUid").asText(), n.get("clientIp").asText(), n.get("serverIp").asText(),
                n.get("modelName").asText(), n.get("modelVersion").asText(), n.get("modelSha").asText(),
                n.get("schemaId").asText(), n.get("schemaHash").asText(),
                DetectorVerdict.valueOf(n.get("verdict").asText()),
                nullableFloat(n.get("score")), nullableDouble(n.get("pValue")),
                S7commScoreGroup.valueOf(n.get("scoreGroup").asText()), n.get("alpha").asDouble(),
                n.get("eventsSinceReset").asLong(), n.get("qualityFlags").asInt(),
                n.get("inferenceMicros").asLong(), Instant.parse(n.get("producedAt").asText()));
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to deserialize S7commDetectorPrediction from topic " + topic, e);
        }
    }

    // A score or p-value is null when the detector did not judge the event.
    private static Float nullableFloat(JsonNode node) {
        return node == null || node.isNull() ? null : (float) node.asDouble();
    }

    private static Double nullableDouble(JsonNode node) {
        return node == null || node.isNull() ? null : node.asDouble();
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `S7commDetectorPredictionSerializerTest` 2, `StreamContractDriftTest` 6; `Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add contracts/stream/s7comm-detector-prediction-v1.json \
  modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/S7commDetectorPredictionSerializer.java \
  modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/S7commDetectorPredictionDeserializer.java \
  modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/S7commDetectorPredictionSerializerTest.java \
  modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/StreamContractDriftTest.java
git commit -m "feat(s7comm): the detector prediction stream contract and serializer

<attribution lines>"
```

---

### Task 11: The ClickHouse table, row and mapper

**Files:**
- Create: `infrastructure/clickhouse/ddl/004_s7comm_detector_predictions.sql`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/S7commDetectorPredictionRow.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/S7commDetectorPredictionRowMapper.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/S7commDetectorPredictionRowMapperTest.java`
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/SchemaDriftTest.java` (one test)
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlDirectoryTest.java` (the expected file list)

**Interfaces:**
- Consumes: `S7commDetectorPrediction` (Task 5); `ClickHouseTimestamps.format(Instant)` (existing).
- Produces: `S7commDetectorPredictionRow` (21 components) and `S7commDetectorPredictionRowMapper.toRow(S7commDetectorPrediction)`.

- [ ] **Step 1: Write the failing tests**

`S7commDetectorPredictionRowMapperTest`:
```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// A prediction becomes one row: ClickHouse timestamps, enum names, nulls kept.
class S7commDetectorPredictionRowMapperTest {

    @Test
    void aWarmupMapsWithANullScoreAndPValue() {
        S7commDetectorPrediction p = new S7commDetectorPrediction("a".repeat(64), "s:C1:25:REQUEST:1",
            Instant.parse("2026-09-28T12:00:00.123Z"), new SensorId("s"), "C1", "10.0.0.5", "10.0.0.9",
            "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1", "c".repeat(64),
            DetectorVerdict.WARMUP, null, null, S7commScoreGroup.WRITE_REQUEST, 0.015625, 7, 16, 0L,
            Instant.parse("2026-09-28T12:00:00.456Z"));
        S7commDetectorPredictionRow row = new S7commDetectorPredictionRowMapper().toRow(p);
        assertEquals("2026-09-28 12:00:00.123", row.eventTime());
        assertEquals("2026-09-28 12:00:00.456", row.rowVersion(), "row_version is producedAt");
        assertEquals("WARMUP", row.verdict());
        assertEquals("WRITE_REQUEST", row.scoreGroup());
        assertNull(row.score());
        assertNull(row.pValue());
        assertEquals(0.015625, row.alpha());
        assertEquals(7, row.eventsSinceReset());
        assertEquals("10.0.0.5", row.clientIp());
    }
}
```
Append to `SchemaDriftTest` (and import `S7commDetectorPredictionRow`):
```java
    @Test
    void s7commDetectorPredictionRowPropertiesMatchDdlColumns() throws Exception {
        assertJsonPropertiesAreDdlColumns(S7commDetectorPredictionRow.class, "s7comm_detector_predictions");
    }
```
In `DdlDirectoryTest.ddlFilesAreReturnedInLexicalOrder`, change the expected list to:
```java
        assertEquals(List.of("001_mvp_tables.sql", "002_add_invalid_events_log_type.sql",
                "003_modbus_detector_predictions.sql", "004_s7comm_detector_predictions.sql"), names,
            "DDL files must be applied in lexical order");
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-clickhouse -am -Dtest='S7commDetectorPredictionRowMapperTest,SchemaDriftTest,DdlDirectoryTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commDetectorPredictionRow`.

- [ ] **Step 3: Implement**

`infrastructure/clickhouse/ddl/004_s7comm_detector_predictions.sql`:
```sql
-- S7comm Stage 1 detector predictions (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md,
-- section 6): one row per S7comm event, from netsec.s7comm.prediction.v1. The
-- Modbus prediction table's rules -- ReplacingMergeTree, the same ORDER BY and
-- 180-day TTL -- so a replayed prediction collapses into one row. score and
-- p_value are null unless the verdict is NORMAL or ANOMALY. Idempotent.
CREATE TABLE IF NOT EXISTS s7comm_detector_predictions (
  prediction_id      FixedString(64),
  event_id           String,
  event_time         DateTime64(3, 'UTC'),
  sensor             LowCardinality(String),
  connection_uid     String,
  client_ip          String,
  server_ip          String,
  model_name         LowCardinality(String),
  model_version      LowCardinality(String),
  model_sha          FixedString(64),
  schema_id          LowCardinality(String),
  schema_hash        FixedString(64),
  verdict            LowCardinality(String),
  score              Nullable(Float32),
  p_value            Nullable(Float64),
  score_group        LowCardinality(String),
  alpha              Float64,
  events_since_reset UInt64,
  quality_flags      UInt32,
  inference_us       UInt32,
  created_at         DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version        DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (model_name, model_version, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY;
```

`S7commDetectorPredictionRow`:
```java
package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;

// One s7comm_detector_predictions row, column for column (created_at is the
// server's default). SchemaDriftTest pins these names to the DDL.
public record S7commDetectorPredictionRow(
    @JsonProperty("prediction_id") String predictionId,
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("sensor") String sensor,
    @JsonProperty("connection_uid") String connectionUid,
    @JsonProperty("client_ip") String clientIp,
    @JsonProperty("server_ip") String serverIp,
    @JsonProperty("model_name") String modelName,
    @JsonProperty("model_version") String modelVersion,
    @JsonProperty("model_sha") String modelSha,
    @JsonProperty("schema_id") String schemaId,
    @JsonProperty("schema_hash") String schemaHash,
    @JsonProperty("verdict") String verdict,
    @JsonProperty("score") Float score,
    @JsonProperty("p_value") Double pValue,
    @JsonProperty("score_group") String scoreGroup,
    @JsonProperty("alpha") double alpha,
    @JsonProperty("events_since_reset") long eventsSinceReset,
    @JsonProperty("quality_flags") int qualityFlags,
    @JsonProperty("inference_us") long inferenceUs,
    @JsonProperty("row_version") String rowVersion) {
}
```

`S7commDetectorPredictionRowMapper`:
```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;

import java.io.Serializable;

// A prediction as one row. row_version is producedAt, as for the Modbus
// predictions, so a replay's later copy wins the ReplacingMergeTree merge.
public final class S7commDetectorPredictionRowMapper implements Serializable {

    public S7commDetectorPredictionRow toRow(S7commDetectorPrediction p) {
        return new S7commDetectorPredictionRow(
            p.predictionId(), p.eventId(), ClickHouseTimestamps.format(p.eventTime()), p.sensor().value(),
            p.connectionUid(), p.clientIp(), p.serverIp(), p.modelName(), p.modelVersion(), p.modelSha(),
            p.schemaId(), p.schemaHash(), p.verdict().name(), p.score(), p.pValue(), p.scoreGroup().name(),
            p.alpha(), p.eventsSinceReset(), p.qualityFlags(), p.inferenceMicros(),
            ClickHouseTimestamps.format(p.producedAt()));
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `S7commDetectorPredictionRowMapperTest` 1, `SchemaDriftTest` 4, `DdlDirectoryTest` 3; `Failures: 0, Errors: 0`. `DdlMigrationTest`, the container test that would apply `004` to a real ClickHouse, is compiled but not run on this machine. The live `deploy.sh restart` in Task 19 applies it.

- [ ] **Step 5: Commit**

```bash
git add infrastructure/clickhouse/ddl/004_s7comm_detector_predictions.sql \
  modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/S7commDetectorPredictionRow.java \
  modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/S7commDetectorPredictionRowMapper.java \
  modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/S7commDetectorPredictionRowMapperTest.java \
  modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/SchemaDriftTest.java \
  modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlDirectoryTest.java
git commit -m "feat(s7comm): the s7comm_detector_predictions table, row and mapper

<attribution lines>"
```

---

### Task 12: The Flink side output and the scoring operator

**Files:**
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/KeyedS7commVector.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/KeyedS7commVectorKeySelector.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/S7commScoringProcessFunction.java`
- Modify: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/S7commFeatureProcessFunction.java`
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/S7commFeatureProcessFunctionTest.java` (two tests)
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/S7commScoringProcessFunctionTest.java`

**Interfaces:**
- Consumes: `ScoreS7commSequenceUseCase`, `S7commScoringResult`, `ReconstructionScorer(Factory)` (Task 6); `S7commScoreWindow`, `S7commDetectorPrediction` (Task 5).
- Produces:
  - `record KeyedS7commVector(S7commConnectionKey key, FeatureVector vector, boolean freshState, String clientIp, String serverIp)`;
  - `S7commFeatureProcessFunction.SCORING_TAG` (`OutputTag<KeyedS7commVector>`, id `s7comm-scoring`);
  - `KeyedS7commVectorKeySelector`;
  - `S7commScoringProcessFunction(ReconstructionScorerFactory, Duration)` and `static disabled(Duration)`, with state `s7comm-score-window`.

- [ ] **Step 1: Write the failing tests**

Append to `S7commFeatureProcessFunctionTest`, and add the imports `java.util.List` (if not present), `static org.junit.jupiter.api.Assertions.assertFalse` and `assertTrue`:
```java
    // What the operator put on the scoring side output, in order.
    private static List<KeyedS7commVector> scoring(OneInputStreamOperatorTestHarness<S7commEvent, FeatureVector> harness) {
        return harness.getSideOutput(S7commFeatureProcessFunction.SCORING_TAG).stream()
            .map(StreamRecord::getValue).toList();
    }

    // Scoring design section 4: every vector also reaches s7comm-score, with its
    // connection, whether the connection started from empty state, and its
    // client and server whichever way the record went.
    @Test
    void everyVectorAlsoGoesToScoringWithItsConnection() throws Exception {
        var harness = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        harness.processElement(new StreamRecord<>(response("CA", 1000.1, 1)));
        List<KeyedS7commVector> side = scoring(harness);
        assertEquals(2, side.size());
        assertTrue(side.get(0).freshState(), "the connection's first event");
        assertFalse(side.get(1).freshState());
        for (KeyedS7commVector k : side) {
            assertEquals("CA", k.key().uid());
            assertEquals("10.0.0.5", k.clientIp(), "the request's sender is the response's receiver");
            assertEquals("10.0.0.9", k.serverIp());
        }
        assertEquals(harness.extractOutputValues().stream().map(FeatureVector::eventId).toList(),
            side.stream().map(k -> k.vector().eventId()).toList());
        harness.close();
    }

    // Review Focus 2: an expired feature state is a fresh connection to the scorer too.
    @Test
    void aConnectionWhoseStateExpiredIsFreshAgain() throws Exception {
        var harness = harness(Duration.ofMinutes(60));
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        harness.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        harness.processElement(new StreamRecord<>(request("CA", 1001.0, 2)));
        assertTrue(scoring(harness).get(1).freshState(), "the TTL expired the state: the scorer must re-warm");
        harness.close();
    }
```

`S7commScoringProcessFunctionTest`:
```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;
import io.netsecml.platform.port.out.ReconstructionScorer;
import io.netsecml.platform.port.out.ReconstructionScorerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The scoring operator in a Flink harness, with a stub detector: warm-up, a
// fresh connection, the idle TTL, a restore mid-window giving exactly an
// uninterrupted run's predictions, and scoring switched off and on.
class S7commScoringProcessFunctionTest {

    private static final S7commConnectionKey KEY = new S7commConnectionKey(new SensorId("s"), "C1");

    // Identity continuous transforms and a one-score policy: the stub decides the score.
    static S7commDetectorBundle bundle() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("s" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        S7commPreprocessing p = new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"),
            List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            scores.put(g, new double[]{0.5});
        }
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64),
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, 16, 21, "input",
            "reconstruction", p, p.scoreWeights(List.of("s7_operation")),
            new S7commConformalPolicy(scores, Map.of(), 0.001));
    }

    // Serializable, as the operator requires; builds its stub on the
    // "TaskManager". The stub adds the OLDEST row's feature 0 to the last row's,
    // so each score depends on the whole window: a window restored wrongly
    // scores differently.
    static final class StubFactory implements ReconstructionScorerFactory {
        @Override
        public ReconstructionScorer create() {
            S7commDetectorBundle bundle = bundle();
            return new ReconstructionScorer() {
                @Override
                public S7commDetectorBundle bundle() {
                    return bundle;
                }

                @Override
                public float[] reconstructLast(float[][] window) {
                    float[] row = window[window.length - 1].clone();
                    row[0] += window[0][0];
                    return row;
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static OneInputStreamOperatorTestHarness<KeyedS7commVector, S7commDetectorPrediction> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new S7commScoringProcessFunction(new StubFactory(), Duration.ofHours(1))),
            new KeyedS7commVectorKeySelector(), TypeInformation.of(S7commConnectionKey.class));
    }

    private static OneInputStreamOperatorTestHarness<KeyedS7commVector, S7commDetectorPrediction> disabledHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(S7commScoringProcessFunction.disabled(Duration.ofHours(1))),
            new KeyedS7commVectorKeySelector(), TypeInformation.of(S7commConnectionKey.class));
    }

    // Event i: a response whose feature 0 is i / 100.
    private static KeyedS7commVector event(int i, boolean fresh) {
        float[] v = new float[16];
        v[0] = i / 100f;
        v[14] = 3f;
        v[15] = 4f;
        return new KeyedS7commVector(KEY, new FeatureVector("s:C1:" + i + ":RESPONSE:" + i,
            Instant.ofEpochSecond(1000 + i), new SensorId("s"), LogType.S7COMM, "C1",
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, v, 0,
            Instant.ofEpochSecond(1000 + i)), fresh, "10.0.0.5", "10.0.0.9");
    }

    @Test
    void itWarmsUpThenScores() throws Exception {
        var harness = harness();
        harness.open();
        for (int i = 0; i < 17; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        List<S7commDetectorPrediction> out = harness.extractOutputValues();
        assertEquals(17, out.size());
        assertEquals(DetectorVerdict.WARMUP, out.get(14).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(15).verdict());
        assertEquals(0f, out.get(15).score(), "the oldest row is event 0: 0^2 / 17");
        assertEquals(0.0001f / 17f, out.get(16).score(), 1e-9f, "the oldest row is event 1: 0.01^2 / 17");
        harness.close();
    }

    @Test
    void aFreshConnectionRewarms() throws Exception {
        var harness = harness();
        harness.open();
        for (int i = 0; i < 16; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        harness.processElement(new StreamRecord<>(event(16, true)));
        S7commDetectorPrediction last = harness.extractOutputValues().get(16);
        assertEquals(DetectorVerdict.WARMUP, last.verdict());
        assertEquals(1, last.eventsSinceReset());
        harness.close();
    }

    @Test
    void anIdleWindowExpiresAfterTheTtl() throws Exception {
        var harness = harness();
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        for (int i = 0; i < 15; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        harness.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        harness.processElement(new StreamRecord<>(event(15, false)));
        S7commDetectorPrediction last = harness.extractOutputValues().get(15);
        assertEquals(DetectorVerdict.WARMUP, last.verdict(), "the 15 earlier rows expired");
        assertEquals(1, last.eventsSinceReset());
        harness.close();
    }

    @Test
    void aRestoreMidWindowGivesTheUninterruptedPredictions() throws Exception {
        var uninterrupted = harness();
        uninterrupted.open();
        for (int i = 0; i < 25; i++) {
            uninterrupted.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        List<S7commDetectorPrediction> expected = uninterrupted.extractOutputValues();
        uninterrupted.close();

        var first = harness();
        first.open();
        for (int i = 0; i < 12; i++) {
            first.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
        first.close();

        var second = harness();
        second.initializeState(snapshot);
        second.open();
        for (int i = 12; i < 25; i++) {
            second.processElement(new StreamRecord<>(event(i, false)));
        }
        List<S7commDetectorPrediction> actual = second.extractOutputValues();
        second.close();

        for (int i = 0; i < actual.size(); i++) {
            S7commDetectorPrediction e = expected.get(12 + i);
            assertEquals(e.verdict(), actual.get(i).verdict(), "event " + (12 + i));
            assertEquals(e.eventsSinceReset(), actual.get(i).eventsSinceReset(), "event " + (12 + i));
            assertEquals(e.score(), actual.get(i).score(), "event " + (12 + i));
        }
    }

    // Scoring off (an empty S7COMM_DETECTOR_BUNDLE): the operator stays in the
    // job, so no savepoint state is orphaned, but it emits nothing.
    @Test
    void aDisabledOperatorEmitsNothing() throws Exception {
        var harness = disabledHarness();
        harness.open();
        for (int i = 0; i < 20; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        assertEquals(0, harness.extractOutputValues().size());
        harness.close();
    }

    // On -> off -> on through savepoints: each restore succeeds, and the window
    // a connection held before scoring was switched off is gone, so the
    // detector never reads vectors from both sides of the gap as consecutive.
    @Test
    void scoringSwitchedOffAndOnAgainRestoresAndRewarms() throws Exception {
        var on = harness();
        on.open();
        for (int i = 0; i < 12; i++) {
            on.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        OperatorSubtaskState whileOn = on.snapshot(1L, 1L);
        on.close();

        var off = disabledHarness();
        off.initializeState(whileOn);
        off.open();
        for (int i = 12; i < 18; i++) {
            off.processElement(new StreamRecord<>(event(i, false)));
        }
        assertEquals(0, off.extractOutputValues().size());
        OperatorSubtaskState whileOff = off.snapshot(2L, 2L);
        off.close();

        var again = harness();
        again.initializeState(whileOff);
        again.open();
        for (int i = 18; i < 34; i++) {
            again.processElement(new StreamRecord<>(event(i, false)));
        }
        List<S7commDetectorPrediction> out = again.extractOutputValues();
        assertEquals(1, out.get(0).eventsSinceReset(), "the window held before scoring was off is gone");
        assertEquals(DetectorVerdict.WARMUP, out.get(14).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(15).verdict());
        again.close();
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-flink -am -Dtest='S7commFeatureProcessFunctionTest,S7commScoringProcessFunctionTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class KeyedS7commVector`.

- [ ] **Step 3: Implement**

`KeyedS7commVector`:
```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commConnectionKey;

// An S7comm feature vector as s7comm-features hands it to s7comm-score
// (scoring design section 4): its connection key, whether the connection
// started from empty state on this event, and the connection's endpoints --
// none of which the vector itself carries.
public record KeyedS7commVector(S7commConnectionKey key, FeatureVector vector, boolean freshState, String clientIp,
                                String serverIp) {
}
```

`KeyedS7commVectorKeySelector`:
```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.S7commConnectionKey;
import org.apache.flink.api.java.functions.KeySelector;

// s7comm-score is keyed by the same connection key as s7comm-features.
public final class KeyedS7commVectorKeySelector implements KeySelector<KeyedS7commVector, S7commConnectionKey> {
    @Override
    public S7commConnectionKey getKey(KeyedS7commVector value) {
        return value.key();
    }
}
```

In `S7commFeatureProcessFunction`, add the import `org.apache.flink.util.OutputTag` and this field below `DEFAULT_STATE_TTL`:
```java
    // What s7comm-score reads (scoring design section 4): each vector with its
    // connection key, whether the connection started from empty state, and
    // its endpoints. An anonymous subclass so Flink keeps the element type.
    public static final OutputTag<KeyedS7commVector> SCORING_TAG = new OutputTag<>("s7comm-scoring") {};
```
Then replace `processElement`'s body with:
```java
        // Read through value() on every call, never cached: on the heap backend
        // that is what makes in-place mutation safe while a checkpoint runs.
        S7commConnectionState state = connectionState.value();
        // Empty state -- a new connection, a TTL expiry, or a restore without
        // state -- is where the scorer's window must start again (spec section 5).
        boolean freshState = state == null;
        if (freshState) {
            state = S7commConnectionState.empty();
        }

        // build() advances the state in place and returns it; update() is still
        // required -- it stores a new key's state, keeps RocksDB/ForSt correct,
        // and is the write that refreshes the TTL.
        FeatureBuildResult<S7commConnectionState> result = useCase.build(event, state);
        connectionState.update(result.newState());
        out.collect(result.vector());

        // The client sends to port 102, as the feature engine's direction rule
        // says; a response's endpoints are the other way round.
        String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
        String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
        ctx.output(SCORING_TAG, new KeyedS7commVector(ctx.getCurrentKey(), result.vector(), freshState, client, server));
```

`S7commScoringProcessFunction`:
```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.S7commScoringResult;
import io.netsecml.platform.application.usecase.ScoreS7commSequenceUseCase;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.port.out.ReconstructionScorer;
import io.netsecml.platform.port.out.ReconstructionScorerFactory;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

// s7comm-score (scoring design section 4): holds each connection's window of
// preprocessed vectors and emits one prediction per S7comm event. The scorer
// is created per subtask in open() and closed in close().
//
// disabled() is the same operator with scoring switched off (an empty
// S7COMM_DETECTOR_BUNDLE): it keeps its uid and its state descriptor in the
// job, so a savepoint taken while scoring ran restores without orphaned state,
// but it loads no model, emits nothing and clears each connection's window as
// its events pass -- so switching scoring back on re-warms every connection.
public final class S7commScoringProcessFunction
        extends KeyedProcessFunction<S7commConnectionKey, KeyedS7commVector, S7commDetectorPrediction> {

    // null only when disabled.
    private final ReconstructionScorerFactory factory;
    private final Duration stateTtl;

    private transient ValueState<S7commScoreWindow> windowState;
    private transient ReconstructionScorer scorer;
    private transient ScoreS7commSequenceUseCase useCase;
    private transient Counter unscorable;

    public S7commScoringProcessFunction(ReconstructionScorerFactory factory, Duration stateTtl) {
        this(factory, stateTtl, true);
    }

    // Scoring switched off: the operator and its state stay, nothing is scored.
    public static S7commScoringProcessFunction disabled(Duration stateTtl) {
        return new S7commScoringProcessFunction(null, stateTtl, false);
    }

    private S7commScoringProcessFunction(ReconstructionScorerFactory factory, Duration stateTtl, boolean enabled) {
        this.factory = enabled ? Objects.requireNonNull(factory, "factory") : null;
        Objects.requireNonNull(stateTtl, "stateTtl");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) {
        // "s7comm-score-window": a state name is checkpoint identity; never
        // rename it. The same idle TTL as the feature state (spec section 5).
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();
        ValueStateDescriptor<S7commScoreWindow> descriptor = new ValueStateDescriptor<>(
            "s7comm-score-window", TypeInformation.of(S7commScoreWindow.class));
        descriptor.enableTimeToLive(ttl);
        windowState = getRuntimeContext().getState(descriptor);
        // Disabled: no model, no use case -- the state above is all it keeps.
        if (factory == null) {
            return;
        }
        // The model is loaded here, once per subtask; a bad bundle fails the job.
        scorer = factory.create();
        useCase = new ScoreS7commSequenceUseCase(scorer, Clock.systemUTC());
        unscorable = getRuntimeContext().getMetricGroup().counter("unscorable");
    }

    @Override
    public void processElement(KeyedS7commVector in, Context ctx, Collector<S7commDetectorPrediction> out)
            throws Exception {
        // Disabled: drop this connection's window, emit nothing.
        if (factory == null) {
            windowState.clear();
            return;
        }
        // Read through value() on every call, never cached, as the feature state is.
        S7commScoringResult result = useCase.score(in.vector(), in.freshState(), in.clientIp(), in.serverIp(),
            windowState.value());
        windowState.update(result.window());
        if (result.prediction().verdict() == DetectorVerdict.UNSCORABLE) {
            unscorable.inc();
        }
        out.collect(result.prediction());
    }

    @Override
    public void close() throws Exception {
        if (scorer != null) {
            scorer.close();
        }
        super.close();
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `S7commFeatureProcessFunctionTest` 8, `S7commScoringProcessFunctionTest` 6; `Failures: 0, Errors: 0`.

- [ ] **Step 5: The rest of the module still passes**

Run: `./mvnw test -pl modules/adapter-flink -am -Dtest='S7comm*Test,Modbus*Test' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: every class passes, `S7commUpstreamOracleTest` 2/2 included.

- [ ] **Step 6: Commit**

```bash
git add modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/KeyedS7commVector.java \
  modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/KeyedS7commVectorKeySelector.java \
  modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/S7commScoringProcessFunction.java \
  modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/S7commFeatureProcessFunction.java \
  modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/S7commFeatureProcessFunctionTest.java \
  modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/S7commScoringProcessFunctionTest.java
git commit -m "feat(s7comm): the scoring side output and the s7comm-score operator

<attribution lines>"
```

---

### Task 13: Wire scoring into the online job

**Files:**
- Create: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/S7commDetectorScorerFactory.java`
- Modify: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/S7commDetectorScorerFactoryTest.java`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobTopologyTest.java` (two tests)

**Interfaces:**
- Consumes: Tasks 8, 9, 10, 12.
- Produces:
  - `OnlineFeatureJob.S7commScoring(String bundleDir, String predictionTopic)`;
  - `static S7commScoring s7commScoring(Map<String,String> env)`;
  - the eleven-argument `build(env, bootstrapServers, conn, dns, modbus, s7comm, sensor, Duration modbusStateTtl, Duration s7commStateTtl, ModbusScoring modbusScoring, S7commScoring s7commScoring)`, which `main()` calls;
  - `S7commDetectorScorerFactory(String bundleDir) implements ReconstructionScorerFactory`.

- [ ] **Step 1: Write the failing tests**

`S7commDetectorScorerFactoryTest`:
```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.port.out.ReconstructionScorer;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The composition root's factory: the real bundle, loaded and opened,
// reconstructs the all-zero window as Python does (OnnxReconstructionScorerTest's numbers).
class S7commDetectorScorerFactoryTest {

    private static final String FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v1").toString();

    @Test
    void itLoadsTheBundleAndReconstructs() {
        try (ReconstructionScorer scorer = new S7commDetectorScorerFactory(FIXTURE).create()) {
            assertEquals(-0.47339922189712524f, scorer.reconstructLast(new float[16][21])[0], 1e-5f);
            assertEquals("s7comm-stage1-detector/v1", scorer.bundle().bundleId());
        }
    }

    @Test
    void aMissingBundleFailsLoudly() {
        assertThrows(IllegalStateException.class, () -> new S7commDetectorScorerFactory("/no/such/bundle").create());
    }
}
```
Append to `OnlineFeatureJobTopologyTest`:
```java
    // S7comm scoring adds exactly three uids -- s7comm-score,
    // s7comm-prediction-sink and the committer Flink's Sink V2 derives from the
    // sink's uid -- beside Modbus scoring, and changes no existing uid.
    @Test
    void s7commScoringAddsItsTwoOperatorsBesideModbusScoring() {
        OnlineFeatureJob.ModbusScoring modbus = new OnlineFeatureJob.ModbusScoring(
            "/opt/netsec/models/modbus-stage1-detector/v1", "netsec.modbus.prediction.v1");
        Set<String> withS7 = uidsOf(buildWithBoth(modbus, new OnlineFeatureJob.S7commScoring(
            "/opt/netsec/models/s7comm-stage1-detector/v1", "netsec.s7comm.prediction.v1")));
        Set<String> without = uidsOf(buildWith(modbus));
        assertEquals(without.size() + 3, withS7.size(), "found: " + withS7);
        assertTrue(withS7.containsAll(Set.of("s7comm-score", "s7comm-prediction-sink",
            "Sink Committer: s7comm-prediction-sink")));
        assertTrue(withS7.containsAll(without), "no existing uid changes");
    }

    // An empty S7COMM_DETECTOR_BUNDLE keeps s7comm-score and its sink in the
    // job (disabled), so switching S7 scoring off never orphans savepoint state.
    @Test
    void s7commScoringOffKeepsItsOperatorsSoNoStateIsOrphaned() {
        OnlineFeatureJob.S7commScoring off = OnlineFeatureJob.s7commScoring(
            Map.of("S7COMM_DETECTOR_BUNDLE", "", "NETSEC_MODELS_DIR", "/opt/netsec/models"));
        OnlineFeatureJob.S7commScoring on = OnlineFeatureJob.s7commScoring(
            Map.of("S7COMM_DETECTOR_BUNDLE", "s7comm-stage1-detector/v1", "NETSEC_MODELS_DIR", "/opt/netsec/models"));
        assertNull(off.bundleDir(), "an empty pin scores nothing");
        assertEquals("/opt/netsec/models/s7comm-stage1-detector/v1", on.bundleDir());
        assertEquals("netsec.s7comm.prediction.v1", off.predictionTopic());
        OnlineFeatureJob.ModbusScoring modbus = OnlineFeatureJob.modbusScoring(Map.of());
        assertEquals(uidsOf(buildWithBoth(modbus, on)), uidsOf(buildWithBoth(modbus, off)));
    }

    private static StreamExecutionEnvironment buildWithBoth(OnlineFeatureJob.ModbusScoring modbus,
                                                            OnlineFeatureJob.S7commScoring s7comm) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OnlineFeatureJob.build(env, "localhost:9092",
            new OnlineFeatureJob.ProtocolTopics("conn", "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("dns", "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("netsec.modbus.raw.v1", "netsec.modbus.feature-vector.v1",
                "netsec.modbus.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("netsec.s7comm.raw.v1", "netsec.s7comm.feature-vector.v1",
                "netsec.s7comm.dlq.v1"),
            new SensorId("sensor-eu-1"), Duration.ofMinutes(60), Duration.ofMinutes(60), modbus, s7comm);
        return env;
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='S7commDetectorScorerFactoryTest,OnlineFeatureJobTopologyTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commDetectorScorerFactory` and `class S7commScoring`.

- [ ] **Step 3: Implement**

`S7commDetectorScorerFactory`:
```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.onnx.runtime.OnnxReconstructionScorer;
import io.netsecml.platform.adapter.registry.LoadedS7commDetector;
import io.netsecml.platform.adapter.registry.S7commDetectorBundleLoader;
import io.netsecml.platform.port.out.ReconstructionScorer;
import io.netsecml.platform.port.out.ReconstructionScorerFactory;

import java.io.IOException;
import java.nio.file.Path;

// The composition root's ReconstructionScorerFactory: loads and verifies the
// S7comm bundle (adapter-registry-filesystem), then opens its graph
// (adapter-onnx) -- two adapters, so it lives here. Serializable as a path
// string; runs in each subtask's open() on the TaskManager, where models/ is mounted.
public final class S7commDetectorScorerFactory implements ReconstructionScorerFactory {

    private final String bundleDir;

    public S7commDetectorScorerFactory(String bundleDir) {
        this.bundleDir = bundleDir;
    }

    @Override
    public ReconstructionScorer create() {
        try {
            LoadedS7commDetector loaded = S7commDetectorBundleLoader.load(Path.of(bundleDir));
            return new OnnxReconstructionScorer(loaded.model(), loaded.bundle());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the S7comm detector bundle at " + bundleDir, e);
        }
    }
}
```

In `OnlineFeatureJob`:

1. Add imports: `io.netsecml.platform.adapter.flink.process.KeyedS7commVectorKeySelector`, `io.netsecml.platform.adapter.flink.process.S7commScoringProcessFunction`, `io.netsecml.platform.adapter.kafka.sink.S7commDetectorPredictionSerializer`, `io.netsecml.platform.adapter.registry.S7commDetectorBundleLoader`, `io.netsecml.platform.domain.inference.S7commDetectorPrediction`.

2. After `modbusScoring(...)`, add:
```java
    // S7comm scoring's settings (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
    // section 7), shaped like ModbusScoring: a null bundleDir keeps s7comm-score
    // and its sink in the job but disabled; a null S7commScoring builds neither
    // (the overloads that predate S7 scoring).
    public record S7commScoring(String bundleDir, String predictionTopic) {
    }

    // main()'s S7 scoring settings from its environment: the pinned bundle
    // under NETSEC_MODELS_DIR, or a null bundleDir when S7COMM_DETECTOR_BUNDLE is
    // empty. Never null, so main() always builds the two S7 scoring operators.
    static S7commScoring s7commScoring(Map<String, String> env) {
        String bundle = env.getOrDefault("S7COMM_DETECTOR_BUNDLE", "");
        String bundleDir = bundle.isBlank()
            ? null
            : Path.of(env.getOrDefault("NETSEC_MODELS_DIR", "/opt/netsec/models"), bundle).toString();
        return new S7commScoring(bundleDir,
            env.getOrDefault("S7COMM_PREDICTION_TOPIC", "netsec.s7comm.prediction.v1"));
    }
```

3. Replace the ten-argument `build(...)` (the one taking `ModbusScoring scoring`) and its comment with:
```java
    // As above, with Modbus scoring and no S7 scoring; kept so every caller
    // that predates S7 scoring builds exactly the topology it always has.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers, ProtocolTopics conn,
                              ProtocolTopics dns, ProtocolTopics modbus, ProtocolTopics s7comm, SensorId sensor,
                              Duration modbusStateTtl, Duration s7commStateTtl, ModbusScoring scoring) {
        build(env, bootstrapServers, conn, dns, modbus, s7comm, sensor, modbusStateTtl, s7commStateTtl, scoring,
            null);
    }

    // As above, with S7comm scoring too; main() passes modbusScoring(env) and
    // s7commScoring(env), whose bundleDirs are null when their pins are empty.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers, ProtocolTopics conn,
                              ProtocolTopics dns, ProtocolTopics modbus, ProtocolTopics s7comm, SensorId sensor,
                              Duration modbusStateTtl, Duration s7commStateTtl, ModbusScoring modbusScoring,
                              S7commScoring s7commScoring) {
        build(env, bootstrapServers, conn, dns, sensor);
        modbusChain(env, bootstrapServers, modbus, sensor, modbusStateTtl, modbusScoring);
        s7commChain(env, bootstrapServers, s7comm, sensor, s7commStateTtl, s7commScoring);
    }
```

4. In `s7commChain`, add the parameter `S7commScoring scoring` (after `Duration stateTtl`). Change `DataStream<FeatureVector> s7commFeatureVectors` to `SingleOutputStreamOperator<FeatureVector> s7commFeatureVectors`, so its side output can be read. At the end of the method, after `sinkRejected(...)`, add:
```java
        // Scoring (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
        // section 4): the side output, keyed by the same connection key, into
        // s7comm-score, then to the prediction topic. Off when null.
        if (scoring != null) {
            // A null bundleDir: the same operator, disabled (see S7commScoring).
            S7commScoringProcessFunction scorer = scoring.bundleDir() == null
                ? S7commScoringProcessFunction.disabled(stateTtl)
                : new S7commScoringProcessFunction(new S7commDetectorScorerFactory(scoring.bundleDir()), stateTtl);
            DataStream<S7commDetectorPrediction> predictions = s7commFeatureVectors
                .getSideOutput(S7commFeatureProcessFunction.SCORING_TAG)
                .keyBy(new KeyedS7commVectorKeySelector())
                .process(scorer)
                .name("s7comm-score")
                .uid("s7comm-score");
            sinkS7commPredictions(predictions, bootstrapServers, scoring.predictionTopic(), "s7comm-prediction-sink");
        }
```

5. After `sinkModbusPredictions(...)`, add:
```java
    // The S7comm prediction topic: s7comm-detector-prediction-v1, one message per S7comm event.
    private static void sinkS7commPredictions(DataStream<S7commDetectorPrediction> predictions,
                                              String bootstrapServers, String topic, String uid) {
        S7commDetectorPredictionSerializer serializer = new S7commDetectorPredictionSerializer();
        KafkaSink<S7commDetectorPrediction> sink = KafkaSink.<S7commDetectorPrediction>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<S7commDetectorPrediction>builder()
                .setTopic(topic)
                // An anonymous class, not a lambda, so Flink keeps the generic type.
                .setValueSerializationSchema(new SerializationSchema<S7commDetectorPrediction>() {
                    @Override
                    public byte[] serialize(S7commDetectorPrediction prediction) {
                        return serializer.serialize(topic, prediction);
                    }
                })
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        predictions.sinkTo(sink).name(uid).uid(uid);
    }
```

6. In `main()`, replace the lines from `ModbusScoring scoring = modbusScoring(System.getenv());` through the `build(...)` call with:
```java
        ModbusScoring scoring = modbusScoring(System.getenv());
        if (scoring.bundleDir() != null) {
            SequenceDetectorBundleLoader.load(Path.of(scoring.bundleDir()));
        }
        // S7comm scoring (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
        // section 7), verified the same way before submission.
        S7commScoring s7commScoring = s7commScoring(System.getenv());
        if (s7commScoring.bundleDir() != null) {
            S7commDetectorBundleLoader.load(Path.of(s7commScoring.bundleDir()));
        }

        build(env, bootstrapServers, conn, dns, modbus, s7comm, new SensorId(sensorId), modbusStateTtl,
            s7commStateTtl, scoring, s7commScoring);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `S7commDetectorScorerFactoryTest` 2, `OnlineFeatureJobTopologyTest` 12; `Failures: 0, Errors: 0`.

- [ ] **Step 5: The module's other non-container tests still pass**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='OnlineFeatureJobTopologyTest,ZeekRecordCheckTest,ModbusDetectorScorerFactoryTest,OnlineFeatureJobRestartStrategyTest,ModbusDetectorOracleTest,S7commDetectorScorerFactoryTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: every class passes (12, 8, 2, 2, 1, 2).

- [ ] **Step 6: Commit**

```bash
git add modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/S7commDetectorScorerFactory.java \
  modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java \
  modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/S7commDetectorScorerFactoryTest.java \
  modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobTopologyTest.java
git commit -m "feat(s7comm): wire S7 scoring into the online job

<attribution lines>"
```

---

### Task 14: The archive job's tenth chain

**Files:**
- Create: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/S7commDetectorPredictionRowMapFunction.java`
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Test: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java` (one test)

**Interfaces:**
- Consumes: Tasks 10-11.
- Produces:
  - `ArchiveJob.s7commPredictionChain(String topic)`, with the uids `s7comm-prediction-source`, `s7comm-prediction-row` and `s7comm-predictions-clickhouse-sink`;
  - `ArchiveJob.connDnsModbusS7commAndBothPredictionChains(10 topics)`, which `main()` calls.

- [ ] **Step 1: Write the failing test**

Append to `ArchiveJobTopologyTest`:
```java
    // The ten chains main() wires: the nine of
    // connDnsModbusS7commAndModbusPredictionChains plus S7comm predictions,
    // whose three uids are new and distinct.
    @Test
    void tenChainsProduceThirtyDistinctUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", ArchiveJob.connDnsModbusS7commAndBothPredictionChains(
                "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
                "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1",
                "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1",
                "netsec.s7comm.feature-vector.v1", "netsec.s7comm.dlq.v1",
                "netsec.modbus.prediction.v1", "netsec.s7comm.prediction.v1"),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));
        Set<String> uids = new HashSet<>();
        for (StreamNode node : env.getStreamGraph(false).getStreamNodes()) {
            assertTrue(uids.add(node.getTransformationUID()), "duplicate uid " + node.getTransformationUID());
        }
        assertEquals(30, uids.size());
        assertTrue(uids.containsAll(Set.of("s7comm-prediction-source", "s7comm-prediction-row",
            "s7comm-predictions-clickhouse-sink", "modbus-prediction-source")), "found: " + uids);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw test -pl modules/bootstrap-archive-job -am -Dtest='ArchiveJobTopologyTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: method connDnsModbusS7commAndBothPredictionChains`.

- [ ] **Step 3: Implement**

`S7commDetectorPredictionRowMapFunction`:
```java
package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.S7commDetectorPredictionRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow;
import io.netsecml.platform.adapter.kafka.sink.S7commDetectorPredictionDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// One prediction message from netsec.s7comm.prediction.v1 as one
// s7comm_detector_predictions row, as ModbusDetectorPredictionRowMapFunction does.
public final class S7commDetectorPredictionRowMapFunction
        extends RichMapFunction<byte[], S7commDetectorPredictionRow> {

    private final String topic;
    private transient S7commDetectorPredictionDeserializer deserializer;
    private transient S7commDetectorPredictionRowMapper mapper;

    public S7commDetectorPredictionRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new S7commDetectorPredictionDeserializer();
        mapper = new S7commDetectorPredictionRowMapper();
    }

    @Override
    public S7commDetectorPredictionRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
```

In `ArchiveJob`, add the import `io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow`. After `connDnsModbusS7commAndModbusPredictionChains(...)`, add:
```java
    // S7comm Stage 1 predictions (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
    // section 6), their own three uids.
    public static LogTypeChain<S7commDetectorPredictionRow> s7commPredictionChain(String topic) {
        return new LogTypeChain<>(topic, new S7commDetectorPredictionRowMapFunction(topic),
            "s7comm_detector_predictions", "s7comm-prediction-source", "s7comm-prediction-row",
            "s7comm-predictions-clickhouse-sink");
    }

    // The ten chains main() wires: the nine above plus S7comm predictions. A new
    // method, as every protocol before it paid (CLAUDE.md, the multi-protocol seams).
    public static List<LogTypeChain<?>> connDnsModbusS7commAndBothPredictionChains(
            String connFeatureTopic, String connDlqTopic, String dnsFeatureTopic, String dnsDlqTopic,
            String modbusFeatureTopic, String modbusDlqTopic, String s7commFeatureTopic, String s7commDlqTopic,
            String modbusPredictionTopic, String s7commPredictionTopic) {
        List<LogTypeChain<?>> chains = new ArrayList<>(connDnsModbusS7commAndModbusPredictionChains(connFeatureTopic,
            connDlqTopic, dnsFeatureTopic, dnsDlqTopic, modbusFeatureTopic, modbusDlqTopic, s7commFeatureTopic,
            s7commDlqTopic, modbusPredictionTopic));
        chains.add(s7commPredictionChain(s7commPredictionTopic));
        return List.copyOf(chains);
    }
```
In `main()`, after the `modbusPredictionTopic` line, add:
```java
        // S7comm Stage 1 predictions; the topic must exist even when S7 scoring is off.
        String s7commPredictionTopic = System.getenv().getOrDefault("S7COMM_PREDICTION_TOPIC",
            "netsec.s7comm.prediction.v1");
```
Then replace the `build(...)` call and its comment with:
```java
        // Ten chains through connDnsModbusS7commAndBothPredictionChains() and the
        // parameterised, list-form build(): conn's two (unchanged uids), then
        // dns's, modbus's and s7comm's two each, then Modbus and S7comm
        // predictions. The nine-, eight-, six- and four-chain methods stay public
        // for the tests that call them directly. ArchiveJobTopologyTest's
        // ten-chain case builds through the same method.
        build(env, bootstrapServers,
            connDnsModbusS7commAndBothPredictionChains(featureTopic, dlqTopic, dnsFeatureTopic, dnsDlqTopic,
                modbusFeatureTopic, modbusDlqTopic, s7commFeatureTopic, s7commDlqTopic, modbusPredictionTopic,
                s7commPredictionTopic),
            clickHouse);
```

- [ ] **Step 4: Run the test to verify it passes**

Run: the Step 2 command.
Expected: `Tests run: 12, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/S7commDetectorPredictionRowMapFunction.java \
  modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java \
  modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java
git commit -m "feat(s7comm): the archive job's tenth chain, S7comm predictions

<attribution lines>"
```

---
### Task 15: The scoring oracle

**Files:**
- Create: `tests/fixtures/s7comm/generate_detector_oracle.py`
- Create (generated): `tests/fixtures/s7comm/detector_oracle_v1.jsonl`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/S7commDetectorOracleTest.java`

**Interfaces:**
- Consumes: everything above; the delivery `models/S7/`; `$SP/venv` (Task 1).
- Produces: the oracle fixture. The generator's `Reference` class and its `record`, `pipelined`, `READ` helpers are reused by Task 19's live check.

- [ ] **Step 1: Write the generator**

`tests/fixtures/s7comm/generate_detector_oracle.py`:
```python
#!/usr/bin/env python3
"""Generate the S7comm Stage 1 scoring oracle by running the model team's own code.

Builds deterministic S7 traffic exactly as icsnpp-s7comm writes it (both
endpoint pairs, hex-string function codes). Each record is handed to upstream's
own feature builder with spec section 2.1's S2 applied (a USERDATA PDU's
function code read as 0x00). The generator then runs the delivered fitted
preprocessor (preprocessor.joblib, with S1: the operation upper-cased), the
delivered ONNX graph in Python ONNX Runtime, and upstream's own
operation_groups and conformal_pvalues, exec'd from debiased.py with its torch
import skipped. The features handed to the preprocessor are first rounded to
float32, exactly as the Java vector carries them.

Refuses to run unless every delivery file's SHA-256 matches the one this
fixture was designed against. Refuses to write a fixture that misses a verdict
it must cover, or that holds a verdict which could flip within
S7commDetectorOracleTest's score tolerance (plan ruling P3).

Usage (from the repository root, in a venv with numpy, pandas, scikit-learn,
joblib and onnxruntime):
    python tests/fixtures/s7comm/generate_detector_oracle.py \
        --delivery models/S7 --out tests/fixtures/s7comm/detector_oracle_v1.jsonl
The output is deterministic.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

import joblib
import numpy as np
import onnxruntime as ort
import pandas as pd

RELEASE = "models/stage1_anomaly/v4_causal_final_r1/artifacts/v4_causal_final_model"
# Delivery file -> (its place in the throwaway package, or None when only
# read) and the SHA-256 of the exact file this fixture was generated from.
PINS = {
    "src/s7zeek/domain/events.py": (
        "s7zeek/domain/events.py", "60413956423e0ffa770e0fdaea8c3529d58068e6e9b45cdf9a18d3abbb7c72ae"),
    "src/s7zeek/features/s7_parser.py": (
        "s7zeek/features/s7_parser.py", "8ef6bbdead97a4207d8cc74b98494fa4dd87dd85b3d572514cbe0aa6cda377fe"),
    "src/s7zeek/features/customer_icsnpp_enriched_builder.py": (
        "s7zeek/features/customer_icsnpp_enriched_builder.py",
        "3aa48d969095e45c0084f713f8a34c9a8092674dc5f0cad9cec2998c3ffb3c99"),
    "src/s7zeek/features/customer_icsnpp_time_normalized_builder.py": (
        "s7zeek/features/customer_icsnpp_time_normalized_builder.py",
        "65a03719c1986705d2819ebbd27d352cd66f7fb6ba8dc06115cbcc06180d7fb4"),
    "src/s7zeek/adapters/kafka_source.py": (
        "s7zeek/adapters/kafka_source.py", "bedae0d6427a86dac97b1a0af1c3bbcfbc3301ac8741575590dc28a331d39aca"),
    "src/s7zeek/modeling/preprocessing.py": (
        "s7zeek/modeling/preprocessing.py", "63d4c7ecb94a52f328d426da0e72d35598c8ec3e1c636c1319bd95fdece00891"),
    "src/s7zeek/modeling/debiased.py": (
        None, "faf09cf290dbf534e2d24b5c3aba9bb2ddee9f9b0e28a3b84611244896f162b1"),
    "src/s7zeek/inference/causal_shadow.py": (
        None, "2cded3d7d4384486903ab3bc23feaf86d0d4032387744c633801afd0fa2567e3"),
    f"{RELEASE}/preprocessor.joblib": (
        None, "6b91d4b7b541a146ba00d9dcbc043286c726885dc703e42bcf83fbdaff054175"),
    f"{RELEASE}/s7comm_lstm_autoencoder_debiased.onnx": (
        None, "2a2e5fe233ee37f22e8e7fe29e2438c1145ca7c799719091eaa0a309b9716858"),
    f"{RELEASE}/causal_online_shadow_policy.json": (
        None, "3bc3fa79a64015d9aafa46de989b43675cb436d31193df974df8f627ca6cc559"),
    f"{RELEASE}/causal_online_conformal_calibration_scores.npz": (
        None, "425b839c294f0dd191a103d011b5c4e3ffefc1b41d50b2556f43af468af69630"),
}
FEATURES = [
    "s7_outstanding_requests", "s7_outstanding_mean_16", "s7_response_match_rate_16",
    "s7_same_function_run_length", "s7_same_direction_run_length", "s7_request_ratio_16",
    "s7_direction_change_rate_16", "s7_function_change_rate_16", "s7_function_entropy_16",
    "s7_function_transition_entropy_16", "s7_rosctr_change_rate_16",
    "s7_pdu_reference_unique_ratio_32", "is_request_direction", "s7_function_changed",
    "s7_rosctr", "s7_operation",
]
NUMERIC = FEATURES[:14]
SEQUENCE = 16

# The endpoints every synthetic connection uses; the live check overrides them.
CLIENT = ("10.0.20.5", 50110)
SERVER = ("10.0.20.9", 102)

# A request/response kind: (request ROSCTR, request code, request name, response ROSCTR, response code).
READ = (1, 0x04, "Read Variable", 3, 0x04)
WRITE = (1, 0x05, "Write Variable", 3, 0x05)
SETUP = (1, 0xF0, "Setup Communication", 3, 0xF0)
CPU_FUNCTIONS = (7, 0x44, "Request: CPU Functions", 7, 0x84)  # user data: S2 reads both as 0x00
PLC_STOP = (1, 0x29, "PLC Stop", 3, 0x29)
ACK_READ = (1, 0x04, "Read Variable", 2, None)  # answered by a bare ACK (ROSCTR 2), never seen in training


def tolerance(score: float) -> float:
    """S7commDetectorOracleTest's score tolerance (plan ruling P3)."""
    return 1e-9 + 1e-4 * abs(score)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_package(delivery: Path) -> Path:
    """Copy the pinned upstream modules into a throwaway importable package; verify every pin."""
    root = Path(tempfile.mkdtemp(prefix="s7detector-"))
    for package in ("s7zeek", "s7zeek/domain", "s7zeek/features", "s7zeek/adapters", "s7zeek/modeling"):
        (root / package).mkdir(parents=True, exist_ok=True)
        (root / package / "__init__.py").write_text("")
    for name, (target, expected) in PINS.items():
        actual = sha256(delivery / name)
        if actual != expected:
            sys.exit(f"{delivery / name}: sha256 {actual} != expected {expected}; "
                     "the delivery changed -- review it before regenerating this fixture")
        if target is not None:
            shutil.copyfile(delivery / name, root / target)
    return root


def decision_functions(delivery: Path) -> dict:
    """operation_groups, conformal_pvalues and GROUP_NAMES, exec'd from debiased.py itself."""
    tree = ast.parse((delivery / "src/s7zeek/modeling/debiased.py").read_text())
    keep = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in ("operation_groups", "conformal_pvalues"))
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "").startswith("GROUP") for t in n.targets))]
    namespace = {"np": np}
    exec(compile(ast.Module(body=keep, type_ignores=[]), "debiased.py", "exec"), namespace)
    return namespace


def record(uid: str, ts: float, request: bool, pdu: int, kind: tuple) -> dict:
    """One ICSNPP s7comm.log record: both endpoint pairs, the code as a hex string."""
    rosctr, code, name = (kind[0], kind[1], kind[2]) if request else (kind[3], kind[4], None)
    sender, receiver = (CLIENT, SERVER) if request else (SERVER, CLIENT)
    r = {"ts": round(ts, 6), "uid": uid, "id_orig_h": CLIENT[0], "id_orig_p": CLIENT[1],
         "id_resp_h": SERVER[0], "id_resp_p": SERVER[1], "is_orig": request,
         "source_h": sender[0], "source_p": sender[1], "destination_h": receiver[0], "destination_p": receiver[1],
         "rosctr_code": rosctr, "pdu_reference": pdu}
    if code is not None:
        r["function_code"] = f"0x{code:02x}"
    if name is not None:
        r["function_name"] = name
    return r


def pipelined(uid: str, requests: int, kind_of, t0: float) -> list[dict]:
    """The training capture's shape: two requests (PDU references 7 and 8) in flight before their responses."""
    out, ts = [], t0
    for i in range(0, requests, 2):
        a, b = kind_of(i), kind_of(i + 1)
        out += [record(uid, ts, True, 7, a), record(uid, ts + 0.001, True, 8, b),
                record(uid, ts + 0.004, False, 7, a), record(uid, ts + 0.005, False, 8, b)]
        ts += 0.1
    return out


def alternating(uid: str, requests: int, kind: tuple, t0: float) -> list[dict]:
    """Strict request/response alternation with a new PDU reference each time: common, and far from training."""
    out, ts = [], t0
    for i in range(requests):
        out += [record(uid, ts, True, 100 + i, kind), record(uid, ts + 0.004, False, 100 + i, kind)]
        ts += 0.1
    return out


def flood(uid: str, requests: int, t0: float) -> list[dict]:
    """Reads that are never answered."""
    return [record(uid, t0 + i * 0.001, True, 1000 + i, READ) for i in range(requests)]


# (name, records, the index before which the connection's state is dropped -- a TTL expiry -- or None)
STREAMS = [
    ("pipelined-read", pipelined("CS7ORA", 120, lambda i: SETUP if i == 0 else READ, 1_790_000_000.0), None),
    ("pipelined-userdata",
     pipelined("CS7ORB", 120, lambda i: CPU_FUNCTIONS if i % 20 == 19 else READ, 1_790_001_000.0), None),
    ("pipelined-write", pipelined("CS7ORC", 80, lambda i: WRITE if i % 20 == 19 else READ, 1_790_002_000.0), None),
    ("pipelined-ack-and-stop",
     pipelined("CS7ORD", 80, lambda i: PLC_STOP if i == 41 else (ACK_READ if i % 10 == 9 else READ),
               1_790_003_000.0), None),
    ("alternating-read", alternating("CS7ORE", 40, READ, 1_790_004_000.0), None),
    ("alternating-userdata", alternating("CS7ORF", 30, CPU_FUNCTIONS, 1_790_005_000.0), None),
    ("unanswered-flood", flood("CS7ORG", 60, 1_790_006_000.0), None),
    ("reset", pipelined("CS7ORH", 60, lambda i: READ, 1_790_007_000.0), 40),
]


class Reference:
    """upstream's runtime as the oracle runs it: the builder (S2 on its input), the fitted
    preprocessor (S1), the ONNX graph, and the group-conditional conformal decision."""

    def __init__(self, delivery: Path):
        self.package = build_package(delivery)
        sys.path.insert(0, str(self.package))
        from s7zeek.adapters.kafka_source import normalize_zeek_message
        from s7zeek.features.customer_icsnpp_time_normalized_builder import (
            CustomerICSNPPTimeNormalizedFeatureBuilder,
        )
        self.normalize = normalize_zeek_message
        self.builder_type = CustomerICSNPPTimeNormalizedFeatureBuilder
        self.decide = decision_functions(delivery)
        release = delivery / RELEASE
        self.pre = joblib.load(release / "preprocessor.joblib")
        if list(self.pre.feature_names_in_) != FEATURES:
            sys.exit("the delivered preprocessor's features are not s7comm-feature-v1's")
        # One thread each way, so the graph's arithmetic is the same run to run.
        options = ort.SessionOptions()
        options.intra_op_num_threads = 1
        options.inter_op_num_threads = 1
        self.session = ort.InferenceSession(str(release / "s7comm_lstm_autoencoder_debiased.onnx"), options,
                                            providers=["CPUExecutionProvider"])
        self.policy = json.loads((release / "causal_online_shadow_policy.json").read_text())
        self.calibration = np.load(release / "causal_online_conformal_calibration_scores.npz")
        self.groups = self.decide["GROUP_NAMES"]
        self.pooled = np.concatenate([self.calibration[g] for g in self.groups.values()
                                      if len(self.calibration[g])])
        # causal_shadow.py's weights: 0 on the s7_operation columns, 1 elsewhere.
        self.weights = np.array([0.0 if n.startswith("categorical__s7_operation_") else 1.0
                                 for n in self.pre.get_feature_names_out()], np.float32)

    def close(self):
        shutil.rmtree(self.package)

    def reference(self, group: str):
        """apply_group_conformal_policy: a group with no scores is judged against all of them, fallback alpha."""
        own = self.calibration[group]
        if len(own) == 0:
            return self.pooled, float(self.policy["fallback_alpha"])
        return own, float(self.policy["alpha_by_group"].get(group, self.policy["fallback_alpha"]))

    def p_value(self, calibration, score: float) -> float:
        return float(self.decide["conformal_pvalues"](calibration, np.array([score], np.float64))[0])

    def run(self, records: list[dict], reset_at: int | None = None) -> list[dict]:
        """One connection's records -> one oracle line per record."""
        builder, window, since, out = self.builder_type(), [], 0, []
        for index, raw in enumerate(records):
            reset = index == reset_at
            if reset:  # a TTL expiry: the feature state and the window start empty
                builder, window, since = self.builder_type(), [], 0
            handed = dict(raw)
            if handed.get("rosctr_code") == 7:  # S2
                handed["function_code"] = 0
            event = self.normalize(handed)
            row = builder.process_event(event)
            numeric = [float(np.float32(row[f])) for f in NUMERIC]  # what the Java vector carries
            frame = pd.DataFrame([numeric + [row["s7_rosctr"], row["s7_operation"].upper()]],  # S1
                                 columns=FEATURES)
            x = np.asarray(self.pre.transform(frame), np.float32)[0]
            window.append(x)
            del window[:-SEQUENCE]
            since += 1
            code = event.function_code
            group = self.groups[int(self.decide["operation_groups"](
                [row["is_request_direction"]], [int(code == 4)], [int(code == 5)])[0])]
            calibration, alpha = self.reference(group)
            line = {"reset_before": reset, "raw": json.dumps(raw, sort_keys=True), "values": numeric,
                    "rosctr": row["s7_rosctr"], "operation": row["s7_operation"],
                    "preprocessed": [float(v) for v in x], "group": group, "alpha": alpha,
                    "events_since_reset": since}
            if len(window) < SEQUENCE:
                line.update(verdict="WARMUP", score=None, p=None, p_low=None, p_high=None)
            else:
                seq = np.stack(window)[None]
                recon = self.session.run(None, {"input": seq})[0]
                # causal_shadow.py's score, in its float32 arithmetic.
                score = float(((((recon[:, -1, :] - seq[:, -1, :]) ** 2) * self.weights).sum(axis=1)
                               / np.float32(self.weights.sum()))[0])
                p = self.p_value(calibration, score)
                line.update(verdict="ANOMALY" if np.isfinite(p) and p <= alpha else "NORMAL", score=score, p=p,
                            p_low=self.p_value(calibration, score + tolerance(score)),
                            p_high=self.p_value(calibration, score - tolerance(score)))
            out.append(line)
        return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivery", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    reference = Reference(args.delivery)
    lines = [json.dumps({"meta": {
        "generator": "tests/fixtures/s7comm/generate_detector_oracle.py",
        "python": sys.version.split()[0],
        "onnxruntime": ort.__version__,
        "delivery_sha256": {name: sha for name, (_, sha) in PINS.items()},
        "features": FEATURES,
        "input_amendments": ["S1: the operation upper-cased before the one-hot",
                             "S2: a ROSCTR 7 record's function code handed to upstream as 0x00"],
    }}, sort_keys=True)]
    coverage = {g: {"NORMAL": 0, "ANOMALY": 0} for g in reference.groups.values()}
    ambiguous = []
    try:
        for stream, records, reset_at in STREAMS:
            for index, line in enumerate(reference.run(records, reset_at)):
                line["stream"] = stream
                if line["verdict"] in ("NORMAL", "ANOMALY"):
                    coverage[line["group"]][line["verdict"]] += 1
                    if (line["p_low"] <= line["alpha"]) != (line["p_high"] <= line["alpha"]):
                        ambiguous.append(f"{stream} event {index}")
                lines.append(json.dumps(line, sort_keys=True))
    finally:
        reference.close()

    # What the fixture must cover: both verdicts where the detector can give them.
    for group, counts in coverage.items():
        print(f"{group:14s} NORMAL {counts['NORMAL']:4d}  ANOMALY {counts['ANOMALY']:4d}")
    required = [(g, "NORMAL") for g in ("RESPONSE", "READ_REQUEST", "OTHER_REQUEST")] \
        + [(g, "ANOMALY") for g in coverage]
    missing = [f"{g} {v}" for g, v in required if coverage[g][v] == 0]
    if ambiguous:
        sys.exit("verdicts that could flip within the Java test's score tolerance: " + ", ".join(ambiguous[:10])
                 + " -- change the streams")
    if missing:
        sys.exit("the streams produce no " + ", ".join(missing) + " -- change the streams")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {len(lines) - 1} events from {len(STREAMS)} streams to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Generate the fixture, twice**

```bash
"$SP/venv/bin/python" tests/fixtures/s7comm/generate_detector_oracle.py --delivery models/S7 --out tests/fixtures/s7comm/detector_oracle_v1.jsonl
sha256sum tests/fixtures/s7comm/detector_oracle_v1.jsonl
"$SP/venv/bin/python" tests/fixtures/s7comm/generate_detector_oracle.py --delivery models/S7 --out tests/fixtures/s7comm/detector_oracle_v1.jsonl
sha256sum tests/fixtures/s7comm/detector_oracle_v1.jsonl
```
Expected: a coverage table with NORMAL > 0 for RESPONSE, READ_REQUEST and OTHER_REQUEST, and ANOMALY > 0 for all four groups. Then `wrote 1120 events from 8 streams`, with identical hashes both times. If the generator exits on coverage or ambiguity, change the streams (a stream's length, kinds or PDU references), never the requirement or the tolerance. Record a `Ruling:` line saying what changed, and regenerate.

- [ ] **Step 3: Write the Java test**

```java
package io.netsecml.platform.bootstrap.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.application.usecase.S7commScoringResult;
import io.netsecml.platform.application.usecase.ScoreS7commSequenceUseCase;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.port.out.ReconstructionScorer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The S7comm scoring proof (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
// section 10): every event of the upstream-generated oracle, as ICSNPP writes
// it, goes through this platform's real parser, mapper, feature use case,
// preprocessing and Java ONNX scorer, with state and windows per uid threaded
// as the Flink operators thread them. Vectors must equal upstream's bit for
// bit, and preprocessed values to 1e-6. Verdicts, groups, alphas and counts
// must match exactly; scores to 1e-9 + 1e-4 relative, and p-values within the
// band that tolerance allows (plan ruling P3).
class S7commDetectorOracleTest {

    private static final Path ORACLE = Path.of("..", "..", "tests", "fixtures", "s7comm", "detector_oracle_v1.jsonl");
    private static final String BUNDLE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v1").toString();
    private static final SensorId SENSOR = new SensorId("sensor-oracle");

    @Test
    void everyEventMatchesUpstream() throws Exception {
        ObjectMapper json = new ObjectMapper();
        List<String> lines = Files.readAllLines(ORACLE, StandardCharsets.UTF_8);
        JsonZeekS7commParser parser = new JsonZeekS7commParser();
        S7commEventMapper mapper = new S7commEventMapper();
        S7commBuildFeaturesUseCase features = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        Map<String, S7commScoreWindow> windows = new HashMap<>();
        Map<String, Integer> verdicts = new HashMap<>();
        try (ReconstructionScorer scorer = new S7commDetectorScorerFactory(BUNDLE).create()) {
            ScoreS7commSequenceUseCase scoring = new ScoreS7commSequenceUseCase(scorer, Clock.systemUTC());
            // Line 0 is the generator's meta.
            for (int i = 1; i < lines.size(); i++) {
                JsonNode expected = json.readTree(lines.get(i));
                String at = "event " + i + " (" + expected.get("stream").asText() + ")";
                S7commEvent event = parseAndMap(parser, mapper, expected.get("raw").asText());
                String uid = event.connectionUid();
                // A TTL expiry: the feature state and the window start empty.
                if (expected.get("reset_before").asBoolean()) {
                    states.remove(uid);
                    windows.remove(uid);
                }
                // As S7commFeatureProcessFunction does: absent state is a fresh connection.
                S7commConnectionState state = states.get(uid);
                boolean fresh = state == null;
                FeatureBuildResult<S7commConnectionState> built =
                    features.build(event, fresh ? S7commConnectionState.empty() : state);
                states.put(uid, built.newState());
                float[] v = built.vector().values();

                // The 14 numeric features bit for bit; the two codes decoded.
                for (int f = 0; f < 14; f++) {
                    assertEquals((float) expected.get("values").get(f).asDouble(), v[f], 0f, at + " feature " + f);
                }
                assertEquals(expected.get("rosctr").asText(), S7commCategories.decodeRosctr((int) v[14]), at);
                assertEquals(expected.get("operation").asText(), S7commCategories.decodeOperation((int) v[15]), at);

                // The preprocessing, against the delivered fitted preprocessor.
                float[] x = scorer.bundle().preprocessing().apply(v);
                for (int f = 0; f < x.length; f++) {
                    assertEquals((float) expected.get("preprocessed").get(f).asDouble(), x[f], 1e-6f,
                        at + " column " + f);
                }

                // Scoring, per uid, with the endpoints s7comm-features hands over.
                String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
                String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
                S7commScoringResult result = scoring.score(built.vector(), fresh, client, server, windows.get(uid));
                windows.put(uid, result.window());
                S7commDetectorPrediction p = result.prediction();
                assertEquals(expected.get("verdict").asText(), p.verdict().name(), at);
                assertEquals(expected.get("group").asText(), p.scoreGroup().name(), at);
                assertEquals(expected.get("alpha").asDouble(), p.alpha(), 0.0, at);
                assertEquals(expected.get("events_since_reset").asLong(), p.eventsSinceReset(), at);
                if (p.verdict().scored()) {
                    double want = expected.get("score").asDouble();
                    assertEquals(want, p.score(), 1e-9 + 1e-4 * Math.abs(want), at + " score");
                    double low = expected.get("p_low").asDouble();
                    double high = expected.get("p_high").asDouble();
                    assertTrue(p.pValue() >= low && p.pValue() <= high,
                        at + " p " + p.pValue() + " outside [" + low + ", " + high + "]");
                }
                verdicts.merge(p.verdict().name(), 1, Integer::sum);
            }
        }
        // The generator enforced per-group coverage; this pins that the fixture still has it.
        assertTrue(verdicts.getOrDefault("NORMAL", 0) > 0 && verdicts.getOrDefault("ANOMALY", 0) > 0
            && verdicts.getOrDefault("WARMUP", 0) > 0, "verdicts " + verdicts);
    }

    // The real parser, then the real mapper; the oracle holds only S7comm records.
    private static S7commEvent parseAndMap(JsonZeekS7commParser parser, S7commEventMapper mapper, String raw) {
        MappingResult<ZeekS7commRecord> parsed = parser.parse(raw.getBytes(StandardCharsets.UTF_8));
        assertTrue(parsed.isValid(), () -> "parser rejected " + raw);
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), SENSOR);
        assertTrue(mapped.isValid(), () -> "mapper rejected " + raw);
        return (S7commEvent) mapped.value();
    }
}
```

- [ ] **Step 4: Run it**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='S7commDetectorOracleTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 1, Failures: 0, Errors: 0`. A failure on a score or p-value is a real difference between Java and Python. Find which stage first differs (vector, preprocessed column, then score); never loosen the tolerance to pass.

- [ ] **Step 5: A planted bug breaks it**

In `S7commPreprocessing.category(...)`, temporarily delete `.toUpperCase(Locale.ROOT)` (undoing S1). Run the Step 4 command. Expected: FAIL at the first user-data event, on `column 17` (`FUNCTION_0X00`). Restore the line, rerun, and expect a pass.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/s7comm/generate_detector_oracle.py tests/fixtures/s7comm/detector_oracle_v1.jsonl \
  modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/S7commDetectorOracleTest.java
git commit -m "test(s7comm): the upstream-generated scoring oracle

<attribution lines>"
```

---

### Task 16: Deploy: topic, pin, preflight, supervisor and selftest

**Files:**
- Modify: `deploy/kafka/topics.conf`, `deploy/.env.template`, `.env.example`, `deploy/docker-compose.yml`
- Modify: `deploy/lib/stack.sh` (`detector_bundle_setting`, `check_detector_bundle`, and a new `check_one_bundle`)
- Modify: `deploy/lib/selftest.sh` (`selftest_cleanup`, `selftest_run`)
- Modify: `deploy/flink/submit-jobs.sh` (`is_bundle_failure` and its log line)
- Test: `deploy/tests/test_stack.sh`, `deploy/tests/test_compose.sh`, `deploy/tests/test_selftest.sh`, `deploy/tests/test_submit_jobs.sh`

**Interfaces:**
- Produces: `detector_bundle_setting VAR`, `check_one_bundle LABEL VAR SCRIPT FILES` and `check_detector_bundle` (both pins).

- [ ] **Step 1: Write the failing tests**

`deploy/tests/test_stack.sh`:
1. Change both `assert_eq 13 "$(grep -c -- '--create' ...)"` lines to `14` (the second one's message becomes `"create_topics creates all 14 topics"`). Add after the first:
```bash
assert_eq 1 "$(grep -c -- '--topic netsec.s7comm.prediction.v1 --partitions 1 --replication-factor 1 --config retention.ms=604800000' "$tmp/calls")" "the S7 prediction topic: 1 partition, 7 days"
```
2. Every `printf 'MODBUS_DETECTOR_BUNDLE=...\n' > "$ENV_FILE"` in the existing bundle and restart tests also sets an empty S7 pin, so that those tests keep checking only the Modbus bundle:
```bash
printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\nS7COMM_DETECTOR_BUNDLE=\n' > "$ENV_FILE"
```
(and `printf 'MODBUS_DETECTOR_BUNDLE=\nS7COMM_DETECTOR_BUNDLE=\n'` for the empty-pin one). Apply this to all four such lines.
3. Before the final `finish`, add:
```bash
# The S7comm pin (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
# section 7), checked like the Modbus one: missing, present, corrupt, empty.
REPO_ROOT_SAVED="$REPO_ROOT"; REPO_ROOT="$tmp/s7repo"; mkdir -p "$REPO_ROOT/models"
ENV_FILE_SAVED="$ENV_FILE"; ENV_FILE="$tmp/s7.env"
printf 'MODBUS_DETECTOR_BUNDLE=\nS7COMM_DETECTOR_BUNDLE=s7comm-stage1-detector/v1\n' > "$ENV_FILE"
out="$( (check_detector_bundle) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'the S7comm detector bundle models/s7comm-stage1-detector/v1 is missing' <<< "$out")" "a missing S7 bundle is refused"
assert_eq 1 "$(grep -cx 'exit=1' <<< "$out")" "and stops"
mkdir -p "$REPO_ROOT/models/s7comm-stage1-detector/v1"; cp "${REPO_ROOT_SAVED}/tests/fixtures/models/s7comm-stage1-detector/v1/"* "$REPO_ROOT/models/s7comm-stage1-detector/v1/"
out="$( (check_detector_bundle) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -cx 'exit=0' <<< "$out")" "a present S7 bundle passes"
printf 'x' >> "$REPO_ROOT/models/s7comm-stage1-detector/v1/calibration.npz"
out="$( (check_detector_bundle) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'models/s7comm-stage1-detector/v1/calibration.npz does not match its SHA-256 in bundle.json' <<< "$out")" "a corrupt calibration file is refused"
assert_eq 1 "$(grep -cx 'exit=1' <<< "$out")" "and stops"
printf 'MODBUS_DETECTOR_BUNDLE=\nS7COMM_DETECTOR_BUNDLE=\n' > "$ENV_FILE"
out="$( (check_detector_bundle) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -cx 'exit=0' <<< "$out")" "empty pins need no bundle"
# Review Focus 1: an older .env without the S7 line gets the template's pin, as compose does.
printf 'MODBUS_DETECTOR_BUNDLE=\n' > "$ENV_FILE"
assert_eq s7comm-stage1-detector/v1 "$(detector_bundle_setting S7COMM_DETECTOR_BUNDLE)" "the template's S7 pin applies"
REPO_ROOT="$REPO_ROOT_SAVED"; ENV_FILE="$ENV_FILE_SAVED"

# Review Focus 1: restart stops nothing when only the S7 bundle is missing.
DEPLOY_DIR_SAVED="$DEPLOY_DIR"; DEPLOY_DIR="$tmp/deploy"; touch "$DEPLOY_DIR/jars/online-feature-job.jar" "$DEPLOY_DIR/jars/archive-job.jar"
REPO_ROOT_SAVED="$REPO_ROOT"; REPO_ROOT="$tmp/s7restart"; mkdir -p "$REPO_ROOT/models/modbus-stage1-detector/v1"
cp "${REPO_ROOT_SAVED}/tests/fixtures/models/modbus-stage1-detector/v1/"* "$REPO_ROOT/models/modbus-stage1-detector/v1/"
ENV_FILE_SAVED="$ENV_FILE"; ENV_FILE="$tmp/s7restart.env"
printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\nS7COMM_DETECTOR_BUNDLE=s7comm-stage1-detector/v1\n' > "$ENV_FILE"
load_env() { :; }
docker() { return 0; }
list_interfaces() { printf 'lo\neth0\n'; }
tune_check_drift() { :; }
stack_down() { echo "stack_down called"; }
stack_up() { echo "stack_up called"; }
# shellcheck disable=SC2034  # stack_preflight reads ZEEK_INTERFACE
out="$( (ZEEK_INTERFACE=eth0; stack_restart) 2>&1; echo "exit=$?")"
assert_eq 0 "$(grep -c 'stack_down called' <<< "$out")" "restart stops nothing when the S7 bundle is missing"
assert_eq 1 "$(grep -c 'models/s7comm-stage1-detector/v1 is missing' <<< "$out")" "and says why"
DEPLOY_DIR="$DEPLOY_DIR_SAVED"; REPO_ROOT="$REPO_ROOT_SAVED"; ENV_FILE="$ENV_FILE_SAVED"
unset -f load_env docker list_interfaces tune_check_drift stack_down stack_up
```

`deploy/tests/test_compose.sh`. After the Modbus `"the prediction topic"` assertion, add:
```bash
# S7comm scoring (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md section 7).
assert_eq s7comm-stage1-detector/v1 "$(q '.services["job-submitter"].environment.S7COMM_DETECTOR_BUNDLE')" "the S7 detector pin reaches the jobs"
assert_eq netsec.s7comm.prediction.v1 "$(q '.services["job-submitter"].environment.S7COMM_PREDICTION_TOPIC')" "the S7 prediction topic"
```
After the Modbus block's closing `make_env 127.0.0.1`, add:
```bash
# Review Focus 1: the same for the S7comm pin.
sed -i '/^S7COMM_DETECTOR_BUNDLE=/d; /^S7COMM_PREDICTION_TOPIC=/d' "$tmp/env"
assert_eq s7comm-stage1-detector/v1 "$(config | jq -r '.services["job-submitter"].environment.S7COMM_DETECTOR_BUNDLE')" "an older .env scores S7 with the default bundle"
assert_eq netsec.s7comm.prediction.v1 "$(config | jq -r '.services["job-submitter"].environment.S7COMM_PREDICTION_TOPIC')" "and gets the default S7 topic"
printf 'S7COMM_DETECTOR_BUNDLE=\n' >> "$tmp/env"
assert_eq "" "$(config | jq -r '.services["job-submitter"].environment.S7COMM_DETECTOR_BUNDLE')" "an empty S7 pin turns S7 scoring off"
make_env 127.0.0.1
```

`deploy/tests/test_selftest.sh`. Replace the whole "Review finding (selftest leftover)" block, from `ENV_FILE_SAVED=` to `unset -f load_env sleep compose ch_query`, with:
```bash
# With scoring on, the selftest waits for each pair's two predictions -- which
# arrive through their own archive chains, possibly a checkpoint after the
# vectors -- then deletes them with its other rows. With scoring off it waits
# for none. Both pins, each on or off.
ENV_FILE_SAVED="$ENV_FILE"; ENV_FILE="$tmp/selftest.env"
export MODBUS_RAW_TOPIC=netsec.modbus.raw.v1 S7COMM_RAW_TOPIC=netsec.s7comm.raw.v1
load_env() { :; }
sleep() { :; }
compose() { cat > /dev/null; }
ch_query() {
  printf '%s\n' "$1" >> "$tmp/queries"
  case "$1" in
    *"FROM modbus_detector_predictions"*)
      # None on the first poll, both on the next.
      if [ -f "$tmp/modbus-ready" ]; then printf '2\n'; else touch "$tmp/modbus-ready"; printf '0\n'; fi ;;
    *"FROM s7comm_detector_predictions"*)
      if [ -f "$tmp/s7-ready" ]; then printf '2\n'; else touch "$tmp/s7-ready"; printf '0\n'; fi ;;
    *"FROM feature_vectors"*) printf '2\t2\n' ;;
    *"FROM invalid_events"*) printf '0\n' ;;
  esac
}
fresh_run() { rm -f "$tmp/modbus-ready" "$tmp/s7-ready"; : > "$tmp/queries"; }

printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\nS7COMM_DETECTOR_BUNDLE=s7comm-stage1-detector/v1\n' > "$ENV_FILE"; fresh_run
out="$(selftest_run 2>&1)"
assert_eq 1 "$(grep -c 'selftest PASSED: 2 Modbus and 2 S7comm feature vectors, 2 Modbus predictions, 2 S7comm predictions reached ClickHouse, no DLQ rows (test rows removed)' <<< "$out")" "both on: both waited for and reported"
assert_eq 2 "$(grep -c 'SELECT count() FROM s7comm_detector_predictions' "$tmp/queries")" "polling until the S7 predictions arrive"
assert_eq 1 "$(grep -c "ALTER TABLE modbus_detector_predictions DELETE WHERE connection_uid = 'SELFTEST-[0-9]*-M'" "$tmp/queries")" "the Modbus predictions deleted"
assert_eq 1 "$(grep -c "ALTER TABLE s7comm_detector_predictions DELETE WHERE connection_uid = 'SELFTEST-[0-9]*-S'" "$tmp/queries")" "the S7 predictions deleted"

printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\nS7COMM_DETECTOR_BUNDLE=\n' > "$ENV_FILE"; fresh_run
out="$(selftest_run 2>&1)"
assert_eq 1 "$(grep -c 'selftest PASSED: 2 Modbus and 2 S7comm feature vectors, 2 Modbus predictions reached ClickHouse, no DLQ rows, scoring off for S7comm (test rows removed)' <<< "$out")" "S7 off: passes without S7 predictions"
assert_eq 0 "$(grep -c 'SELECT count() FROM s7comm_detector_predictions' "$tmp/queries")" "and never waits for them"

printf 'MODBUS_DETECTOR_BUNDLE=\nS7COMM_DETECTOR_BUNDLE=\n' > "$ENV_FILE"; fresh_run
out="$(selftest_run 2>&1)"
assert_eq 1 "$(grep -c 'selftest PASSED: 2 Modbus and 2 S7comm feature vectors reached ClickHouse, no DLQ rows, scoring off for Modbus S7comm (test rows removed)' <<< "$out")" "both off"
assert_eq 0 "$(grep -c 'SELECT count() FROM modbus_detector_predictions' "$tmp/queries")" "waits for no Modbus predictions"
ENV_FILE="$ENV_FILE_SAVED"
unset -f load_env sleep compose ch_query fresh_run
```

`deploy/tests/test_submit_jobs.sh`. In the bundle-failure block, change `'submitting online-feature-job failed on the Modbus detector bundle'` to `'submitting online-feature-job failed on a detector bundle'`, and `"failed on the Modbus detector bundle.*model.onnx does not match` to `"failed on a detector bundle.*model.onnx does not match`. Then, before that block's `unset -f flink`, add an S7 case:
```bash
# The S7comm loader's failure is a bundle failure too.
flink() {
  printf '%s\n' "$*" >> "$tmp/calls"
  case "$*" in
    *online-feature-job.jar*)
      printf '%s\n' "Caused by: java.lang.IllegalStateException: calibration.npz does not match bundle.json's recorded SHA-256" \
        '	at io.netsecml.platform.adapter.registry.S7commDetectorBundleLoader.load(S7commDetectorBundleLoader.java:60)'
      return 1 ;;
  esac
  return 0
}
out="$(supervise_once 2>&1)"
assert_eq 1 "$(grep -c 'submitting online-feature-job failed on a detector bundle' <<< "$out")" "an S7 bundle failure is named as one"
assert_eq 0 "$(grep -c 'online-feature-job.*aside' <<< "$out")" "and never offers to move its saved state aside"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `for t in test_stack test_compose test_selftest test_submit_jobs; do bash deploy/tests/$t.sh | tail -1; done`
Expected: each file ends with failures. The failing assertions are:
- `test_stack`: 13 topics, not 14; `detector_bundle_setting` ignores its argument;
- `test_compose`: no S7 pin in compose;
- `test_selftest`: no S7 predictions polled, and the old PASSED messages;
- `test_submit_jobs`: "the Modbus detector bundle" wording, and the S7 loader not recognised.

- [ ] **Step 3: Implement**

`deploy/kafka/topics.conf`, after the `MODBUS_PREDICTION_TOPIC` line:
```
S7COMM_PREDICTION_TOPIC       1  168
```

`deploy/.env.template`: after `MODBUS_PREDICTION_TOPIC=netsec.modbus.prediction.v1`, add `S7COMM_PREDICTION_TOPIC=netsec.s7comm.prediction.v1`. After the `MODBUS_DETECTOR_BUNDLE=` block, add:
```
# The S7comm Stage 1 detector bundle, under the repository's models/ folder
# (spec: docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md).
# Package it with deploy/models/package-s7comm-detector.sh <delivery-dir> and
# copy it there. Set it empty to run S7comm features without scoring.
S7COMM_DETECTOR_BUNDLE=s7comm-stage1-detector/v1
```
Make the same two additions to the root `.env.example`, after its `MODBUS_PREDICTION_TOPIC` and `MODBUS_DETECTOR_BUNDLE` lines.

`deploy/docker-compose.yml`, `job-submitter` environment, after the `MODBUS_PREDICTION_TOPIC` line:
```yaml
      # '-' (not ':-'), as for the Modbus pin: unset scores with the default
      # bundle, an explicit empty value turns S7 scoring off.
      S7COMM_DETECTOR_BUNDLE: ${S7COMM_DETECTOR_BUNDLE-s7comm-stage1-detector/v1}
      S7COMM_PREDICTION_TOPIC: ${S7COMM_PREDICTION_TOPIC:-netsec.s7comm.prediction.v1}
```

`deploy/lib/stack.sh`: replace `detector_bundle_setting` and `check_detector_bundle` (and their comments) with:
```bash
# A detector pin: deploy/.env's value of VAR when it has the line (even empty),
# else the template's default -- what compose resolves (plan ruling P4).
detector_bundle_setting() {
  if grep -q "^$1=" "$ENV_FILE" 2>/dev/null; then
    env_value "$ENV_FILE" "$1"
  else
    env_value "${DEPLOY_DIR}/.env.template" "$1"
  fi
}

# One pinned bundle must be on disk and intact before the jobs start: LABEL
# names it, VAR is its pin, SCRIPT packages it, FILES are file:bundle.json-key
# pairs -- the check each loader makes before the job submits, done here so a
# missing or corrupt bundle stops 'up' and 'restart' before anything stops.
check_one_bundle() {
  local label="$1" var="$2" script="$3" files="$4" bundle file key expected actual
  bundle="$(detector_bundle_setting "$var")"
  [ -z "$bundle" ] && return 0
  [ -f "${REPO_ROOT}/models/${bundle}/bundle.json" ] \
    || die "the ${label} detector bundle models/${bundle} is missing: package it with deploy/models/${script} <delivery-dir> and copy it to models/${bundle}/, or set ${var}= (empty) in deploy/.env to run without ${label} scoring"
  for file in $files; do
    key="${file#*:}"; file="${file%%:*}"
    expected="$(jq -r ".${key} // empty" "${REPO_ROOT}/models/${bundle}/bundle.json" 2>/dev/null || true)"
    actual="$(sha256sum "${REPO_ROOT}/models/${bundle}/${file}" 2>/dev/null | cut -c1-64 || true)"
    if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
      die "the ${label} detector bundle is corrupt: models/${bundle}/${file} does not match its SHA-256 in bundle.json (expected ${expected:-none}, got ${actual:-no file}); package it again with deploy/models/${script}, or set ${var}= (empty) in deploy/.env to run without ${label} scoring"
    fi
  done
}

# Every pinned detector bundle (both scoring designs, section 7).
check_detector_bundle() {
  check_one_bundle Modbus MODBUS_DETECTOR_BUNDLE package-modbus-detector.sh \
    "model.onnx:modelSha preprocessing.json:preprocessingSha thresholds.json:thresholdsSha"
  check_one_bundle S7comm S7COMM_DETECTOR_BUNDLE package-s7comm-detector.sh \
    "model.onnx:modelSha preprocessing.json:preprocessingSha policy.json:policySha calibration.npz:calibrationSha"
}
```

`deploy/lib/selftest.sh`: in `selftest_cleanup`, after the Modbus predictions delete, add:
```bash
  ch_query "ALTER TABLE s7comm_detector_predictions DELETE WHERE connection_uid = '$3'" mutations_sync=1 >/dev/null \
    || warn "could not delete the selftest's S7comm prediction rows"
```
In `selftest_run`:
- change the `local` line to declare `modbus_scoring s7_scoring preds s7_preds` in place of `scoring preds`;
- replace the `scoring="$(detector_bundle_setting)"` block with:
```bash
  # With a detector bundle pinned, that protocol's pair must also be scored:
  # two predictions each (both WARMUP), archived by their own chains.
  modbus_scoring="$(detector_bundle_setting MODBUS_DETECTOR_BUNDLE)"
  s7_scoring="$(detector_bundle_setting S7COMM_DETECTOR_BUNDLE)"
```
- replace the polling loop's body from `preds=""` through the end of the failure branch with:
```bash
    preds=""
    s7_preds=""
    if [ -n "$modbus_scoring" ]; then
      preds="$(ch_query "SELECT count() FROM modbus_detector_predictions WHERE connection_uid = '${uid_m}'" || true)"
    fi
    if [ -n "$s7_scoring" ]; then
      s7_preds="$(ch_query "SELECT count() FROM s7comm_detector_predictions WHERE connection_uid = '${uid_s}'" || true)"
    fi
    if [ "$got" = "$(printf '2\t2')" ] && [ "$dlq" = 0 ] \
       && { [ -z "$modbus_scoring" ] || [ "$preds" = 2 ]; } && { [ -z "$s7_scoring" ] || [ "$s7_preds" = 2 ]; }; then
      break
    fi
    if [ "${dlq:-0}" != 0 ] || [ "$(date +%s)" -ge "$deadline" ]; then
      # The predictions are part of the verdict only where scoring is on.
      local want_preds=""
      if [ -n "$modbus_scoring" ]; then
        want_preds="; Modbus predictions = '${preds}', want 2"
      fi
      if [ -n "$s7_scoring" ]; then
        want_preds="${want_preds}; S7comm predictions = '${s7_preds}', want 2"
      fi
      warn "selftest FAILED: feature vectors (modbus, s7comm) = '${got}', want 2 and 2; DLQ rows = '${dlq}', want 0${want_preds}"
      ch_query "SELECT log_type, reason_code, detail FROM invalid_events WHERE event_id LIKE '%${run}%' FORMAT PrettyCompactMonoBlock" >&2 || true
      selftest_cleanup "$run" "$uid_m" "$uid_s"
      return 1
    fi
```
- replace the closing `if [ -n "$scoring" ]; then log ... else log ... fi` with:
```bash
  # What reached ClickHouse, and which scoring was off.
  local reached="" off=""
  if [ -n "$modbus_scoring" ]; then reached="${reached}, 2 Modbus predictions"; else off="${off} Modbus"; fi
  if [ -n "$s7_scoring" ]; then reached="${reached}, 2 S7comm predictions"; else off="${off} S7comm"; fi
  log "selftest PASSED: 2 Modbus and 2 S7comm feature vectors${reached} reached ClickHouse, no DLQ rows${off:+, scoring off for${off}} (test rows removed)"
```
Update the function's header comment: "…its rows -- each scored pair's two predictions included -- are deleted afterwards."

`deploy/flink/submit-jobs.sh`: replace `is_bundle_failure` and its comment with:
```bash
# True when 'flink run' output on stdin failed inside a detector bundle's
# loader, Modbus or S7comm: the online job's main() verifies each pinned bundle
# before it submits, so a missing or corrupt bundle fails here, with state not
# involved (plan ruling P4).
is_bundle_failure() {
  grep -qE 'SequenceDetectorBundleLoader|S7commDetectorBundleLoader'
}
```
and its log line with:
```bash
        log "submitting ${name} failed on a detector bundle, not on its saved state, so leave that alone: $(root_cause <<< "$output"). Package the bundle again (deploy/models/package-modbus-detector.sh or package-s7comm-detector.sh) or set its pin (MODBUS_DETECTOR_BUNDLE= or S7COMM_DETECTOR_BUNDLE=) empty in deploy/.env to run without that scoring, then 'deploy.sh restart'. Retrying in 60 s."
```

- [ ] **Step 4: Run every deploy check**

Run: `./mvnw -q package -pl modules/bootstrap-online-job,modules/bootstrap-archive-job -am -DskipTests && bash deploy/tests/run-all.sh`
Expected: every file reports `0 failed`, shellcheck is clean, and the last line is `run-all: every check passed`. If shellcheck flags SC2015 or SC2016, rewrite as `if` statements, as the Modbus unit did.

- [ ] **Step 5: Commit**

```bash
git add deploy/kafka/topics.conf deploy/.env.template .env.example deploy/docker-compose.yml deploy/lib/stack.sh \
  deploy/lib/selftest.sh deploy/flink/submit-jobs.sh deploy/tests/test_stack.sh deploy/tests/test_compose.sh \
  deploy/tests/test_selftest.sh deploy/tests/test_submit_jobs.sh
git commit -m "feat(deploy): the S7comm prediction topic, detector pin, preflight and selftest

<attribution lines>"
```

---

### Task 17: The real-traffic check through the Java production path (before merge)

**Files:** nothing in the repository. Scratch only: `$SP/S7ScoreCapture.java`, `$SP/s7java/` and `$SP/s7check/java-check.txt`.

**Interfaces:**
- Consumes: the shaded online JAR; the fixture bundle; Task 1's Zeek logs in `$SP/s7check/`.

- [ ] **Step 1: Write the harness `$SP/S7ScoreCapture.java`**

```java
// Throwaway (plan Task 17): scores Zeek s7comm.log files through the
// production parser, mapper, feature use case, preprocessing and ONNX scorer,
// per uid as the online job keys them, and prints spec section 11's report.
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.application.usecase.S7commScoringResult;
import io.netsecml.platform.application.usecase.ScoreS7commSequenceUseCase;
import io.netsecml.platform.bootstrap.online.S7commDetectorScorerFactory;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.port.out.ReconstructionScorer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class S7ScoreCapture {

    // args[0]: the bundle directory; then one or more s7comm.log files.
    public static void main(String[] args) throws Exception {
        for (int a = 1; a < args.length; a++) {
            report(args[0], Path.of(args[a]));
        }
    }

    private static void report(String bundle, Path log) throws Exception {
        JsonZeekS7commParser parser = new JsonZeekS7commParser();
        S7commEventMapper mapper = new S7commEventMapper();
        S7commBuildFeaturesUseCase features = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        Map<String, S7commScoreWindow> windows = new HashMap<>();
        // Report line -> {NORMAL, ANOMALY}.
        Map<String, long[]> tally = new TreeMap<>();
        long rejected = 0;
        long scored = 0;
        long micros = 0;
        try (ReconstructionScorer scorer = new S7commDetectorScorerFactory(bundle).create()) {
            ScoreS7commSequenceUseCase scoring = new ScoreS7commSequenceUseCase(scorer, Clock.systemUTC());
            for (String line : Files.readAllLines(log, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                // The real parser and mapper; a rejection is counted, as the DLQ would take it.
                MappingResult<ZeekS7commRecord> parsed = parser.parse(line.getBytes(StandardCharsets.UTF_8));
                MappingResult<NetworkEvent> mapped = parsed.isValid()
                    ? mapper.map(parsed.value(), new SensorId("capture")) : null;
                if (mapped == null || !mapped.isValid()) {
                    rejected++;
                    continue;
                }
                S7commEvent event = (S7commEvent) mapped.value();
                // Per uid, as S7commFeatureProcessFunction and s7comm-score keep state.
                String uid = event.connectionUid();
                S7commConnectionState state = states.get(uid);
                boolean fresh = state == null;
                FeatureBuildResult<S7commConnectionState> built =
                    features.build(event, fresh ? S7commConnectionState.empty() : state);
                states.put(uid, built.newState());
                String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
                String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
                S7commScoringResult result = scoring.score(built.vector(), fresh, client, server, windows.get(uid));
                windows.put(uid, result.window());
                S7commDetectorPrediction p = result.prediction();
                if (!p.verdict().scored()) {
                    continue;
                }
                // Tally the verdict under every report line it belongs to.
                scored++;
                micros += p.inferenceMicros();
                float[] v = built.vector().values();
                int slot = p.verdict() == DetectorVerdict.NORMAL ? 0 : 1;
                String phase = p.eventsSinceReset() > 64 ? "past 64th" : "16th-64th";
                String kind = "rosctr " + S7commCategories.decodeRosctr((int) v[14]) + " "
                    + S7commCategories.decodeOperation((int) v[15]);
                for (String key : List.of("all", phase, "group " + p.scoreGroup(), kind)) {
                    tally.computeIfAbsent(key, k -> new long[2])[slot]++;
                }
            }
        }
        System.out.printf("== %s: %d rejected, %d scored, %.1f us per window%n", log, rejected, scored,
            scored == 0 ? 0.0 : (double) micros / scored);
        tally.forEach((key, n) -> System.out.printf("  %-44s %6.2f%% NORMAL of %d%n", key,
            100.0 * n[0] / (n[0] + n[1]), n[0] + n[1]));
    }
}
```

- [ ] **Step 2: Build, compile and run it**

```bash
./mvnw -q package -pl modules/bootstrap-online-job -am -DskipTests
JAR="$(ls modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar)"
rm -rf "$SP/s7java" && mkdir -p "$SP/s7java"
javac -cp "$JAR" -d "$SP/s7java" "$SP/S7ScoreCapture.java"
java -cp "$JAR:$SP/s7java" S7ScoreCapture tests/fixtures/models/s7comm-stage1-detector/v1 "$SP"/s7check/*/s7comm.log | tee "$SP/s7check/java-check.txt"
```
Expected: an `==` block per capture. Each capture's `all` / `past 64th` / group figures should agree with Task 1's `[S1+S2]` lines to within a few events; a larger difference is a finding to explain before going on. Record the `us per window` figure: it is the scoring throughput measurement.

- [ ] **Step 3: The attack captures, joined to labels where they can be**

Using the label format from Task 1 Step 6: if a label row maps to an S7 record (a timestamp and endpoints), write a throwaway join in `$SP` and report per capture the share of attack-labelled S7 events scored `ANOMALY`. If labels do not map to S7 records, as for TCP-level floods, report each attack capture's `% NORMAL` and state that recall is not measurable from these captures.

- [ ] **Step 4: Apply the stop rule again (spec §11)**

If any benign capture's `past 64th` figure is below 99% NORMAL: **STOP** before Task 18. Report `java-check.txt` to the owner with the Task 1 comparison. Otherwise write the figures to the ledger and continue.

---

### Task 18: Documentation and full verification

**Files:**
- Modify: `CLAUDE.md`, `deploy/README.md`, `docs/clickhouse.md`

- [ ] **Step 1: Update `CLAUDE.md`**

- **Architecture, data flow:** after the Modbus scoring sentence, add: "S7comm is scored the same way: `s7comm-features` hands every vector, with its connection key, whether the connection started from empty state, and its endpoints, to `s7comm-score` (the model team's S7comm Stage 1 LSTM autoencoder in ONNX Runtime, with its group-conditional conformal decision) → `netsec.s7comm.prediction.v1` → the archive job's tenth chain → ClickHouse `s7comm_detector_predictions`." Change "ONNX inference is wired for Modbus Stage 1 only" to "ONNX inference is wired for the Modbus and S7comm Stage 1 detectors only".
- **Key invariants, the state-name bullet:** add "S7comm's scorer state is `s7comm-score-window`: each connection's last 16 preprocessed vectors and its event count since the last reset, keyed like `s7comm-features` and expiring under `S7COMM_STATE_TTL_MINUTES`."
- **Implementation state:** after the Modbus scoring unit's paragraph, add "The S7comm scoring unit (`feat/s7comm-scoring`, from `feat/deploy-mvp`)" with the spec and plan paths and these deliverables:
  - S2 in the feature use case and S1 in the preprocessing;
  - `S7commPreprocessing`, `S7commConformalPolicy`, the bundle, window and prediction types, `ReconstructionScorer` and `ScoreS7commSequenceUseCase`;
  - `contracts/model/s7comm-detector-bundle-v1.json`, `deploy/models/package-s7comm-detector.sh`, `S7commDetectorBundleLoader` (reading the delivered `.npz` directly) and `OnnxReconstructionScorer`;
  - `contracts/stream/s7comm-detector-prediction-v1.json` and DDL `004`;
  - `s7comm-score` and `s7comm-prediction-sink` (disabled when the pin is empty) and the archive job's tenth chain (`connDnsModbusS7commAndBothPredictionChains`);
  - deployment: the topic, the pin, and the preflight and selftest for both pins;
  - the scoring oracle (`tests/fixtures/s7comm/detector_oracle_v1.jsonl`, 1120 events).

  Update the pipeline paragraph: ten chains, and `s7comm_detector_predictions` among the tables.
- **Verification state:** a new table, "Verified fresh for the S7comm scoring unit at `<the Task 18 docs commit's parent>`", filled from Step 4's per-module counts. It names `S7commDetectorOracleTest` as the scoring proof, and says the container tables predate this unit. Keep the Modbus table, relabelled as the prior verification.
- **S7comm limits and decisions:** replace the "No conn.log context and no scoring" bullet with "No conn.log context." and add these bullets:
  - **The scoring inputs (S1, S2; 2026-09-28).** The training table came from upstream's own pcap parser. S2: a USERDATA PDU's function code is read as 0x00, which changes the vectors on `netsec.s7comm.feature-vector.v1`. S1: the operation is upper-cased before the one-hot. Neither is confirmed by the model team yet.
  - **The detector's "normal" is one connection's shape.** Trained on a single uid: a pipelined poller with two PDU references. A strictly alternating poll scored `ANOMALY` in every probe. The real-traffic result: Task 17's per-capture `past 64th` and `all` figures, with the captures named. A connection unlike the training one is flagged throughout.
  - **Post-reset false alarms.** The delivered stress test shows about 10% for the first ~64 events after any reset; `eventsSinceReset` exposes it.
  - **Scoring cost.** Task 17's `us per window`, and that `s7comm-score` reads a side output of `s7comm-features`, so it back-pressures S7 features the way Modbus scoring does.
  - **Stage 2 (`attack_mode_router_v1_r2`) is not wired**; it is the next S7 unit.
- **Known limits, multi-protocol seams:** add "The two scoring units are parallel code (spec D2): `S7commScoreWindow` beside `ModbusScoreWindow`, `S7commScoringProcessFunction` beside `ModbusScoringProcessFunction`, two bundle loaders. A third detector is the point to generalise."
- **"Scored today":** "Modbus and S7comm, each by its Stage 1 detector only."
- **Design records:** add the S7comm scoring spec.

- [ ] **Step 2: Update `deploy/README.md` and `docs/clickhouse.md`**

`deploy/README.md`:
- Replace the line "S7 is not scored yet, and the older `predictions` table stays empty." with "S7comm is scored by its Stage 1 detector too; the older `predictions` table stays empty."
- In the Scoring section, after the Modbus paragraph, add:
```markdown
S7comm is scored the same way by its Stage 1 LSTM autoencoder, into
`netsec.s7comm.prediction.v1` and ClickHouse `s7comm_detector_predictions`
(design: `docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md`).
Its bundle is pinned by `S7COMM_DETECTOR_BUNDLE` (default
`s7comm-stage1-detector/v1`); package it once from the S7 delivery:

    ./deploy/models/package-s7comm-detector.sh models/S7

It checks every file against the release's own `FROZEN_MANIFEST.json`. A
connection's first 15 events are `WARMUP`, and decisions before about its 64th
event carry the model's post-reset false-alarm rate: `events_since_reset`
tells them apart. Set `S7COMM_DETECTOR_BUNDLE=` (empty) to run S7 features only.
```
`docs/clickhouse.md`:
- Add a table row after the Modbus one: `| s7comm_detector_predictions | ReplacingMergeTree(row_version) | toYYYYMMDD(event_time) | (model_name, model_version, event_time, event_id) | 180 days | archive job, when S7comm scoring is on |`.
- Change the sentence that counts the DDL files so it counts `004_s7comm_detector_predictions.sql` too.
- In the IP-bearing paragraph, say that `s7comm_detector_predictions` carries `client_ip` and `server_ip` too, by the same owner decision.

- [ ] **Step 3: Commit the documentation**

```bash
git add CLAUDE.md deploy/README.md docs/clickhouse.md
git commit -m "docs(s7comm): the S7comm scoring unit, its inputs and its real-traffic result

<attribution lines>"
```

- [ ] **Step 4: Full verification, one reactor run with every container class excluded**

```bash
find modules -path '*/target/surefire-reports' -type d -prune -exec rm -rf {} +
./mvnw verify -Dtest='!ArchiveJobE2ETest,!ClickHouseOutageTest,!ClientV2InserterTest,!DdlMigrationTest,!FeatureVectorDeduplicationTest,!OnlineFeatureJobE2ETest,!ClickHouseTestSupport' \
  -Dsurefire.failIfNoSpecifiedTests=false > "$SP/verify.log" 2>&1; echo "exit=$?"; grep -E 'BUILD|Tests run:.*Fail' "$SP/verify.log" | tail -5
for m in modules/*/; do
  r="${m}target/surefire-reports"; [ -d "$r" ] || continue
  sum() { grep -ho '<testsuite [^>]*' "$r"/TEST-*.xml | grep -o " $1=\"[0-9]*\"" | grep -o '[0-9]*' | awk '{s+=$1} END {print s+0}'; }
  echo "$(basename "$m") tests=$(sum tests) failures=$(sum failures) errors=$(sum errors) skipped=$(sum skipped)"
done
bash deploy/tests/run-all.sh | tail -1
```
Expected: `exit=0`, `BUILD SUCCESS`; every module `failures=0 errors=0 skipped=0`; `run-all: every check passed`. Put these counts in Step 1's verification table, then amend the docs commit (`git commit --amend --no-edit` after `git add CLAUDE.md`). Do not amend any other commit.

---

### Task 19: Roll out to server3 and verify live (only after the owner approves the merge)

**Files:** none in the repository beyond CLAUDE.md's live paragraph.

This task starts only after the final whole-branch review, its fixes, and the owner's explicit choice to merge `feat/s7comm-scoring` into `feat/deploy-mvp` and push. A merge and a push are shared-branch actions that the owner decides (superpowers:finishing-a-development-branch).

- [ ] **Step 1: Merge and push (as approved)**

```bash
git switch feat/deploy-mvp && git merge --ff-only feat/s7comm-scoring && git push && git branch -d feat/s7comm-scoring
```
Expected: a fast-forward, then a push to `origin/feat/deploy-mvp`.

- [ ] **Step 2: Package the bundle and copy it to the server**

```bash
bash deploy/models/package-s7comm-detector.sh models/S7
scp -r models/s7comm-stage1-detector server3:/root/mvp-project/backup-project/models/
ssh server3 'cd /root/mvp-project/backup-project && sha256sum models/s7comm-stage1-detector/v1/*'
```
Expected: `packaged models/s7comm-stage1-detector/v1`; the server's hashes equal `bundle.json`'s.

- [ ] **Step 3: Build and restart on the server**

```bash
ssh server3 'cd /root/mvp-project/backup-project && git pull --ff-only && ./deploy/deploy.sh build --jars-only > /root/build-s7.log 2>&1; echo BUILD_EXIT=$?'
ssh server3 'cd /root/mvp-project/backup-project && ./deploy/deploy.sh restart > /root/restart-s7.log 2>&1; echo RESTART_EXIT=$?; tail -25 /root/restart-s7.log'
```
Expected:
- `BUILD_EXIT=0` and `RESTART_EXIT=0`;
- the log shows both jobs resumed from their savepoints, 14 topics, and DDL `004` applied;
- `status` lists both jobs RUNNING.

Then check `s7comm-score` via the Flink REST API (`deploy.sh status`, or `/jobs/<id>/exceptions` through the job's REST port on the server): it must be RUNNING with 0 exceptions.

- [ ] **Step 4: Selftest**

Run: `ssh server3 'cd /root/mvp-project/backup-project && ./deploy/deploy.sh selftest 2>&1 | tail -3'`
Expected: `selftest PASSED: 2 Modbus and 2 S7comm feature vectors, 2 Modbus predictions, 2 S7comm predictions reached ClickHouse, no DLQ rows (test rows removed)`.

- [ ] **Step 5: A live S7 connection, scored as Python scores it**

Write `$SP/s7live.py`:
```python
#!/usr/bin/env python3
"""Throwaway (plan Task 19): a 32-event pipelined S7 read poll, as ICSNPP writes
it, for the live check. Writes it for the producer, and prints what the
delivered Python runtime scores each event."""
import importlib.util
import json
import sys
import time
from pathlib import Path

# The oracle generator's own helpers and reference runtime.
spec = importlib.util.spec_from_file_location("oracle", "tests/fixtures/s7comm/generate_detector_oracle.py")
oracle = importlib.util.module_from_spec(spec)
sys.modules["oracle"] = oracle
spec.loader.exec_module(oracle)
oracle.CLIENT, oracle.SERVER = ("192.0.2.30", 50400), ("192.0.2.40", 102)

uid, out = sys.argv[1], Path(sys.argv[2])
records = oracle.pipelined(uid, 16, lambda i: oracle.READ, time.time())
out.write_text("\n".join(json.dumps(r) for r in records) + "\n")
reference = oracle.Reference(Path("models/S7"))
try:
    for line in reference.run(records):
        print(line["events_since_reset"], line["verdict"], line["score"])
finally:
    reference.close()
```
Then:
```bash
UID_LIVE="S7LIVE-$(date -u +%Y%m%d%H%M%S)"
"$SP/venv/bin/python" "$SP/s7live.py" "$UID_LIVE" "$SP/s7live.jsonl" | tee "$SP/s7live-python.txt"
scp "$SP/s7live.jsonl" server3:/root/s7live.jsonl
ssh server3 "cd /root/mvp-project/backup-project && docker compose -p netsec-ml -f deploy/docker-compose.yml --env-file deploy/.env exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic netsec.s7comm.raw.v1 < /root/s7live.jsonl && rm /root/s7live.jsonl"
sleep 90   # the archive job writes on its 30 s checkpoints
ssh server3 "cd /root/mvp-project/backup-project && ./deploy/deploy.sh sql \"SELECT events_since_reset, verdict, score FROM s7comm_detector_predictions FINAL WHERE connection_uid = '$UID_LIVE' ORDER BY events_since_reset\""
```
Expected: 32 rows. The first 15 are `WARMUP` with a null score. Every scored row's verdict equals Python's, and its score equals Python's within 1e-4 relative (compare with `$SP/s7live-python.txt`).

- [ ] **Step 6: Clean up the server**

```bash
ssh server3 "cd /root/mvp-project/backup-project && ./deploy/deploy.sh sql \"ALTER TABLE feature_vectors DELETE WHERE connection_uid = '$UID_LIVE' SETTINGS mutations_sync = 1\" && ./deploy/deploy.sh sql \"ALTER TABLE s7comm_detector_predictions DELETE WHERE connection_uid = '$UID_LIVE' SETTINGS mutations_sync = 1\" && ./deploy/deploy.sh sql \"SELECT (SELECT count() FROM feature_vectors WHERE connection_uid LIKE 'S7LIVE-%') + (SELECT count() FROM s7comm_detector_predictions WHERE connection_uid LIKE 'S7LIVE-%')\" && rm -f /root/build-s7.log /root/restart-s7.log"
```
Expected: the final count is `0`.

- [ ] **Step 7: Record the live result**

In `CLAUDE.md`'s "deployed stack, verified live on server3" paragraph, add a sentence for 2026-09-28 or the rollout date: S7comm scoring rolled out; both jobs resumed; selftest passed with both pairs' predictions; a 32-event live connection gave 15 `WARMUP` rows and 17 scored rows equal to Python ONNX Runtime's. Update the memory file `project_modbus_scoring_followups.md`, or add an S7 one, with what is live and what remains open. Then:
```bash
git add CLAUDE.md && git commit -m "docs(s7comm): live on server3

<attribution lines>" && git push
```
