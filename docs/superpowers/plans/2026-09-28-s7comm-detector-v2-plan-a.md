# S7comm Stage 1 Detector v2 — Plan A: Build and Prove on Normal Traffic

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Train our own S7comm Stage 1 detector (v2) on four real sources of normal S7 polling, prove it keeps false alarms under 1% on held-out and unseen normal traffic, and release it in the delivered bundle format.

**Architecture:** A Java CLI (`S7commFeatureExport`) runs Zeek `s7comm.log` records through the platform's own parser, mapper and feature use case and writes CSV. A Python package, `training/src/netsec_ml/s7comm/`, loads those rows and splits each source by time. It fits the delivered preprocessing recipe, trains the delivered LSTM-autoencoder architecture, calibrates per-group conformal thresholds, evaluates the false-alarm gates, and writes a release directory. On server3, `acquire.sh` fetches and SHA-verifies every capture and runs Zeek and the exporter; the pipeline runs in a throwaway CPU-PyTorch container.

**Tech Stack:** Java 21 (the existing reactor), Python 3.12, PyTorch 2.5.1 (CPU), scikit-learn, onnx, onnxruntime, pandas, numpy, PyYAML, pytest, bash, Docker on server3.

**Spec:** `docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md` (read it; this plan argues from it).

## Global Constraints

- Branch `feat/s7comm-scoring` (holds the v2 spec at `aa489b3`). Java package root `io.netsecml.platform`.
- The Python side never computes a feature. Every value comes from `S7commFeatureExport` (the `training/` project's hard rule).
- PyTorch is allowed in `training/` only, and only the CPU build. The online job keeps ONNX Runtime only.
- Every code block carries a short comment saying why (the codebase's convention and the user's preference).
- Java tests: `./mvnw test -pl <module> -am -Dtest='<Class>' -Dsurefire.failIfNoSpecifiedTests=false`, then read the actual `Tests run:` line. Never run Testcontainers tests or start the deploy stack on this machine.
- Python tests run in `training/.venv` (Python 3.12, created in Task 2): `training/.venv/bin/pytest training/tests/unit/s7comm -q`.
- server3 (`ssh server3`):
  - data under `/root/s7data/v2`, code under `/root/s7work`; never inside the deployed checkout `/root/mvp-project/backup-project`;
  - `/root/1405-06-15/` (another project's) is read only;
  - never touch other projects' containers and never reboot;
  - training containers run with `--cpus 16 --memory 24g`.
- The model contract is the delivered one (spec section 3): 16 raw features in `s7comm-feature-v1` order; the `StableNumericTransformer` recipe; an LSTM autoencoder with hidden 32 and latent 16; a 16-event window; last-event weighted squared error, with weight 0 on the `s7_operation` columns; per-group conformal decision.
- Gates (spec section 7):
  - **G1:** every normal source's held-out test part (roles `normal` and `normal-test`) is at least **99% NORMAL** after the connection's 64th event.
  - **G3:** ONNX and PyTorch reconstructions agree within **1e-4**.
- Commit messages end with these two lines; `<attribution lines>` in a commit step means exactly these:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013kqk5fXA8U4yjTnCUbsnZw
  ```

## Rulings this plan makes

- **A1 — Scope.** Plan A covers normal-traffic training, the false-alarm gate G1 and the parity gate G3. The labelled-attack measurement (G2) and the published-model comparison (spec section 8) are Plan B, by the owner's decision on 2026-09-28.
- **A2 — The one-hot width follows the fitted categories.** It may differ from the delivered 21. Every consumer (the Java loader and scorer of the scoring plan) reads the width from the bundle. The spec's "16 → 21" describes the delivered model.
- **A3 — PyTorch is pinned to 2.5.1.** Its TorchScript-based ONNX exporter takes named inputs and outputs and a dynamic batch axis, which the delivered bundle format uses.
- **A4 — Release layout.** The release directory is `models/stage1_anomaly/<release_id>/` with `FROZEN_MANIFEST.json` and `artifacts/model/` holding the files; the ONNX file is `s7comm_lstm_autoencoder.onnx`. When the scoring plan resumes, its packaging script takes the release directory and file names as arguments (Task 11 records this revision).
- **A5 — The score group comes from the operation code** (feature 15) and `is_request_direction` (feature 12), exactly as the Java scorer derives it (scoring spec D5).
- **A6 — The delivered preprocessing, model and loss code is ported verbatim** into `training/`, with attribution and a parity test that runs whenever `models/S7/` is present.

## Review Focus

1. A connection that spans a split boundary: a training window must never mix parts; a scoring window may. Pinned in Task 5 (`test_a_training_window_never_mixes_parts`).
2. A capture the config names whose export file is missing must fail with an error naming the file, never silently train on less. Pinned in Task 3 (`test_a_missing_export_is_named`).
3. A category seen only in validation or test (unseen at fit time) must one-hot to all zeros without error. Pinned in Task 4 (`test_an_unseen_category_is_all_zeros`).
4. A score group with no calibration scores (usually OTHER_REQUEST) must use the pooled scores and the fallback alpha, both when calibrating and when deciding. Pinned in Task 6 (`test_a_group_without_scores_uses_the_pooled_fallback`).
5. An export row with an empty function code and the `__MISSING__` operation (an ACK PDU) must load and score without NaN. Pinned in Task 3 (`test_an_ack_row_loads_without_nan`).

---

### Task 1: `S7commFeatureExport`, the platform's features as CSV

**Files:**
- Create: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/S7commFeatureExport.java`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/S7commFeatureExportTest.java`

**Interfaces:**
- Consumes: `JsonZeekS7commParser`, `S7commEventMapper`, `S7commBuildFeaturesUseCase`, `S7commConnectionState`, `S7commCategories` (all existing).
- Produces:
  - `S7commFeatureExport.COLUMNS`: `capture, uid, ts, event_id, client_ip, server_ip, is_request, function_code, fresh, rosctr, operation, f0 … f15, quality_flags`;
  - `static Summary export(String capture, List<String> lines, Appendable rows, Appendable rejects)`;
  - `main`, taking `--out FILE --rejects FILE CAPTURE=LOG [CAPTURE=LOG …]`, with exit status 0 (ok), 2 (usage or I/O) or 3 (no rows).

  Tasks 3 and 9 read this CSV.

- [ ] **Step 1: Write the failing tests**

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The exporter on REAL ICSNPP output (tests/fixtures/zeek/): one row per record,
// the use case's own vector per connection, the fresh flag, the client side,
// and rejects kept apart with their reason.
class S7commFeatureExportTest {

    private static final Path SAMPLE = Path.of("..", "..", "tests", "fixtures", "zeek",
        "icsnpp-s7comm-7ebeb03_s7comm.jsonl");

    private static List<String> sample() throws Exception {
        return Files.readAllLines(SAMPLE, StandardCharsets.UTF_8);
    }

    // The data rows of an export, split into fields (header dropped).
    private static List<String[]> rows(String csv) {
        List<String[]> out = new ArrayList<>();
        String[] lines = csv.split("\n");
        for (int i = 1; i < lines.length; i++) {
            out.add(lines[i].split(",", -1));
        }
        return out;
    }

    private static String export(List<String> lines, StringBuilder rejects) throws Exception {
        StringBuilder rows = new StringBuilder(String.join(",", S7commFeatureExport.COLUMNS)).append('\n');
        S7commFeatureExport.export("sample", lines, rows, rejects);
        return rows.toString();
    }

    @Test
    void theIcsnppSampleExportsOneRowPerRecord() throws Exception {
        StringBuilder rows = new StringBuilder();
        StringBuilder rejects = new StringBuilder();
        S7commFeatureExport.Summary s = S7commFeatureExport.export("sample", sample(), rows, rejects);
        assertEquals(84, s.rows());
        assertEquals(0, s.rejected());
        assertEquals(84, rows.toString().split("\n").length);
        assertEquals("", rejects.toString());
        assertEquals(28, S7commFeatureExport.COLUMNS.size(), "11 fields, 16 values, the flags");
    }

    // The values are exactly what S7commBuildFeaturesUseCase gives when fed each
    // uid's records in order, as S7commFeatureProcessFunction feeds them.
    @Test
    void eachRowCarriesTheUseCasesVectorForItsConnection() throws Exception {
        List<String[]> rows = rows(export(sample(), new StringBuilder()));
        S7commBuildFeaturesUseCase useCase = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        int i = 0;
        for (String line : sample()) {
            MappingResult<ZeekS7commRecord> parsed = new JsonZeekS7commParser().parse(
                line.getBytes(StandardCharsets.UTF_8));
            NetworkEvent mapped = new S7commEventMapper().map(parsed.value(), new SensorId("x")).value();
            S7commEvent event = (S7commEvent) mapped;
            S7commConnectionState state = states.computeIfAbsent(event.connectionUid(),
                u -> S7commConnectionState.empty());
            float[] expected = useCase.build(event, state).vector().values();
            String[] row = rows.get(i++);
            for (int f = 0; f < 16; f++) {
                assertEquals(expected[f], Float.parseFloat(row[11 + f]), 0f, "row " + i + " f" + f);
            }
        }
    }

    @Test
    void freshMarksEachConnectionsFirstRowOnly() throws Exception {
        Set<String> seen = new HashSet<>();
        for (String[] row : rows(export(sample(), new StringBuilder()))) {
            assertEquals(seen.add(row[1]) ? "1" : "0", row[8], "uid " + row[1]);
        }
    }

    // A request goes to port 102, so its sender is the client; a response's
    // receiver is. Every row of one connection names the same client and server.
    @Test
    void theClientIsTheSideThatSendsToPort102() throws Exception {
        Map<String, String> clientOf = new HashMap<>();
        for (String[] row : rows(export(sample(), new StringBuilder()))) {
            String previous = clientOf.putIfAbsent(row[1], row[4] + ">" + row[5]);
            assertTrue(previous == null || previous.equals(row[4] + ">" + row[5]), "uid " + row[1]);
        }
    }

    @Test
    void aRecordThePipelineRejectsGoesToRejectsWithItsReason() throws Exception {
        List<String> lines = new ArrayList<>(sample());
        lines.add("{not json");
        StringBuilder rejects = new StringBuilder();
        S7commFeatureExport.Summary s = S7commFeatureExport.export("sample", lines, new StringBuilder(), rejects);
        assertEquals(84, s.rows());
        assertEquals(1, s.rejected());
        assertTrue(rejects.toString().contains("\"capture\":\"sample\""), rejects.toString());
        assertTrue(rejects.toString().contains("\"reason\""), rejects.toString());
    }

    @Test
    void runWritesTheHeaderAndEveryCapture(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("f.csv");
        Path rejects = dir.resolve("r.jsonl");
        int status = S7commFeatureExport.run(new String[]{"--out", out.toString(), "--rejects", rejects.toString(),
            "a=" + SAMPLE, "b=" + SAMPLE}, new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, status);
        List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        assertEquals(String.join(",", S7commFeatureExport.COLUMNS), lines.get(0));
        assertEquals(1 + 84 + 84, lines.size());
    }

    @Test
    void runRefusesBadArguments(@TempDir Path dir) throws Exception {
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream());
        assertEquals(2, S7commFeatureExport.run(new String[]{}, quiet));
        assertEquals(2, S7commFeatureExport.run(new String[]{"--out", dir.resolve("f.csv").toString(), "--rejects",
            dir.resolve("r").toString(), "a=" + dir.resolve("missing.log")}, quiet));
        assertEquals(2, S7commFeatureExport.run(new String[]{"--out", dir.resolve("f.csv").toString(), "--rejects",
            dir.resolve("r").toString(), "no-equals-sign"}, quiet));
        Files.writeString(dir.resolve("empty.log"), "");
        assertEquals(3, S7commFeatureExport.run(new String[]{"--out", dir.resolve("f.csv").toString(), "--rejects",
            dir.resolve("r").toString(), "a=" + dir.resolve("empty.log")}, quiet));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='S7commFeatureExportTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class S7commFeatureExport`.

- [ ] **Step 3: Implement**

```java
package io.netsecml.platform.bootstrap.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

// Exports S7comm feature vectors for training
// (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md section 5):
// every line of a Zeek s7comm.log goes through the online job's own parser,
// mapper and S7commBuildFeaturesUseCase, with one connection state per uid
// exactly as S7commFeatureProcessFunction keeps it, and becomes one CSV row.
// Training reads these rows and never computes a feature itself, so a model
// and the live scorer cannot disagree about what a feature is.
public final class S7commFeatureExport {

    // The CSV columns, in order: identity and context, then f0..f15 (the
    // s7comm-feature-v1 values), then the vector's quality flags.
    public static final List<String> COLUMNS = columns();

    // How many rows one capture gave, and how many records it rejected.
    public record Summary(int rows, int rejected) {
    }

    // The sensor id only labels event ids.
    private static final SensorId SENSOR = new SensorId("s7comm-feature-export");
    private static final ObjectMapper JSON = new ObjectMapper();

    private S7commFeatureExport() {
    }

    private static List<String> columns() {
        List<String> c = new ArrayList<>(List.of("capture", "uid", "ts", "event_id", "client_ip", "server_ip",
            "is_request", "function_code", "fresh", "rosctr", "operation"));
        for (int i = 0; i < 16; i++) {
            c.add("f" + i);
        }
        c.add("quality_flags");
        return List.copyOf(c);
    }

    // One capture's records, in order, with one connection state per uid. A
    // rejected record becomes one JSON line in `rejects`, with its reason.
    public static Summary export(String capture, List<String> lines, Appendable rows, Appendable rejects)
            throws IOException {
        if (capture.isEmpty() || capture.contains(",")) {
            throw new IllegalArgumentException("a capture name must be non-empty and contain no comma: " + capture);
        }
        S7commBuildFeaturesUseCase useCase = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        int written = 0;
        int rejected = 0;
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            MappingResult<NetworkEvent> mapped = parseAndMap(line);
            if (!mapped.isValid()) {
                rejected++;
                ObjectNode reject = JSON.createObjectNode();
                reject.put("capture", capture);
                reject.put("reason", String.valueOf(mapped.reason()));
                reject.put("detail", mapped.detail());
                reject.put("line", line);
                rejects.append(JSON.writeValueAsString(reject)).append('\n');
                continue;
            }
            S7commEvent event = narrow(mapped.value());
            // As S7commFeatureProcessFunction: absent state is a fresh connection.
            S7commConnectionState state = states.get(event.connectionUid());
            boolean fresh = state == null;
            FeatureBuildResult<S7commConnectionState> built =
                useCase.build(event, fresh ? S7commConnectionState.empty() : state);
            states.put(event.connectionUid(), built.newState());
            rows.append(row(capture, event, fresh, built.vector().values(), built.vector().qualityFlags()))
                .append('\n');
            written++;
        }
        return new Summary(written, rejected);
    }

    // The online job's two stages: parse, then map.
    private static MappingResult<NetworkEvent> parseAndMap(String line) {
        MappingResult<ZeekS7commRecord> parsed = new JsonZeekS7commParser().parse(line.getBytes(StandardCharsets.UTF_8));
        return parsed.isValid()
            ? new S7commEventMapper().map(parsed.value(), SENSOR)
            : MappingResult.invalid(parsed.reason(), parsed.detail());
    }

    // S7commEventMapper only ever produces an S7commEvent; any other is a wiring error.
    private static S7commEvent narrow(NetworkEvent event) {
        return switch (event) {
            case S7commEvent s7 -> s7;
            case ConnEvent ignored -> throw new IllegalStateException("S7commEventMapper produced a ConnEvent");
            case DnsEvent ignored -> throw new IllegalStateException("S7commEventMapper produced a DnsEvent");
            case ModbusEvent ignored -> throw new IllegalStateException("S7commEventMapper produced a ModbusEvent");
        };
    }

    // One CSV row. Floats print with Float.toString, which reads back exactly;
    // the categories print as S7commCategories decodes them.
    private static String row(String capture, S7commEvent event, boolean fresh, float[] v, int flags) {
        String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
        String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
        StringBuilder b = new StringBuilder();
        b.append(capture).append(',').append(event.connectionUid()).append(',')
            .append(String.format(Locale.ROOT, "%.6f", event.tsSeconds())).append(',')
            .append(event.eventId().value()).append(',').append(client).append(',').append(server).append(',')
            .append(event.isRequest() ? 1 : 0).append(',')
            .append(event.functionCode() == null ? "" : event.functionCode().toString()).append(',')
            .append(fresh ? 1 : 0).append(',')
            .append(S7commCategories.decodeRosctr((int) v[14])).append(',')
            .append(S7commCategories.decodeOperation((int) v[15]));
        for (float value : v) {
            b.append(',').append(Float.toString(value));
        }
        return b.append(',').append(flags).toString();
    }

    // usage: S7commFeatureExport --out FILE --rejects FILE CAPTURE=LOG [CAPTURE=LOG ...]
    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    // Exit status: 0 when rows were written; 2 for bad arguments or I/O; 3 when
    // the logs held no records at all, so nothing was exported.
    static int run(String[] args, PrintStream out) {
        if (args.length < 5 || !args[0].equals("--out") || !args[2].equals("--rejects")) {
            out.println("usage: S7commFeatureExport --out FILE --rejects FILE CAPTURE=LOG [CAPTURE=LOG ...]");
            return 2;
        }
        int total = 0;
        try (Writer rows = Files.newBufferedWriter(Path.of(args[1]), StandardCharsets.UTF_8);
             Writer rejects = Files.newBufferedWriter(Path.of(args[3]), StandardCharsets.UTF_8)) {
            rows.append(String.join(",", COLUMNS)).append('\n');
            for (int i = 4; i < args.length; i++) {
                int eq = args[i].indexOf('=');
                if (eq <= 0) {
                    out.println("expected CAPTURE=LOG, got " + args[i]);
                    return 2;
                }
                String capture = args[i].substring(0, eq);
                List<String> lines = Files.readAllLines(Path.of(args[i].substring(eq + 1)), StandardCharsets.UTF_8);
                Summary s = export(capture, lines, rows, rejects);
                out.printf("%s: %d rows, %d rejected%n", capture, s.rows(), s.rejected());
                total += s.rows();
            }
        } catch (IOException | IllegalArgumentException e) {
            out.println("cannot export: " + e.getMessage());
            return 2;
        }
        return total == 0 ? 3 : 0;
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `Tests run: 7, Failures: 0, Errors: 0`.

- [ ] **Step 5: The shaded JAR carries it**

Run: `./mvnw -q package -pl modules/bootstrap-online-job -am -DskipTests && java -cp modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar io.netsecml.platform.bootstrap.online.S7commFeatureExport --out /tmp/claude-1000/s7x.csv --rejects /tmp/claude-1000/s7x.rej sample=tests/fixtures/zeek/icsnpp-s7comm-7ebeb03_s7comm.jsonl; echo "exit=$?"; head -2 /tmp/claude-1000/s7x.csv | cut -c1-160; rm -f /tmp/claude-1000/s7x.csv /tmp/claude-1000/s7x.rej`
Expected: `sample: 84 rows, 0 rejected`, `exit=0`, then the header and one data row.

- [ ] **Step 6: Commit**

```bash
git add modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/S7commFeatureExport.java \
  modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/S7commFeatureExportTest.java
git commit -m "feat(s7comm): export the platform's S7 feature vectors for training

<attribution lines>"
```

---

### Task 2: The training environment

**Files:**
- Modify: `training/pyproject.toml` (optional dependencies `s7comm`, `dev`)
- Create: `training/docker/s7comm.Dockerfile`
- Create: `training/src/netsec_ml/s7comm/__init__.py`
- Test: `training/tests/unit/s7comm/__init__.py`, `training/tests/unit/s7comm/test_environment.py`
- Modify: `CLAUDE.md` (the CPU-only invariant)

**Interfaces:**
- Produces: the local `training/.venv` (Python 3.12, CPU torch 2.5.1) and the server3 image `netsec-ml/s7-train:1` (built in Task 10 from this Dockerfile).

- [ ] **Step 1: Write the failing test**

`training/tests/unit/s7comm/test_environment.py`:
```python
"""The training environment itself: the pinned CPU PyTorch, and the package importable."""
import torch

import netsec_ml.s7comm


def test_torch_is_the_pinned_cpu_build():
    # Ruling A3: 2.5.1, CPU only (spec section 6).
    assert torch.__version__.startswith("2.5.1")
    assert not torch.cuda.is_available()


def test_the_package_is_importable():
    assert netsec_ml.s7comm.__doc__
```
And an empty `training/tests/unit/s7comm/__init__.py`.

- [ ] **Step 2: Create the venv and watch the test fail**

```bash
uv venv --python 3.12 training/.venv
uv pip install --python training/.venv/bin/python torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu
uv pip install --python training/.venv/bin/python pytest
training/.venv/bin/pytest training/tests/unit/s7comm -q
```
Expected: FAIL — `ModuleNotFoundError: No module named 'netsec_ml'`.

- [ ] **Step 3: Implement**

In `training/pyproject.toml`, replace the `[project.optional-dependencies]` table with:
```toml
[project.optional-dependencies]
# The S7comm Stage 1 detector v2 (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md).
# torch comes from the CPU index first (see training/README.md); this pin only records it.
s7comm = [
    "torch==2.5.1",
    "numpy>=1.26",
    "pandas>=2.2",
    "scikit-learn>=1.5",
    "joblib>=1.4",
    "onnx>=1.16",
    "onnxruntime>=1.19",
    "pyyaml>=6.0",
]
dev = [
    "pytest",
    "pytest-cov",
]
```

`training/src/netsec_ml/s7comm/__init__.py`:
```python
"""S7comm Stage 1 detector v2: training on the platform's own exported features
(docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md). Nothing in this
package computes a feature; every value comes from S7commFeatureExport."""
```

`training/docker/s7comm.Dockerfile`:
```dockerfile
# The throwaway training image for the S7comm detector v2 (spec section 6): CPU-only
# PyTorch and the scientific stack, nothing else. Built and run on server3 only.
FROM python:3.12-slim
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir "numpy>=1.26" "pandas>=2.2" "scikit-learn>=1.5" "joblib>=1.4" \
      "onnx>=1.16" "onnxruntime>=1.19" "pyyaml>=6.0" pytest
ENV PYTHONPATH=/work/src PYTHONUNBUFFERED=1
WORKDIR /work
```

Install the package and its extras into the venv:
```bash
uv pip install --python training/.venv/bin/python -e 'training[s7comm,dev]'
```

In `CLAUDE.md`, replace the invariant line
`- CPU-only: no GPU, no CUDA, no deep-learning frameworks. ONNX Runtime Java with intra/inter-op threads pinned to 1 per subtask.`
with:
```
- CPU-only: no GPU, no CUDA. No deep-learning framework on the serving path: the online job runs
  ONNX Runtime Java only, with intra/inter-op threads pinned to 1 per subtask. CPU-only PyTorch is
  allowed in `training/` alone, to train the S7comm detector (2026-09-28, owner's decision; spec
  `docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md` section 6).
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm -q`
Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add training/pyproject.toml training/docker/s7comm.Dockerfile training/src/netsec_ml/s7comm/__init__.py \
  training/tests/unit/s7comm/__init__.py training/tests/unit/s7comm/test_environment.py CLAUDE.md
git commit -m "build(training): the CPU PyTorch environment for the S7comm detector v2

<attribution lines>"
```

---
### Task 3: Loading the exports, sources and time splits

**Files:**
- Create: `training/src/netsec_ml/s7comm/data.py`
- Create: `training/configs/s7comm-v2.yaml`
- Test: `training/tests/unit/s7comm/test_data.py`

**Interfaces:**
- Consumes: the CSV of Task 1.
- Produces, in `netsec_ml.s7comm.data`:
  - constants `FEATURES`, `CONTINUOUS`, `BINARY`, `CATEGORICAL`, `EXPORT_COLUMNS`, `ROLES`;
  - `Source(name, captures, clients, role)`;
  - `load_config(path) -> dict`, `sources_of(config) -> list[Source]`, `load_export(path) -> DataFrame`, `load_sources(config, sources) -> DataFrame` and `split_by_time(data, split) -> Series`.

  The loaded frame has columns `capture, uid, ts, event_id, client_ip, server_ip, fresh, quality_flags`, the 16 `FEATURES`, `operation_code`, `event_index`, `source` and `role`.

- [ ] **Step 1: Write the failing tests**

`training/tests/unit/s7comm/test_data.py`:
```python
"""Loading S7commFeatureExport rows, claiming them for sources, and splitting by time."""
import numpy as np
import pytest

from netsec_ml.s7comm import data as D

HEADER = ",".join(D.EXPORT_COLUMNS)


def row(capture, uid, ts, client="10.0.0.1", request=1, rosctr="1", operation="READ_VAR", code="4", fresh=0):
    """One export row as S7commFeatureExport writes it; values zero except the ones the
    categories and direction imply."""
    values = [0.0] * 16
    values[12] = float(request)
    values[14] = float(rosctr) if rosctr.isdigit() else -1.0
    values[15] = float(code) if code else -1.0
    fields = [capture, uid, f"{ts:.6f}", f"s:{uid}:{ts}", client, "10.0.0.9", str(request), code, str(fresh),
              rosctr, operation, *[repr(v) for v in values], "0"]
    return ",".join(fields)


def write(directory, capture, rows):
    (directory / f"{capture}.csv").write_text(HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def config(directory, sources):
    return {"features_dir": str(directory), "sources": sources,
            "split": {"train": 0.70, "validation": 0.15, "gap_events": 64, "gap_fraction": 0.01}}


def test_an_export_loads_under_contract_names(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0), row("c1", "u1", 2.0, request=0, rosctr="3")])
    df = D.load_export(tmp_path / "c1.csv")
    assert list(df[list(D.FEATURES[:14])].dtypes.unique()) == [np.dtype("float64")]
    assert list(df["s7_operation"]) == ["READ_VAR", "READ_VAR"]
    assert list(df["s7_rosctr"]) == ["1", "3"]
    assert list(df["operation_code"]) == [4.0, 4.0]


def test_a_file_with_other_columns_is_refused(tmp_path):
    (tmp_path / "c1.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="S7commFeatureExport"):
        D.load_export(tmp_path / "c1.csv")


# Review Focus 2: a named capture with no export fails, naming the file.
def test_a_missing_export_is_named(tmp_path):
    cfg = config(tmp_path, {"s": {"captures": ["nowhere"], "role": "normal"}})
    with pytest.raises(FileNotFoundError, match="nowhere.csv"):
        D.load_sources(cfg, D.sources_of(cfg))


# Review Focus 5: an ACK (no function code, operation __MISSING__) loads with no NaN.
def test_an_ack_row_loads_without_nan(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, request=0, rosctr="2", operation="__MISSING__", code="")])
    df = D.load_export(tmp_path / "c1.csv")
    assert not df[list(D.FEATURES[:14])].isna().any().any()
    assert df["s7_operation"][0] == "__MISSING__"


def test_sources_claim_rows_by_capture_and_client_and_the_first_wins(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, client="A"), row("c1", "u2", 1.5, client="B")])
    cfg = config(tmp_path, {"hmi": {"captures": ["c1"], "clients": ["A"], "role": "normal"},
                            "rest": {"captures": ["c1"], "role": "report"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    assert dict(zip(data["client_ip"], data["source"])) == {"A": "hmi", "B": "rest"}
    assert dict(zip(data["source"], data["role"])) == {"hmi": "normal", "rest": "report"}


def test_unclaimed_rows_have_no_source_or_role(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, client="A"), row("c1", "u2", 1.5, client="Z")])
    cfg = config(tmp_path, {"hmi": {"captures": ["c1"], "clients": ["A"], "role": "normal"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    unclaimed = data[data["client_ip"] == "Z"]
    assert list(unclaimed["source"]) == [""] and list(unclaimed["role"]) == [""]


def test_the_event_index_counts_within_each_connection(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0), row("c1", "u2", 1.1), row("c1", "u1", 1.2)])
    cfg = config(tmp_path, {"s": {"captures": ["c1"], "role": "normal"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    assert list(zip(data["uid"], data["event_index"])) == [("u1", 0), ("u2", 0), ("u1", 1)]


def test_a_normal_source_is_split_by_time_with_gaps(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", float(i)) for i in range(1000)])
    cfg = config(tmp_path, {"s": {"captures": ["c1"], "role": "normal"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    part = D.split_by_time(data, cfg["split"])
    # gap = min(64, 1% of 1000) = 10
    assert part.value_counts().to_dict() == {"train": 700, "validation": 140, "test": 140, "gap": 20}
    ts = data["ts"]
    assert ts[part == "train"].max() < ts[part == "validation"].min() < ts[part == "test"].min()


def test_other_roles_are_test_only(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", float(i)) for i in range(20)])
    cfg = config(tmp_path, {"s": {"captures": ["c1"], "role": "unseen"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    assert set(D.split_by_time(data, cfg["split"])) == {"test"}


def test_an_unknown_role_is_refused(tmp_path):
    with pytest.raises(ValueError, match="role"):
        D.sources_of(config(tmp_path, {"s": {"captures": ["c1"], "role": "training"}}))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm/test_data.py -q`
Expected: FAIL — `ImportError: cannot import name 'data'`.

- [ ] **Step 3: Implement**

`training/src/netsec_ml/s7comm/data.py`:
```python
"""S7comm training data: the rows S7commFeatureExport writes, claimed by logical sources and
split by time (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md sections 4-6).
Nothing here computes a feature: every value comes from the platform's own Java code."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

# s7comm-feature-v1, in contract order.
FEATURES = (
    "s7_outstanding_requests", "s7_outstanding_mean_16", "s7_response_match_rate_16",
    "s7_same_function_run_length", "s7_same_direction_run_length", "s7_request_ratio_16",
    "s7_direction_change_rate_16", "s7_function_change_rate_16", "s7_function_entropy_16",
    "s7_function_transition_entropy_16", "s7_rosctr_change_rate_16",
    "s7_pdu_reference_unique_ratio_32", "is_request_direction", "s7_function_changed",
    "s7_rosctr", "s7_operation",
)
CONTINUOUS = FEATURES[:12]
BINARY = FEATURES[12:14]
CATEGORICAL = FEATURES[14:]

# S7commFeatureExport.COLUMNS, in order.
EXPORT_COLUMNS = (
    "capture", "uid", "ts", "event_id", "client_ip", "server_ip", "is_request", "function_code",
    "fresh", "rosctr", "operation", *(f"f{i}" for i in range(16)), "quality_flags",
)

# normal: trained, calibrated and tested; normal-test and unseen: tested only (unseen clients are
# the generalisation evidence); report: scored and reported, never gated.
ROLES = ("normal", "normal-test", "unseen", "report")


@dataclass(frozen=True)
class Source:
    """A logical source: some captures' records (only some clients' when `clients` is set)."""
    name: str
    captures: tuple[str, ...]
    clients: tuple[str, ...]
    role: str


def load_config(path) -> dict:
    """The run configuration (training/configs/s7comm-v2.yaml)."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def sources_of(config: dict) -> list[Source]:
    """The configured sources, in config order; an unknown role is refused."""
    out = []
    for name, spec in config["sources"].items():
        if spec["role"] not in ROLES:
            raise ValueError(f"source {name}: role {spec['role']!r} is not one of {ROLES}")
        out.append(Source(name, tuple(spec["captures"]), tuple(spec.get("clients") or ()), spec["role"]))
    return out


def load_export(path) -> pd.DataFrame:
    """One capture's export: the 14 numeric features under their contract names, the two
    categories as the strings the Java decoder wrote, and the operation code (ruling A5)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"feature export {path} is missing: run training/s7comm/acquire.sh first")
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    if tuple(raw.columns) != EXPORT_COLUMNS:
        raise ValueError(f"{path}: columns are not S7commFeatureExport's: {list(raw.columns)}")
    df = pd.DataFrame({
        "capture": raw["capture"], "uid": raw["uid"], "ts": raw["ts"].astype("float64"),
        "event_id": raw["event_id"], "client_ip": raw["client_ip"], "server_ip": raw["server_ip"],
        "fresh": raw["fresh"].astype("int64"), "quality_flags": raw["quality_flags"].astype("int64"),
    })
    for i, name in enumerate(FEATURES[:14]):
        df[name] = raw[f"f{i}"].astype("float64")
    df["s7_rosctr"] = raw["rosctr"]
    df["s7_operation"] = raw["operation"]
    df["operation_code"] = raw["f15"].astype("float64")
    return df


def load_sources(config: dict, sources: list[Source]) -> pd.DataFrame:
    """Every capture any source names, each row tagged with its source ('' when none claims it),
    its role, and its 0-based event index within its connection (export order)."""
    captures = sorted({c for s in sources for c in s.captures})
    data = pd.concat([load_export(Path(config["features_dir"]) / f"{c}.csv") for c in captures],
                     ignore_index=True)
    data["event_index"] = data.groupby(["capture", "uid"]).cumcount()
    data["source"] = ""
    for s in sources:  # the first source in config order that claims a row keeps it
        claim = (data["source"] == "") & data["capture"].isin(s.captures)
        if s.clients:
            claim &= data["client_ip"].isin(s.clients)
        data.loc[claim, "source"] = s.name
    data["role"] = data["source"].map({s.name: s.role for s in sources}).fillna("")
    return data


def split_by_time(data: pd.DataFrame, split: dict) -> pd.Series:
    """train / validation / test / gap per row. Each normal source is ordered by timestamp and cut
    70/15/15 (as configured) with a gap of min(gap_events, gap_fraction x size) rows between parts;
    every other role is test only; unclaimed rows stay ''."""
    part = pd.Series("", index=data.index, dtype=object)
    for _, rows in data[data["role"] == "normal"].groupby("source"):
        order = rows.sort_values(["ts", "capture", "event_index"], kind="stable").index
        n = len(order)
        gap = min(int(split["gap_events"]), int(split["gap_fraction"] * n))
        a = int(n * split["train"])
        b = int(n * (split["train"] + split["validation"]))
        part.loc[order[:a]] = "train"
        part.loc[order[a:a + gap]] = "gap"
        part.loc[order[a + gap:b]] = "validation"
        part.loc[order[b:b + gap]] = "gap"
        part.loc[order[b + gap:]] = "test"
    part[data["role"].isin(["normal-test", "unseen", "report"])] = "test"
    return part
```

`training/configs/s7comm-v2.yaml` (paths are the training container's, Task 10):
```yaml
# S7comm Stage 1 detector v2 (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md).
# Captures are named as in training/s7comm/sources.sha256; each has <features_dir>/<capture>.csv.
release_id: v2_multisource_r1
features_dir: /data/features
sources:
  qut-control:     {captures: [qut-control], clients: [10.10.10.20], role: normal}
  4sics-hmi:       {captures: [4sics-151020, 4sics-151021, 4sics-151022], clients: [10.10.10.20], role: normal}
  server3-benign:  {captures: [server3-s7, server3-s701, server3-s702, server3-S7COMM], clients: [192.168.10.100], role: normal}
  libnodave-bench: {captures: [libnodave-bench], clients: [192.168.1.10], role: normal}
  qut-attack-hmi:  {captures: [qut-attack], clients: [10.10.10.20], role: normal-test}
  s7comm-clean:    {captures: [s7comm-clean], clients: [192.168.0.21], role: unseen}
  cyclic-1s:       {captures: [cyclic-1s], clients: [192.168.1.20], role: unseen}
  engineering:     {captures: [eng-readDiagData, eng-readVarTab, eng-plc-status, eng-blocklist, eng-download-db1, eng-plc-time, eng-snap7-everything], role: report}
  4sics-other:     {captures: [4sics-151020, 4sics-151021, 4sics-151022], clients: [10.10.10.30, 192.168.1.10, 192.168.2.42], role: report}
split: {train: 0.70, validation: 0.15, gap_events: 64, gap_fraction: 0.01}
model: {hidden: 32, latent: 16, sequence: 16, train_stride: 8}
training: {batch: 256, lr: 0.001, weight_decay: 1.0e-5, clip: 1.0, max_epochs: 40, patience: 7, seed: 42, threads: 16}
candidates:
  - {name: id0.5-equal, identity_weight: 0.5, balance: equal, write_fraction: 0.1}
  - {name: id0.0-equal, identity_weight: 0.0, balance: equal, write_fraction: 0.1}
  - {name: id0.5-proportional, identity_weight: 0.5, balance: proportional, write_fraction: 0.1}
calibration: {alpha: {RESPONSE: 0.001, READ_REQUEST: 0.001}, fallback_alpha: 0.001, min_calibration: 1000}
gates: {normal_rate_past_64: 0.99, onnx_parity: 1.0e-4}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command. Expected: `10 passed`.

- [ ] **Step 5: Commit**

```bash
git add training/src/netsec_ml/s7comm/data.py training/configs/s7comm-v2.yaml training/tests/unit/s7comm/test_data.py
git commit -m "feat(training): load S7 feature exports, claim sources, split by time

<attribution lines>"
```

---

### Task 4: The delivered preprocessing recipe, ported

**Files:**
- Create: `training/src/netsec_ml/s7comm/preprocessing.py`
- Test: `training/tests/unit/s7comm/test_preprocessing.py`

**Interfaces:**
- Consumes: `FEATURES`, `CONTINUOUS`, `BINARY`, `CATEGORICAL` (Task 3).
- Produces:
  - `StableNumericTransformer`, `build_preprocessor` and `export_preprocessor_contract` (verbatim upstream);
  - `BOUNDED` (the eight ratios);
  - `fit(train_frame) -> ColumnTransformer`;
  - `transform(pre, frame) -> np.ndarray[float32]`;
  - `write_contract(pre, path) -> dict`.

- [ ] **Step 1: Write the failing tests**

`training/tests/unit/s7comm/test_preprocessing.py`:
```python
"""The delivered StableNumericTransformer recipe, ported: its contract shape, its rules, and
parity with the delivered code when models/S7 is present."""
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from netsec_ml.s7comm import preprocessing as P
from netsec_ml.s7comm.data import BINARY, CATEGORICAL, CONTINUOUS, FEATURES

UPSTREAM = Path(__file__).resolve().parents[4] / "models/S7/src/s7zeek/modeling/preprocessing.py"


def frame(n=200, seed=0):
    """A training-like frame: counts, ratios in [0, 1], binaries, two categories each."""
    rng = np.random.default_rng(seed)
    f = pd.DataFrame({name: rng.uniform(0, 1, n) for name in CONTINUOUS})
    f["s7_outstanding_requests"] = rng.integers(0, 3, n).astype(float)
    f["s7_same_function_run_length"] = rng.integers(1, 2000, n).astype(float)
    f["is_request_direction"] = rng.integers(0, 2, n).astype(float)
    f["s7_function_changed"] = rng.integers(0, 2, n).astype(float)
    f["s7_rosctr"] = rng.choice(["1", "3"], n)
    f["s7_operation"] = rng.choice(["READ_VAR", "WRITE_VAR"], n)
    return f[list(FEATURES)]


def test_the_contract_has_the_delivered_shape(tmp_path):
    pre = P.fit(frame())
    c = P.write_contract(pre, tmp_path / "preprocessor_contract.json")
    assert c["raw_feature_order"] == list(FEATURES)
    assert c["continuous"]["transformer"] == "StableNumericTransformer"
    assert c["binary"]["imputer_statistics"] == [0.0, 0.0]
    assert c["categorical"]["categories"] == {"s7_rosctr": ["1", "3"], "s7_operation": ["READ_VAR", "WRITE_VAR"]}
    assert c["transformed_dimension"] == 12 + 2 + 2 + 2
    assert c["transformed_feature_order"][14] == "categorical__s7_rosctr_1"
    assert json.loads((tmp_path / "preprocessor_contract.json").read_text()) == c


def test_bounded_features_are_clipped_and_never_scaled():
    pre = P.fit(frame())
    f = frame(1, seed=1)
    f["s7_request_ratio_16"] = 1.7
    out = P.transform(pre, f)
    assert out.dtype == np.float32
    assert out[0, CONTINUOUS.index("s7_request_ratio_16")] == 1.0


# Review Focus 3: a category the fit never saw one-hot encodes as all zeros.
def test_an_unseen_category_is_all_zeros():
    pre = P.fit(frame())
    f = frame(1, seed=2)
    f["s7_rosctr"] = "2"
    out = P.transform(pre, f)
    assert list(out[0, 14:16]) == [0.0, 0.0]


@pytest.mark.skipif(not UPSTREAM.exists(), reason="models/S7 (the delivery) is not present on this machine")
def test_the_port_matches_the_delivered_code():
    spec = importlib.util.spec_from_file_location("upstream_preprocessing", UPSTREAM)
    up = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(up)
    train = frame()
    ours = P.fit(train)
    theirs = up.build_preprocessor(list(CONTINUOUS), list(BINARY), list(CATEGORICAL), bounded_ranges=P.BOUNDED)
    theirs.fit(train)
    probe = frame(50, seed=3)
    assert np.array_equal(P.transform(ours, probe), np.asarray(theirs.transform(probe), np.float32))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm/test_preprocessing.py -q`
Expected: FAIL — `ImportError: cannot import name 'preprocessing'`.

- [ ] **Step 3: Implement**

`training/src/netsec_ml/s7comm/preprocessing.py`: the three definitions `StableNumericTransformer`, `build_preprocessor` and `export_preprocessor_contract` are copied **verbatim** from `models/S7/src/s7zeek/modeling/preprocessing.py` (the delivery, SHA-256 `63d4c7ec…0891`). Leave out `save_preprocessor`, which is unused. Put them under this header, and add the platform functions below them:
```python
"""The delivered S7 Stage 1 preprocessing recipe (spec section 3), ported verbatim from the
model team's models/S7/src/s7zeek/modeling/preprocessing.py (sha256 63d4c7ec...0891; ruling A6):
StableNumericTransformer, build_preprocessor, export_preprocessor_contract. Below them, the
platform's three entry points."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from netsec_ml.s7comm.data import BINARY, CATEGORICAL, CONTINUOUS, FEATURES

# ---- verbatim from the delivery: class StableNumericTransformer, def build_preprocessor,
# ---- def export_preprocessor_contract (copy them here unchanged) ----


# The eight ratios the delivered contract keeps in their physical range [0, 1].
BOUNDED = {name: (0.0, 1.0) for name in (
    "s7_response_match_rate_16", "s7_request_ratio_16", "s7_direction_change_rate_16",
    "s7_function_change_rate_16", "s7_function_entropy_16", "s7_function_transition_entropy_16",
    "s7_rosctr_change_rate_16", "s7_pdu_reference_unique_ratio_32")}


def fit(train: pd.DataFrame) -> ColumnTransformer:
    """The delivered recipe (5-95 percentile span, 0.1 floor, clip 20, the eight bounded ratios),
    fitted on the training part only."""
    pre = build_preprocessor(list(CONTINUOUS), list(BINARY), list(CATEGORICAL), bounded_ranges=BOUNDED)
    pre.fit(train[list(FEATURES)])
    return pre


def transform(pre: ColumnTransformer, frame: pd.DataFrame) -> np.ndarray:
    """The model's input rows, float32, in the contract's transformed order."""
    return np.asarray(pre.transform(frame[list(FEATURES)]), dtype=np.float32)


def write_contract(pre: ColumnTransformer, path) -> dict:
    """preprocessor_contract.json, in the delivered schema the Java loader reads."""
    return export_preprocessor_contract(pre, list(CONTINUOUS), list(BINARY), list(CATEGORICAL), Path(path))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command.
Expected: `4 passed` on this machine, where `models/S7` is present. On a machine without it: `3 passed, 1 skipped`.

- [ ] **Step 5: Commit**

```bash
git add training/src/netsec_ml/s7comm/preprocessing.py training/tests/unit/s7comm/test_preprocessing.py
git commit -m "feat(training): port the delivered S7 preprocessing recipe

<attribution lines>"
```

---

### Task 5: Windows, the model and the training loop

**Files:**
- Create: `training/src/netsec_ml/s7comm/model.py`
- Create: `training/src/netsec_ml/s7comm/sequences.py`
- Create: `training/src/netsec_ml/s7comm/train.py`
- Test: `training/tests/unit/s7comm/test_training.py`

**Interfaces:**
- Produces:
  - `model.LSTMAutoencoder` and `model.WeightedLastTimestepMSE` (verbatim upstream);
  - `model.column_weights(names, features, weight) -> np.ndarray[float32]`;
  - `model.last_event_scores(model, X, windows, weights, batch=2048) -> np.ndarray[float64]`;
  - `sequences.connection_rows(data) -> list[np.ndarray]`;
  - `sequences.window_rows(connections, length, stride, keep) -> np.ndarray[int64, (m, length)]`;
  - `sequences.sampling_weights(sources, is_write, balance, write_fraction) -> np.ndarray`;
  - `sequences.WindowSet`;
  - `train.TrainConfig` and `train.train_model(X, train_windows, sample_weights, val_windows, loss_weights, score_weights, cfg) -> (model, history)`.

- [ ] **Step 1: Write the failing tests**

`training/tests/unit/s7comm/test_training.py`:
```python
"""Windows per connection, the delivered architecture and loss, and a deterministic training loop."""
import numpy as np
import pandas as pd
import torch

from netsec_ml.s7comm import model as M
from netsec_ml.s7comm import sequences as S
from netsec_ml.s7comm import train as T


def test_the_model_reconstructs_a_window_of_the_same_shape():
    assert M.LSTMAutoencoder(21, 32, 16)(torch.zeros(2, 16, 21)).shape == (2, 16, 21)


def test_column_weights_zero_every_column_of_a_listed_feature():
    names = ["continuous__a", "binary__b", "categorical__s7_operation_READ_VAR", "categorical__s7_operation_X"]
    assert list(M.column_weights(names, ["s7_operation"], 0.0)) == [1.0, 1.0, 0.0, 0.0]


def test_windows_follow_their_connection_and_the_stride():
    data = pd.DataFrame({"capture": ["c"] * 40, "uid": ["u"] * 40, "event_index": range(40)})
    windows = S.window_rows(S.connection_rows(data), 16, 8, np.ones(40, bool))
    assert [w[-1] for w in windows] == [15, 23, 31, 39]
    assert all((np.diff(w) == 1).all() for w in windows)


# Review Focus 1: a training window never mixes train rows with validation rows.
def test_a_training_window_never_mixes_parts():
    data = pd.DataFrame({"capture": ["c"] * 40, "uid": ["u"] * 40, "event_index": range(40)})
    keep = np.arange(40) < 30
    windows = S.window_rows(S.connection_rows(data), 16, 1, keep)
    assert len(windows) == 15 and windows.max() == 29


def test_sampling_weights_balance_sources_and_raise_writes():
    sources = np.array(["A"] * 90 + ["B"] * 10)
    w = S.sampling_weights(sources, np.zeros(100, bool), "equal", 0.1)
    assert np.isclose(w[sources == "A"].sum(), 0.5) and np.isclose(w[sources == "B"].sum(), 0.5)
    writes = np.zeros(100, bool)
    writes[:2] = True
    w = S.sampling_weights(np.array(["A"] * 100), writes, "proportional", 0.1)
    assert np.isclose(w[writes].sum(), 0.1)


def periodic(n=600):
    """A learnable stream: one connection, a smooth periodic pattern in 21 columns."""
    t = np.arange(n, dtype=np.float32)
    X = np.stack([np.sin(t / (3 + k)) for k in range(21)], axis=1).astype(np.float32)
    data = pd.DataFrame({"capture": ["c"] * n, "uid": ["u"] * n, "event_index": range(n)})
    return X, S.connection_rows(data)


def test_training_lowers_the_loss_and_is_deterministic():
    X, conns = periodic()
    train_w = S.window_rows(conns, 16, 2, np.arange(len(X)) < 450)
    val_w = S.window_rows(conns, 16, 1, np.arange(len(X)) >= 470)
    ones = np.ones(21, np.float32)
    # A small model with a high rate: 14 steps an epoch, so five epochs clearly learn.
    cfg = T.TrainConfig(hidden=16, latent=8, batch=16, lr=1e-2, max_epochs=5, patience=10, seed=7)
    weights = np.full(len(train_w), 1.0 / len(train_w))
    _, h1 = T.train_model(X, train_w, weights, val_w, ones, ones, cfg)
    _, h2 = T.train_model(X, train_w, weights, val_w, ones, ones, cfg)
    assert h1[-1]["val_score"] < h1[0]["val_score"]
    assert [e["val_score"] for e in h1] == [e["val_score"] for e in h2]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm/test_training.py -q`
Expected: FAIL — `ImportError: cannot import name 'model'`.

- [ ] **Step 3: Implement**

`training/src/netsec_ml/s7comm/model.py`: `class LSTMAutoencoder` is copied **verbatim** from `models/S7/src/s7zeek/modeling/lstm_ae.py`, and `class WeightedLastTimestepMSE` **verbatim** from `models/S7/src/s7zeek/modeling/debiased.py` (ruling A6). They go under this header, with the platform's two functions after them:
```python
"""The delivered S7 Stage 1 architecture and training loss (spec section 3), ported verbatim from
the model team's lstm_ae.py (LSTMAutoencoder) and debiased.py (WeightedLastTimestepMSE); below
them, the per-column weights and the runtime's last-event score."""
from __future__ import annotations

import numpy as np
import torch
from torch import nn

# ---- verbatim from the delivery: class LSTMAutoencoder, class WeightedLastTimestepMSE ----


def column_weights(names, features, weight) -> np.ndarray:
    """One weight per transformed column: `weight` for every column a listed raw feature produces
    (upstream transformed_columns_for_raw_features), 1 elsewhere."""
    w = np.ones(len(names), dtype=np.float32)
    for i, name in enumerate(names):
        for f in features:
            if name in (f"continuous__{f}", f"binary__{f}") or name.startswith(f"categorical__{f}_"):
                w[i] = weight
    return w


def last_event_scores(model, X, windows, weights, batch=2048) -> np.ndarray:
    """causal_shadow.py's score for each window, in float32 arithmetic as the runtime computes it:
    sum_j w_j (reconstruction_j - x_j)^2 / sum_j w_j over the window's last row."""
    w = torch.as_tensor(np.asarray(weights, dtype=np.float32)).view(1, -1)
    total = float(w.sum())
    out = np.empty(len(windows), dtype=np.float64)
    model.eval()
    with torch.no_grad():
        for start in range(0, len(windows), batch):
            x = torch.from_numpy(X[windows[start:start + batch]])
            rec = model(x)
            out[start:start + len(x)] = ((((rec[:, -1, :] - x[:, -1, :]) ** 2) * w).sum(dim=1) / total).numpy()
    return out
```

`training/src/netsec_ml/s7comm/sequences.py`:
```python
"""16-event windows per connection, and the per-window sampling weights of the delivered recipe."""
from __future__ import annotations

import numpy as np
import torch
from numpy.lib.stride_tricks import sliding_window_view
from torch.utils.data import Dataset


def connection_rows(data) -> list[np.ndarray]:
    """Each connection's row positions (0..len(data)-1), in event order."""
    frame = data[["capture", "uid", "event_index"]].assign(_pos=np.arange(len(data)))
    frame = frame.sort_values(["capture", "uid", "event_index"], kind="stable")
    return [g["_pos"].to_numpy() for _, g in frame.groupby(["capture", "uid"], sort=False)]


def window_rows(connections, length, stride, keep) -> np.ndarray:
    """[m, length] row positions: every stride-th window of consecutive events of one connection,
    kept only when all its rows satisfy `keep`, so a training window never mixes parts."""
    out = []
    for rows in connections:
        if len(rows) < length:
            continue
        windows = sliding_window_view(rows, length)[::stride]
        ok = keep[windows].all(axis=1)
        if ok.any():
            out.append(windows[ok])
    return np.concatenate(out).astype(np.int64) if out else np.empty((0, length), dtype=np.int64)


def sampling_weights(sources, is_write, balance, write_fraction) -> np.ndarray:
    """One weight per training window, summing to 1: equal total per source ('equal') or one per
    window ('proportional'); windows ending in a write request are then raised to write_fraction
    of the total when rarer (the delivered recipe's write handling)."""
    sources = np.asarray(sources)
    is_write = np.asarray(is_write, dtype=bool)
    w = np.ones(len(sources), dtype=np.float64)
    if balance == "equal":
        for s in np.unique(sources):
            mask = sources == s
            w[mask] = 1.0 / mask.sum()
    elif balance != "proportional":
        raise ValueError(f"balance must be 'equal' or 'proportional', was {balance!r}")
    w /= w.sum()
    q = w[is_write].sum()
    if 0 < q < write_fraction:
        w[is_write] *= write_fraction * (1 - q) / (q * (1 - write_fraction))
        w /= w.sum()
    return w


class WindowSet(Dataset):
    """The windows as model inputs: X rows gathered per window, float32."""

    def __init__(self, X, windows):
        self.X = X
        self.windows = windows

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, i):
        return torch.from_numpy(self.X[self.windows[i]])
```

`training/src/netsec_ml/s7comm/train.py`:
```python
"""The delivered training recipe (spec section 6): Adam, weighted sampling, the last-event loss,
early stopping on the validation score, deterministic under its seed."""
from __future__ import annotations

import copy
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, WeightedRandomSampler

from netsec_ml.s7comm.model import LSTMAutoencoder, WeightedLastTimestepMSE, last_event_scores
from netsec_ml.s7comm.sequences import WindowSet


@dataclass(frozen=True)
class TrainConfig:
    hidden: int = 32
    latent: int = 16
    batch: int = 256
    lr: float = 1e-3
    weight_decay: float = 1e-5
    clip: float = 1.0
    max_epochs: int = 40
    patience: int = 7
    seed: int = 42


def train_model(X, train_windows, sample_weights, val_windows, loss_weights, score_weights, cfg):
    """Train, keep the epoch with the lowest mean validation score, and return (model, history)."""
    if len(train_windows) == 0 or len(val_windows) == 0:
        raise ValueError("training needs training and validation windows")
    torch.manual_seed(cfg.seed)
    np.random.seed(cfg.seed)
    model = LSTMAutoencoder(X.shape[1], cfg.hidden, cfg.latent)
    loss_fn = WeightedLastTimestepMSE(loss_weights)
    optimiser = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    sampler = WeightedRandomSampler(torch.as_tensor(sample_weights, dtype=torch.float64),
                                    num_samples=len(train_windows), replacement=True,
                                    generator=torch.Generator().manual_seed(cfg.seed))
    loader = DataLoader(WindowSet(X, train_windows), batch_size=cfg.batch, sampler=sampler)
    best, best_state, stale, history = float("inf"), None, 0, []
    for epoch in range(cfg.max_epochs):
        model.train()
        total, seen = 0.0, 0
        for x in loader:
            optimiser.zero_grad()
            loss = loss_fn(model(x), x)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), cfg.clip)
            optimiser.step()
            total += float(loss) * len(x)
            seen += len(x)
        val = float(np.mean(last_event_scores(model, X, val_windows, score_weights)))
        history.append({"epoch": epoch + 1, "train_loss": total / seen, "val_score": val})
        if val < best:
            best, best_state, stale = val, copy.deepcopy(model.state_dict()), 0
        else:
            stale += 1
            if stale >= cfg.patience:
                break
    model.load_state_dict(best_state)
    model.eval()
    return model, history
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command. Expected: `6 passed`.

- [ ] **Step 5: Commit**

```bash
git add training/src/netsec_ml/s7comm/model.py training/src/netsec_ml/s7comm/sequences.py \
  training/src/netsec_ml/s7comm/train.py training/tests/unit/s7comm/test_training.py
git commit -m "feat(training): S7 windows, the delivered architecture, and the training loop

<attribution lines>"
```

---

### Task 6: Stream scoring and the conformal calibration

**Files:**
- Create: `training/src/netsec_ml/s7comm/calibrate.py`
- Test: `training/tests/unit/s7comm/test_calibrate.py`

**Interfaces:**
- Consumes: `window_rows`, `last_event_scores` (Task 5).
- Produces:
  - `GROUPS`;
  - `score_groups(is_request_direction, operation_code) -> np.ndarray[str]`;
  - `stream_scores(model, X, connections, weights, rows_mask=None) -> np.ndarray` (NaN during warm-up);
  - `Policy(alpha_by_group, fallback_alpha, calibration)`;
  - `calibrate(scores, groups, alpha, fallback_alpha, min_calibration) -> Policy`;
  - `p_values(policy, scores, groups)`;
  - `decide(policy, scores, groups) -> np.ndarray[str]`, whose values are `WARMUP`, `NORMAL` or `ANOMALY`.

- [ ] **Step 1: Write the failing tests**

`training/tests/unit/s7comm/test_calibrate.py`:
```python
"""The score groups, the online warm-up, and the group-conditional conformal decision, pinned to the
same hand-worked cases as the Java S7commConformalPolicy."""
import numpy as np
import pandas as pd

from netsec_ml.s7comm import calibrate as C
from netsec_ml.s7comm.model import LSTMAutoencoder
from netsec_ml.s7comm.sequences import connection_rows


def test_groups_follow_direction_and_operation():
    groups = C.score_groups(np.array([0, 1, 1, 1, 1]), np.array([4, 4, 5, 240, -1]))
    assert list(groups) == ["RESPONSE", "READ_REQUEST", "WRITE_REQUEST", "OTHER_REQUEST", "OTHER_REQUEST"]


def policy():
    scores = np.array([0.4, 0.1, 0.3, 0.2] + [0.05])
    groups = np.array(["RESPONSE"] * 4 + ["READ_REQUEST"])
    return C.calibrate(scores, groups, {"RESPONSE": 0.2}, 0.001, min_calibration=3)


def test_p_values_count_calibration_scores_at_least_the_score():
    p = C.p_values(policy(), np.array([0.35, 0.3, 0.5, 0.0]), np.array(["RESPONSE"] * 4))
    assert np.allclose(p, [0.4, 0.6, 0.2, 1.0])


def test_a_small_group_gets_the_smallest_attainable_alpha():
    pol = policy()
    assert pol.alpha_by_group["RESPONSE"] == 0.2
    assert pol.alpha_by_group["READ_REQUEST"] == 0.5  # 1 score < min_calibration: 1/(1+1)


# Review Focus 4: a group with no calibration scores uses every score pooled and the fallback alpha.
def test_a_group_without_scores_uses_the_pooled_fallback():
    pol = policy()
    assert "OTHER_REQUEST" not in pol.alpha_by_group
    p = C.p_values(pol, np.array([1.0]), np.array(["OTHER_REQUEST"]))
    assert np.isclose(p[0], 1 / 6)  # none of the 5 pooled scores is >= 1.0
    assert list(C.decide(pol, np.array([1.0]), np.array(["OTHER_REQUEST"]))) == ["NORMAL"]  # 1/6 > 0.001


def test_decide_marks_warmup_normal_and_anomaly():
    verdicts = C.decide(policy(), np.array([np.nan, 0.0, 0.5]), np.array(["RESPONSE"] * 3))
    assert list(verdicts) == ["WARMUP", "NORMAL", "ANOMALY"]


def test_stream_scores_start_at_each_connections_sixteenth_event():
    data = pd.DataFrame({"capture": ["c"] * 20, "uid": ["u"] * 20, "event_index": range(20)})
    X = np.zeros((20, 21), np.float32)
    scores = C.stream_scores(LSTMAutoencoder(21, 8, 4), X, connection_rows(data), np.ones(21, np.float32))
    assert np.isnan(scores[:15]).all() and np.isfinite(scores[15:]).all()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm/test_calibrate.py -q`
Expected: FAIL — `ImportError: cannot import name 'calibrate'`.

- [ ] **Step 3: Implement**

`training/src/netsec_ml/s7comm/calibrate.py`:
```python
"""Online scoring and the group-conditional conformal decision (spec section 3; upstream
operation_groups, conformal_pvalues and apply_group_conformal_policy), producing exactly what the
Java S7commConformalPolicy reads back from the release."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from netsec_ml.s7comm.model import last_event_scores
from netsec_ml.s7comm.sequences import window_rows

GROUPS = ("RESPONSE", "READ_REQUEST", "WRITE_REQUEST", "OTHER_REQUEST")


def score_groups(is_request_direction, operation_code) -> np.ndarray:
    """RESPONSE for a response; else READ_REQUEST (code 4), WRITE_REQUEST (5) or OTHER_REQUEST,
    read from the vector as the Java scorer reads it (ruling A5)."""
    request = np.asarray(is_request_direction) == 1
    code = np.asarray(operation_code)
    out = np.full(len(request), "RESPONSE", dtype=object)
    out[request] = "OTHER_REQUEST"
    out[request & (code == 4)] = "READ_REQUEST"
    out[request & (code == 5)] = "WRITE_REQUEST"
    return out


def stream_scores(model, X, connections, weights, rows_mask=None) -> np.ndarray:
    """The online rule, stride 1: a score for every row with 15 earlier rows in its connection,
    NaN during the warm-up. rows_mask (whole connections) limits which rows are scored."""
    keep = np.ones(len(X), dtype=bool) if rows_mask is None else np.asarray(rows_mask, dtype=bool)
    windows = window_rows(connections, 16, 1, keep)
    scores = np.full(len(X), np.nan)
    if len(windows):
        scores[windows[:, -1]] = last_event_scores(model, X, windows, weights)
    return scores


@dataclass
class Policy:
    alpha_by_group: dict
    fallback_alpha: float
    calibration: dict


def calibrate(scores, groups, alpha, fallback_alpha, min_calibration) -> Policy:
    """Each group's finite validation scores, sorted. A group with at least min_calibration scores
    takes its configured alpha (else the fallback); a smaller group the smallest p-value it can
    give, 1/(n+1); a group with none gets no alpha and is judged on everything pooled."""
    scores = np.asarray(scores, dtype=np.float64)
    groups = np.asarray(groups)
    calibration, alpha_by_group = {}, {}
    for g in GROUPS:
        s = np.sort(scores[(groups == g) & np.isfinite(scores)])
        calibration[g] = s
        if len(s) == 0:
            continue
        alpha_by_group[g] = float(alpha.get(g, fallback_alpha)) if len(s) >= min_calibration else 1.0 / (len(s) + 1)
    return Policy(alpha_by_group, float(fallback_alpha), calibration)


def _reference(policy, group):
    """A group's calibration scores and alpha, or everything pooled with the fallback alpha."""
    own = policy.calibration.get(group, np.empty(0))
    if len(own):
        return own, policy.alpha_by_group[group]
    pooled = np.sort(np.concatenate([policy.calibration[g] for g in GROUPS]))
    return pooled, policy.fallback_alpha


def p_values(policy, scores, groups) -> np.ndarray:
    """(#calibration >= score + 1) / (n + 1) per row; NaN for a non-finite score."""
    scores = np.asarray(scores, dtype=np.float64)
    groups = np.asarray(groups)
    out = np.full(len(scores), np.nan)
    for g in GROUPS:
        rows = (groups == g) & np.isfinite(scores)
        if rows.any():
            ref, _ = _reference(policy, g)
            at_least = len(ref) - np.searchsorted(ref, scores[rows], side="left")
            out[rows] = (at_least + 1.0) / (len(ref) + 1.0)
    return out


def decide(policy, scores, groups) -> np.ndarray:
    """WARMUP where there is no score, ANOMALY where p <= the group's alpha, NORMAL otherwise."""
    groups = np.asarray(groups)
    p = p_values(policy, scores, groups)
    out = np.full(len(p), "WARMUP", dtype=object)
    for g in GROUPS:
        rows = (groups == g) & np.isfinite(p)
        _, a = _reference(policy, g)
        out[rows] = np.where(p[rows] <= a, "ANOMALY", "NORMAL")
    return out
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command. Expected: `6 passed`.

- [ ] **Step 5: Commit**

```bash
git add training/src/netsec_ml/s7comm/calibrate.py training/tests/unit/s7comm/test_calibrate.py
git commit -m "feat(training): S7 stream scoring and the group-conditional conformal decision

<attribution lines>"
```

---
### Task 7: ONNX export, the parity gate G3, and the release directory

**Files:**
- Create: `training/src/netsec_ml/s7comm/release.py`
- Test: `training/tests/unit/s7comm/test_release.py`

**Interfaces:**
- Consumes:
  - `LSTMAutoencoder` (Task 5);
  - `write_contract` (Task 4);
  - `GROUPS` and `Policy` (Task 6).
- Produces:
  - `ARTIFACTS = "artifacts/model"` and `ONNX_NAME = "s7comm_lstm_autoencoder.onnx"`;
  - `export_onnx(model, path, sequence, width)`;
  - `onnx_parity(model, onnx_path, X, windows, batches=(1, 8, 32, 256)) -> list[dict]`;
  - `sha256(path) -> str`;
  - `write_release(root, release_id, *, model, model_config, pre, policy, score_weights, parity, documents, summary) -> Path`;
  - `verify_release(release_dir) -> dict` (the manifest).

  The release directory is `<root>/models/stage1_anomaly/<release_id>/` (ruling A4). Beside the delivered file names it holds:
  - `artifacts/model/s7comm_lstm_autoencoder.onnx`;
  - `lstm_autoencoder.pt`, `preprocessor.joblib` and `preprocessor_contract.json`;
  - `causal_online_conformal_calibration_scores.npz` and `causal_online_shadow_policy.json`;
  - `shadow_deployment_manifest.json`;
  - every `documents` entry;
  - `FROZEN_MANIFEST.json`.

- [ ] **Step 1: Write the failing tests**

`training/tests/unit/s7comm/test_release.py`:
```python
"""The ONNX graph's signature and parity, and a release every consumer can check file by file."""
import json

import numpy as np
import onnxruntime as ort
import pandas as pd
import pytest
import torch
from numpy.lib.stride_tricks import sliding_window_view

from netsec_ml.s7comm import calibrate as C
from netsec_ml.s7comm import preprocessing as P
from netsec_ml.s7comm import release as R
from netsec_ml.s7comm.data import CONTINUOUS, FEATURES
from netsec_ml.s7comm.model import LSTMAutoencoder, column_weights


def small():
    """A fitted preprocessor, an untrained small model, its windows and a policy: enough to release."""
    rng = np.random.default_rng(0)
    frame = pd.DataFrame({name: rng.uniform(0, 1, 300) for name in CONTINUOUS})
    frame["is_request_direction"] = np.arange(300) % 2
    frame["s7_function_changed"] = 0.0
    frame["s7_rosctr"] = np.where(np.arange(300) % 2 == 1, "1", "3")
    frame["s7_operation"] = "READ_VAR"
    pre = P.fit(frame[list(FEATURES)])
    X = P.transform(pre, frame)
    torch.manual_seed(0)
    model = LSTMAutoencoder(X.shape[1], 8, 4)
    windows = sliding_window_view(np.arange(len(X)), 16).astype(np.int64)
    policy = C.calibrate(np.linspace(0, 1, 50), np.array(["RESPONSE"] * 25 + ["READ_REQUEST"] * 25),
                         {"RESPONSE": 0.001, "READ_REQUEST": 0.001}, 0.001, 10)
    return pre, X, model, windows, policy


def release(tmp_path):
    """A release of small() under tmp_path, as the pipeline writes one."""
    pre, X, model, windows, policy = small()
    onnx_path = tmp_path / "model.onnx"
    R.export_onnx(model, onnx_path, 16, X.shape[1])
    names = list(pre.get_feature_names_out())
    return R.write_release(
        tmp_path / "out", "test_r1", model=model, model_config={"hidden_size": 8, "latent_size": 4},
        pre=pre, policy=policy, score_weights=column_weights(names, ["s7_operation"], 0.0),
        parity=R.onnx_parity(model, onnx_path, X, windows, batches=(1, 8, 32)),
        documents={"outputs/evaluation_summary.json": {"gates": {"G1": True}}},
        summary={"release_note": "test"})


def test_the_onnx_graph_has_the_delivered_signature(tmp_path):
    _, X, model, _, _ = small()
    R.export_onnx(model, tmp_path / "m.onnx", 16, X.shape[1])
    session = ort.InferenceSession(str(tmp_path / "m.onnx"), providers=["CPUExecutionProvider"])
    (inp,), (out,) = session.get_inputs(), session.get_outputs()
    assert (inp.name, inp.shape[1:], inp.type) == ("input", [16, X.shape[1]], "tensor(float)")
    assert isinstance(inp.shape[0], str)  # the batch axis is dynamic
    assert out.name == "reconstruction"


def test_onnx_matches_pytorch_within_the_gate(tmp_path):
    _, X, model, windows, _ = small()
    R.export_onnx(model, tmp_path / "m.onnx", 16, X.shape[1])
    parity = R.onnx_parity(model, tmp_path / "m.onnx", X, windows, batches=(1, 8, 32))
    assert [p["batch"] for p in parity] == [1, 8, 32]
    assert max(p["max_abs_difference"] for p in parity) <= 1e-4


def test_parity_needs_enough_windows(tmp_path):
    _, X, model, windows, _ = small()
    R.export_onnx(model, tmp_path / "m.onnx", 16, X.shape[1])
    with pytest.raises(ValueError, match="256"):
        R.onnx_parity(model, tmp_path / "m.onnx", X, windows[:100], batches=(1, 256))


def test_a_release_pins_every_file_and_verifies(tmp_path):
    rel = release(tmp_path)
    assert rel == tmp_path / "out" / "models" / "stage1_anomaly" / "test_r1"
    manifest = R.verify_release(rel)
    art = R.ARTIFACTS
    assert set(manifest["files"]) == {
        f"{art}/{R.ONNX_NAME}", f"{art}/lstm_autoencoder.pt", f"{art}/preprocessor.joblib",
        f"{art}/preprocessor_contract.json", f"{art}/causal_online_conformal_calibration_scores.npz",
        f"{art}/causal_online_shadow_policy.json", f"{art}/shadow_deployment_manifest.json",
        "outputs/evaluation_summary.json"}
    assert manifest["release_note"] == "test"


def test_the_calibration_and_policy_read_as_the_java_loader_expects(tmp_path):
    rel = release(tmp_path) / R.ARTIFACTS
    with np.load(rel / "causal_online_conformal_calibration_scores.npz") as npz:
        assert sorted(npz.files) == sorted(C.GROUPS)
        assert all(npz[g].dtype == np.float64 for g in C.GROUPS)
        assert len(npz["RESPONSE"]) == 25 and len(npz["OTHER_REQUEST"]) == 0
    policy = json.loads((rel / "causal_online_shadow_policy.json").read_text())
    assert policy["score_semantics"] == {"sequence_length": 16, "stride": 1, "scored_timestep": "LAST_ONLY",
                                         "future_context": False, "minimum_events_before_score": 16}
    assert policy["alpha_by_group"] == {"RESPONSE": 0.001, "READ_REQUEST": 0.001}
    assert policy["fallback_alpha"] == 0.001
    contract = json.loads((rel / "preprocessor_contract.json").read_text())
    shadow = json.loads((rel / "shadow_deployment_manifest.json").read_text())
    assert shadow["input_dimension"] == contract["transformed_dimension"]
    assert shadow["onnx_path"] == f"{R.ARTIFACTS}/{R.ONNX_NAME}"


def test_the_checkpoint_loads_into_the_delivered_architecture(tmp_path):
    ckpt = torch.load(release(tmp_path) / R.ARTIFACTS / "lstm_autoencoder.pt", weights_only=True)
    cfg = ckpt["model_config"]
    model = LSTMAutoencoder(ckpt["input_size"], cfg["hidden_size"], cfg["latent_size"])
    model.load_state_dict(ckpt["state_dict"])


def test_a_changed_file_fails_verification_naming_it(tmp_path):
    rel = release(tmp_path)
    with open(rel / R.ARTIFACTS / "causal_online_shadow_policy.json", "a", encoding="utf-8") as f:
        f.write(" ")
    with pytest.raises(ValueError, match="causal_online_shadow_policy.json"):
        R.verify_release(rel)


def test_a_release_is_never_overwritten(tmp_path):
    release(tmp_path)
    with pytest.raises(FileExistsError):
        release(tmp_path)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm/test_release.py -q`
Expected: FAIL — `ImportError: cannot import name 'release'`.

- [ ] **Step 3: Implement**

`training/src/netsec_ml/s7comm/release.py`:
```python
"""The release (spec sections 3 and 6): the ONNX graph with the delivered signature, the parity gate
G3, and a directory in the delivered layout (ruling A4) whose FROZEN_MANIFEST.json pins every file
by SHA-256. The Java loader of the scoring plan reads the five scoring files from it."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import onnxruntime as ort
import torch

from netsec_ml.s7comm.calibrate import GROUPS
from netsec_ml.s7comm.preprocessing import write_contract

ARTIFACTS = "artifacts/model"
ONNX_NAME = "s7comm_lstm_autoencoder.onnx"
SEQUENCE = 16


def export_onnx(model, path, sequence, width) -> None:
    """input [batch, sequence, width] -> reconstruction, batch dynamic: the delivered signature,
    through torch 2.5.1's TorchScript exporter (ruling A3)."""
    model.eval()
    torch.onnx.export(model, torch.zeros(1, sequence, width), str(path),
                      input_names=["input"], output_names=["reconstruction"],
                      dynamic_axes={"input": {0: "batch"}, "reconstruction": {0: "batch"}},
                      opset_version=17)


def onnx_parity(model, onnx_path, X, windows, batches=(1, 8, 32, 256)) -> list[dict]:
    """G3's evidence: max and mean |ONNX - PyTorch| over the reconstruction of the first `batch`
    windows, at each batch size."""
    if len(windows) < max(batches):
        raise ValueError(f"parity needs at least {max(batches)} windows, got {len(windows)}")
    session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    model.eval()
    out = []
    for b in batches:
        x = np.ascontiguousarray(X[windows[:b]], dtype=np.float32)
        with torch.no_grad():
            expected = model(torch.from_numpy(x)).numpy()
        diff = np.abs(session.run(None, {"input": x})[0] - expected)
        out.append({"batch": int(b), "max_abs_difference": float(diff.max()),
                    "mean_abs_difference": float(diff.mean())})
    return out


def sha256(path) -> str:
    """A file's SHA-256, 64 lowercase hex."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _json(path: Path, value) -> None:
    """Pretty JSON, UTF-8, parents created."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_release(root, release_id, *, model, model_config, pre, policy, score_weights, parity,
                  documents, summary) -> Path:
    """Write the release under <root>/models/stage1_anomaly/<release_id>/ and return it. An existing
    release is never overwritten: a new model is a new release_id."""
    rel = Path(root) / "models" / "stage1_anomaly" / release_id
    if rel.exists():
        raise FileExistsError(f"{rel} exists: a release is frozen, give the new one its own release_id")
    art = rel / ARTIFACTS
    art.mkdir(parents=True)
    names = [str(n) for n in pre.get_feature_names_out()]

    # The graph and the checkpoint, in the delivered checkpoint shape (reliability.load_model).
    export_onnx(model, art / ONNX_NAME, SEQUENCE, len(names))
    torch.save({"input_size": len(names), "state_dict": model.state_dict(),
                "model_config": {"num_layers": 1, "dropout": 0.0, **model_config}}, art / "lstm_autoencoder.pt")

    # The preprocessing, as the pickled pipeline and as the JSON contract Java reads.
    joblib.dump(pre, art / "preprocessor.joblib")
    contract = write_contract(pre, art / "preprocessor_contract.json")

    # Every group's calibration scores (float64, empty when none: the Java loader wants all four),
    # compressed like the delivered file, and the policy that reads them.
    np.savez_compressed(art / "causal_online_conformal_calibration_scores.npz",
                        **{g: np.asarray(policy.calibration.get(g, []), dtype=np.float64) for g in GROUPS})
    _json(art / "causal_online_shadow_policy.json", {
        "schema_version": "s7-causal-online-shadow-policy-v2",
        "status": "SHADOW_ONLY_NOT_PRODUCTION_VALIDATED",
        "score_semantics": {"sequence_length": SEQUENCE, "stride": 1, "scored_timestep": "LAST_ONLY",
                            "future_context": False, "minimum_events_before_score": SEQUENCE},
        "alpha_by_group": policy.alpha_by_group,
        "fallback_alpha": policy.fallback_alpha,
        "calibration_groups": [
            {"group": g, "calibration_n": int(len(policy.calibration.get(g, []))),
             "alpha": policy.alpha_by_group.get(g),
             "minimum_possible_pvalue": 1.0 / (len(policy.calibration[g]) + 1) if len(policy.calibration.get(g, [])) else None}
            for g in GROUPS],
    })
    _json(art / "shadow_deployment_manifest.json", {
        "status": "SHADOW_ONLY_NOT_PRODUCTION_VALIDATED",
        "onnx_path": f"{ARTIFACTS}/{ONNX_NAME}",
        "sequence_length": SEQUENCE,
        "input_dimension": contract["transformed_dimension"],
        "preprocessor_contract": f"{ARTIFACTS}/preprocessor_contract.json",
        "evaluation_mode": "CAUSAL_LAST_TIMESTEP",
        "conformal_calibration_scores": f"{ARTIFACTS}/causal_online_conformal_calibration_scores.npz",
        "shadow_policy": f"{ARTIFACTS}/causal_online_shadow_policy.json",
        "identity_features_excluded_from_event_score": ["s7_operation"],
        "identity_score_weight": 0.0,
        "score_weights": [float(w) for w in score_weights],
        "alpha_by_group": policy.alpha_by_group,
        "transformed_feature_order": names,
        "onnx_parity": parity,
        "onnx_max_abs_difference": max(p["max_abs_difference"] for p in parity),
    })

    # The run's own records (evaluation, selection, datasets), then the manifest over everything.
    for relative, value in documents.items():
        _json(rel / relative, value)
    files = sorted(p for p in rel.rglob("*") if p.is_file())
    _json(rel / "FROZEN_MANIFEST.json", {
        "schema_version": "s7-frozen-model-release-v2",
        "release_id": release_id,
        "release_status": "FROZEN_INTERNAL_SHADOW_MODEL",
        "production_validated": False,
        "model": {"algorithm": "LSTM_AUTOENCODER", "training_objective": "CAUSAL_LAST_TIMESTEP_RECONSTRUCTION",
                  "sequence_length": SEQUENCE, "raw_feature_count": 16,
                  "transformed_dimension": contract["transformed_dimension"]},
        **summary,
        "files": {p.relative_to(rel).as_posix(): {"sha256": sha256(p), "bytes": p.stat().st_size} for p in files},
    })
    return rel


def verify_release(release_dir) -> dict:
    """Every file FROZEN_MANIFEST.json lists exists with its recorded SHA-256; returns the manifest."""
    rel = Path(release_dir)
    manifest = json.loads((rel / "FROZEN_MANIFEST.json").read_text(encoding="utf-8"))
    for relative, pin in manifest["files"].items():
        path = rel / relative
        if not path.is_file():
            raise FileNotFoundError(f"{relative} is listed in FROZEN_MANIFEST.json but missing")
        if sha256(path) != pin["sha256"]:
            raise ValueError(f"{relative} does not match FROZEN_MANIFEST.json's SHA-256")
    return manifest
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command. Expected: `8 passed` (a `TracerWarning` from the exporter is expected and harmless).

- [ ] **Step 5: Commit**

```bash
git add training/src/netsec_ml/s7comm/release.py training/tests/unit/s7comm/test_release.py
git commit -m "feat(training): S7 ONNX export, the parity gate, and the frozen release

<attribution lines>"
```

---

### Task 8: Evaluation, candidate selection and the pipeline

**Files:**
- Create: `training/src/netsec_ml/s7comm/evaluate.py`
- Create: `training/src/netsec_ml/s7comm/pipeline.py`
- Test: `training/tests/unit/s7comm/synthetic.py` (a helper, not a test module)
- Test: `training/tests/unit/s7comm/test_evaluate.py`
- Test: `training/tests/unit/s7comm/test_pipeline.py`

**Interfaces:**
- Consumes: everything in Tasks 3–7.
- Produces:
  - `evaluate.PAST = 64`;
  - `evaluate.rate_table(data, verdicts, mask) -> DataFrame`, with columns `source, scored, past_64, normal_rate_past_64, normal_rate_16_to_64`;
  - `evaluate.group_table(groups, verdicts, mask, late) -> DataFrame`;
  - `evaluate.gate_g1(table, threshold) -> list[str]` (the failures);
  - `evaluate.render_card(summary) -> str`;
  - `pipeline.main(argv) -> int`: 0 with a release, 1 when a gate fails (no release), 2 on a usage error.

  `pipeline.main` writes `<out>/evaluation_summary.json`, `<out>/MODEL_CARD.md` and, only when G1 and G3 hold, `<out>/release/models/stage1_anomaly/<release_id>/`.

- [ ] **Step 1: Write the failing tests**

`training/tests/unit/s7comm/synthetic.py`:
```python
"""Synthetic S7commFeatureExport files for the pipeline test: steady read polling, one connection per
capture, with small noise. Shaped like the real export; not real traffic, and never used to train
anything but the test's throwaway model."""
import numpy as np

from netsec_ml.s7comm.data import EXPORT_COLUMNS


def write_polling(path, capture, client, events, seed, start=1.6e9):
    """`events` alternating READ_VAR requests and responses on one connection, 0.1 s apart."""
    rng = np.random.default_rng(seed)
    lines = [",".join(EXPORT_COLUMNS)]
    for i in range(events):
        request = i % 2 == 0
        values = [float(request), 0.5, 1.0, 1.0 + i, 1.0, 0.5, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0625,
                  float(request), 0.0, 1.0 if request else 3.0, 4.0]
        values[1] += rng.normal(0, 0.01)
        ts = start + 0.1 * i
        lines.append(",".join([capture, f"U{capture}", f"{ts:.6f}", f"s:U{capture}:{i}", client, "10.9.9.9",
                               str(int(request)), "4", str(int(i == 0)), "1" if request else "3", "READ_VAR",
                               *(repr(v) for v in values), "0"]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
```

`training/tests/unit/s7comm/test_evaluate.py`:
```python
"""G1's arithmetic: rates past each connection's 64th event, per source, and the failures it names."""
import numpy as np
import pandas as pd

from netsec_ml.s7comm import evaluate as E


def data(n=100):
    return pd.DataFrame({"source": ["a"] * n, "event_index": np.arange(n)})


def test_rates_count_only_scored_rows_and_split_at_the_64th_event():
    verdicts = np.array(["WARMUP"] * 15 + ["ANOMALY"] * 49 + ["NORMAL"] * 35 + ["ANOMALY"])
    table = E.rate_table(data(), verdicts, np.ones(100, bool))
    row = table.iloc[0]
    assert (row.source, row.scored, row.past_64) == ("a", 85, 36)
    assert np.isclose(row.normal_rate_past_64, 35 / 36)
    assert row.normal_rate_16_to_64 == 0.0


def test_g1_names_a_source_below_the_threshold():
    table = pd.DataFrame([{"source": "a", "scored": 10, "past_64": 100, "normal_rate_past_64": 0.98,
                           "normal_rate_16_to_64": 1.0}])
    failures = E.gate_g1(table, 0.99)
    assert len(failures) == 1 and failures[0].startswith("a: 98.00% NORMAL")
    assert E.gate_g1(table, 0.98) == []


def test_g1_fails_a_source_with_nothing_past_the_64th_event():
    table = pd.DataFrame([{"source": "a", "scored": 10, "past_64": 0, "normal_rate_past_64": np.nan,
                           "normal_rate_16_to_64": 1.0}])
    assert E.gate_g1(table, 0.99) == ["a: no scored event past the 64th in its gated rows"]


def test_the_card_shows_every_gated_source_and_the_gates():
    summary = {"release_id": "r1", "gates": {"G1": {"passed": True, "threshold": 0.99, "failures": []},
                                             "G3": {"passed": True, "max_abs_difference": 1e-7, "threshold": 1e-4}},
               "g1": [{"source": "qut-control", "scored": 5, "past_64": 4, "normal_rate_past_64": 1.0,
                       "normal_rate_16_to_64": 1.0}],
               "unseen": [], "report": [], "groups": [], "leave_one_source_out": {},
               "selection": {"chosen": "c1", "candidates": []}, "sources": {}, "inference_us_per_window": 1.0}
    card = E.render_card(summary)
    assert "| qut-control | 5 | 4 | 100.00% | 100.00% |" in card
    assert "G1" in card and "PASSED" in card
```

`training/tests/unit/s7comm/test_pipeline.py`:
```python
"""The whole pipeline on synthetic exports: a release when the gates hold, none when G1 fails."""
import json

import yaml

from netsec_ml.s7comm import pipeline
from netsec_ml.s7comm.release import verify_release
from tests.unit.s7comm.synthetic import write_polling


def config(tmp_path, threshold):
    features = tmp_path / "features"
    features.mkdir(exist_ok=True)
    for capture, client, events, seed in (("a", "10.0.0.1", 900, 1), ("b", "10.0.0.2", 900, 2),
                                          ("c", "10.0.0.3", 300, 3), ("d", "10.0.0.4", 200, 4)):
        write_polling(features / f"{capture}.csv", capture, client, events, seed)
    cfg = {
        "release_id": "test_r1", "features_dir": str(features),
        "sources": {"a": {"captures": ["a"], "role": "normal"}, "b": {"captures": ["b"], "role": "normal"},
                    "c": {"captures": ["c"], "role": "unseen"}, "d": {"captures": ["d"], "role": "report"}},
        "split": {"train": 0.70, "validation": 0.15, "gap_events": 64, "gap_fraction": 0.01},
        "model": {"hidden": 8, "latent": 4, "sequence": 16, "train_stride": 4},
        "training": {"batch": 64, "lr": 0.01, "weight_decay": 1.0e-5, "clip": 1.0, "max_epochs": 2,
                     "patience": 2, "seed": 1, "threads": 1},
        "candidates": [{"name": "only", "identity_weight": 0.5, "balance": "equal", "write_fraction": 0.1}],
        "calibration": {"alpha": {"RESPONSE": 0.001, "READ_REQUEST": 0.001}, "fallback_alpha": 0.001,
                        "min_calibration": 50},
        "gates": {"normal_rate_past_64": threshold, "onnx_parity": 1.0e-4},
        "parity_batches": [1, 8, 32],
    }
    path = tmp_path / f"config-{threshold}.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return path


def test_passing_gates_write_a_verified_release(tmp_path):
    out = tmp_path / "run"
    assert pipeline.main(["--config", str(config(tmp_path, 0.0)), "--out", str(out)]) == 0
    summary = json.loads((out / "evaluation_summary.json").read_text())
    assert summary["gates"]["G1"]["passed"] and summary["gates"]["G3"]["passed"]
    assert [r["source"] for r in summary["g1"]] == ["a", "b"]
    assert [r["source"] for r in summary["unseen"]] == ["c"]
    assert [r["source"] for r in summary["report"]] == ["d"]
    assert set(summary["leave_one_source_out"]["only"]) == {"a", "b"}
    manifest = verify_release(out / "release" / "models" / "stage1_anomaly" / "test_r1")
    assert "outputs/evaluation_summary.json" in manifest["files"]
    assert (out / "MODEL_CARD.md").read_text().startswith("# S7comm Stage 1 detector")


def test_a_failing_gate_writes_no_release_and_names_the_failure(tmp_path, capsys):
    out = tmp_path / "run"
    assert pipeline.main(["--config", str(config(tmp_path, 1.01)), "--out", str(out)]) == 1
    summary = json.loads((out / "evaluation_summary.json").read_text())
    assert not summary["gates"]["G1"]["passed"]
    assert not (out / "release").exists()
    assert "G1 failed" in capsys.readouterr().err


def test_an_existing_output_directory_is_refused(tmp_path):
    (tmp_path / "run").mkdir()
    assert pipeline.main(["--config", str(config(tmp_path, 0.0)), "--out", str(tmp_path / "run")]) == 2
```

- [ ] **Step 2: Run them to verify they fail**

Run: `training/.venv/bin/pytest training/tests/unit/s7comm/test_evaluate.py training/tests/unit/s7comm/test_pipeline.py -q`
Expected: FAIL — `ImportError: cannot import name 'evaluate'` (and `'pipeline'`).

- [ ] **Step 3: Implement**

`training/src/netsec_ml/s7comm/evaluate.py`:
```python
"""The false-alarm gate G1 and the reported rates (spec section 7), from one verdict per row, and
the model card rendered from the run's evaluation summary (every number comes from that file)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from netsec_ml.s7comm.calibrate import GROUPS

# G1 counts events after the connection's 64th (event_index is 0-based).
PAST = 64
RATE_COLUMNS = ["source", "scored", "past_64", "normal_rate_past_64", "normal_rate_16_to_64"]


def _share(verdicts) -> float:
    """NORMAL's share of the verdicts, NaN when there are none."""
    return float(np.mean(verdicts == "NORMAL")) if len(verdicts) else float("nan")


def rate_table(data, verdicts, mask) -> pd.DataFrame:
    """Per source, over the scored rows in `mask`: how many, how many past the 64th event, and the
    NORMAL share past the 64th event and from the 16th to the 64th."""
    frame = pd.DataFrame({"source": data["source"].to_numpy(), "late": data["event_index"].to_numpy() >= PAST,
                          "verdict": np.asarray(verdicts, dtype=object)})[np.asarray(mask, dtype=bool)]
    frame = frame[frame["verdict"] != "WARMUP"]
    rows = []
    for source, g in frame.groupby("source", sort=True):
        late = g.loc[g["late"], "verdict"].to_numpy()
        rows.append({"source": source, "scored": int(len(g)), "past_64": int(len(late)),
                     "normal_rate_past_64": _share(late),
                     "normal_rate_16_to_64": _share(g.loc[~g["late"], "verdict"].to_numpy())})
    return pd.DataFrame(rows, columns=RATE_COLUMNS)


def group_table(groups, verdicts, mask, late) -> pd.DataFrame:
    """Per score group, over the scored rows in `mask` past the 64th event: count and NORMAL share."""
    keep = np.asarray(mask, dtype=bool) & np.asarray(late, dtype=bool) & (np.asarray(verdicts) != "WARMUP")
    g, v = np.asarray(groups)[keep], np.asarray(verdicts)[keep]
    return pd.DataFrame([{"group": name, "scored": int((g == name).sum()), "normal_rate": _share(v[g == name])}
                         for name in GROUPS])


def gate_g1(table, threshold) -> list[str]:
    """G1's failures, named: a gated source below the threshold past the 64th event, or with no
    scored event there at all. Empty means G1 holds."""
    failures = []
    for r in table.itertuples():
        if r.past_64 == 0:
            failures.append(f"{r.source}: no scored event past the 64th in its gated rows")
        elif r.normal_rate_past_64 < threshold:
            failures.append(f"{r.source}: {r.normal_rate_past_64:.2%} NORMAL past the 64th event, "
                            f"below {threshold:.2%}")
    return failures


def _pct(x) -> str:
    """A rate as a percentage, or a dash when undefined."""
    return "-" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.2%}"


def _rates(rows) -> list[str]:
    """A markdown table of rate_table rows."""
    out = ["| source | scored | past 64th | NORMAL past 64th | NORMAL 16th-64th |", "|---|---|---|---|---|"]
    out += [f"| {r['source']} | {r['scored']} | {r['past_64']} | {_pct(r['normal_rate_past_64'])} | "
            f"{_pct(r['normal_rate_16_to_64'])} |" for r in rows]
    return out


def render_card(summary) -> str:
    """The model card: gates, per-source rates, selection and data, all from the summary."""
    g1, g3 = summary["gates"]["G1"], summary["gates"]["G3"]
    lines = [f"# S7comm Stage 1 detector {summary['release_id']}", "",
             "Generated from `evaluation_summary.json` by `netsec_ml.s7comm.evaluate.render_card`.", "",
             "## Gates", "",
             f"- **G1** ({g1['threshold']:.0%} NORMAL past each connection's 64th event, every normal "
             f"source's held-out rows): {'PASSED' if g1['passed'] else 'FAILED'}",
             *[f"  - {f}" for f in g1["failures"]],
             f"- **G3** (ONNX vs PyTorch within {g3['threshold']:g}): {'PASSED' if g3['passed'] else 'FAILED'}, "
             f"max difference {g3['max_abs_difference']:.3g}", "",
             "## Held-out normal traffic (gated)", "", *_rates(summary["g1"]), "",
             "## Unseen clients (reported, not gated)", "", *_rates(summary["unseen"]), "",
             "## Engineering and other sessions (reported, not gated)", "", *_rates(summary["report"]), "",
             "## Per score group, gated rows past the 64th event", "",
             "| group | scored | NORMAL |", "|---|---|---|",
             *[f"| {r['group']} | {r['scored']} | {_pct(r['normal_rate'])} |" for r in summary["groups"]], "",
             "## Selection", "", f"Chosen candidate: `{summary['selection']['chosen']}`.", "",
             "| candidate | mean leave-one-source-out NORMAL | per held-out source |", "|---|---|---|",
             *[f"| {c['name']} | {_pct(c['objective'])} | "
               + ", ".join(f"{s} {_pct(v)}" for s, v in c["leave_one_source_out"].items()) + " |"
               for c in summary["selection"]["candidates"]], "",
             "## Data", "", "| source | role | captures | rows by part |", "|---|---|---|---|",
             *[f"| {name} | {s['role']} | {', '.join(s['captures'])} | "
               + ", ".join(f"{k} {v}" for k, v in s["rows"].items()) + " |" for name, s in summary["sources"].items()],
             "", f"Inference: {summary['inference_us_per_window']:.0f} us per window (ONNX Runtime, batch 1, one thread).",
             ""]
    return "\n".join(lines)
```

`training/src/netsec_ml/s7comm/pipeline.py`:
```python
"""One training run (spec sections 6 and 7): load the exports, split by time, choose a candidate on
validation and leave-one-source-out runs over the training sources, train the final model, calibrate,
score every source once, check G1 and G3, and release only when both hold.

    python -m netsec_ml.s7comm.pipeline --config configs/s7comm-v2.yaml --out /data/run-1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import onnxruntime as ort
import torch

from netsec_ml.s7comm import calibrate as C
from netsec_ml.s7comm import data as D
from netsec_ml.s7comm import evaluate as E
from netsec_ml.s7comm import preprocessing as P
from netsec_ml.s7comm import release as R
from netsec_ml.s7comm.model import column_weights
from netsec_ml.s7comm.release import sha256
from netsec_ml.s7comm.sequences import connection_rows, sampling_weights, window_rows
from netsec_ml.s7comm.train import TrainConfig, train_model


@dataclass
class Fit:
    """One trained candidate: its preprocessing, inputs, model, policy and every row's score."""
    pre: object
    X: np.ndarray
    model: object
    history: list
    policy: C.Policy
    scores: np.ndarray
    score_weights: np.ndarray


def fit(data, part, connections, groups, sources, candidate, cfg) -> Fit:
    """Train `candidate` on the train part of `sources`, calibrate on their validation part, and
    score every row of every claimed connection (the online rule, stride 1)."""
    train = (part == "train").to_numpy() & data["source"].isin(sources).to_numpy()
    val = (part == "validation").to_numpy() & data["source"].isin(sources).to_numpy()
    pre = P.fit(data[train])
    X = P.transform(pre, data)
    names = [str(n) for n in pre.get_feature_names_out()]
    loss_w = column_weights(names, ["s7_operation"], candidate["identity_weight"])
    score_w = column_weights(names, ["s7_operation"], 0.0)

    # Training windows lie wholly inside one part (Review Focus 1); sample weights per the recipe.
    m, t = cfg["model"], cfg["training"]
    train_w = window_rows(connections, m["sequence"], m["train_stride"], train)
    val_w = window_rows(connections, m["sequence"], 1, val)
    last = train_w[:, -1]
    is_write = (data["is_request_direction"].to_numpy() == 1) & (data["operation_code"].to_numpy() == 5)
    weights = sampling_weights(data["source"].to_numpy()[last], is_write[last], candidate["balance"],
                               candidate["write_fraction"])
    tc = TrainConfig(hidden=m["hidden"], latent=m["latent"], batch=t["batch"], lr=t["lr"],
                     weight_decay=t["weight_decay"], clip=t["clip"], max_epochs=t["max_epochs"],
                     patience=t["patience"], seed=t["seed"])
    model, history = train_model(X, train_w, weights, val_w, loss_w, score_w, tc)

    # Every row's score (each connection whole, as online), then the conformal policy.
    scores = C.stream_scores(model, X, connections, score_w)
    cal = cfg["calibration"]
    policy = C.calibrate(scores[val], groups[val], cal["alpha"], cal["fallback_alpha"], cal["min_calibration"])
    return Fit(pre, X, model, history, policy, scores, score_w)


def select(data, part, connections, groups, normal, cfg):
    """Each candidate's leave-one-source-out NORMAL rate past the 64th event, on each held-out
    source's train and validation rows (never its test rows); the best mean wins, ties to the
    first in config order."""
    held_rows = part.isin(["train", "validation"]).to_numpy()
    results = []
    for candidate in cfg["candidates"]:
        loso = {}
        for held in normal:
            f = fit(data, part, connections, groups, [s for s in normal if s != held], candidate, cfg)
            verdicts = C.decide(f.policy, f.scores, groups)
            mask = held_rows & (data["source"] == held).to_numpy()
            table = E.rate_table(data, verdicts, mask)
            loso[held] = float(table["normal_rate_past_64"].iloc[0]) if len(table) else float("nan")
        results.append({"name": candidate["name"], "leave_one_source_out": loso,
                        "objective": float(np.nanmean(list(loso.values()))) if loso else float("nan")})
    best = max(range(len(results)), key=lambda i: (np.nan_to_num(results[i]["objective"], nan=-1.0), -i))
    return cfg["candidates"][best], results


def inference_cost(onnx_path, X, windows, n=1000) -> float:
    """Microseconds per window, ONNX Runtime, batch 1, one thread: the online job's setting."""
    options = ort.SessionOptions()
    options.intra_op_num_threads = options.inter_op_num_threads = 1
    session = ort.InferenceSession(str(onnx_path), options, providers=["CPUExecutionProvider"])
    batch = [np.ascontiguousarray(X[w][None], dtype=np.float32) for w in windows[:n]]
    start = time.perf_counter()
    for x in batch:
        session.run(None, {"input": x})
    return 1e6 * (time.perf_counter() - start) / max(len(batch), 1)


def run(cfg, out: Path) -> int:
    """The run itself; returns 0 with a release, 1 when a gate fails."""
    torch.set_num_threads(int(cfg["training"]["threads"]))
    sources = D.sources_of(cfg)
    data = D.load_sources(cfg, sources)
    data = data[data["source"] != ""].reset_index(drop=True)  # unclaimed connections are not used
    part = D.split_by_time(data, cfg["split"])
    connections = connection_rows(data)
    groups = C.score_groups(data["is_request_direction"].to_numpy(), data["operation_code"].to_numpy())
    normal = [s.name for s in sources if s.role == "normal"]

    # Choose on training sources only, then train the final model on all of them.
    candidate, candidates = select(data, part, connections, groups, normal, cfg)
    print(f"chosen candidate: {candidate['name']}", flush=True)
    final = fit(data, part, connections, groups, normal, candidate, cfg)
    verdicts = C.decide(final.policy, final.scores, groups)

    # G1 on held-out normal rows; everything else reported.
    role, late = data["role"].to_numpy(), data["event_index"].to_numpy() >= E.PAST
    gated = ((role == "normal") & (part == "test").to_numpy()) | (role == "normal-test")
    g1 = E.rate_table(data, verdicts, gated)
    g1_failures = E.gate_g1(g1, cfg["gates"]["normal_rate_past_64"])

    # G3: ONNX against PyTorch on validation windows.
    val_w = window_rows(connections, 16, 1, (part == "validation").to_numpy())
    onnx_path = out / "candidate.onnx"
    R.export_onnx(final.model, onnx_path, 16, final.X.shape[1])
    parity = R.onnx_parity(final.model, onnx_path, final.X, val_w, tuple(cfg.get("parity_batches", (1, 8, 32, 256))))
    max_diff = max(p["max_abs_difference"] for p in parity)
    g3_ok = max_diff <= cfg["gates"]["onnx_parity"]

    summary = {
        "release_id": cfg["release_id"],
        "gates": {"G1": {"passed": not g1_failures, "threshold": cfg["gates"]["normal_rate_past_64"],
                         "failures": g1_failures},
                  "G3": {"passed": g3_ok, "threshold": cfg["gates"]["onnx_parity"], "max_abs_difference": max_diff}},
        "g1": g1.to_dict("records"),
        "unseen": E.rate_table(data, verdicts, role == "unseen").to_dict("records"),
        "report": E.rate_table(data, verdicts, role == "report").to_dict("records"),
        "groups": E.group_table(groups, verdicts, gated, late).to_dict("records"),
        "leave_one_source_out": {c["name"]: c["leave_one_source_out"] for c in candidates},
        "selection": {"chosen": candidate["name"], "candidates": candidates},
        "training_history": final.history,
        "parity": parity,
        "inference_us_per_window": inference_cost(onnx_path, final.X, val_w),
        "sources": {s.name: {"role": s.role, "captures": list(s.captures), "clients": list(s.clients),
                             "rows": {str(k): int(v) for k, v in part[data["source"] == s.name].value_counts().items()}}
                    for s in sources},
        "exports": {c: sha256(Path(cfg["features_dir"]) / f"{c}.csv") for c in sorted(data["capture"].unique())},
    }
    (out / "evaluation_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    card = E.render_card(summary)
    (out / "MODEL_CARD.md").write_text(card, encoding="utf-8")
    print(card, flush=True)
    onnx_path.unlink()

    # Release only when both gates hold (spec section 7: a failure goes to the owner first).
    if g1_failures or not g3_ok:
        for f in g1_failures:
            print(f"G1 failed: {f}", file=sys.stderr)
        if not g3_ok:
            print(f"G3 failed: ONNX differs from PyTorch by {max_diff:.3g}", file=sys.stderr)
        return 1
    rel = R.write_release(
        out / "release", cfg["release_id"], model=final.model,
        model_config={"hidden_size": cfg["model"]["hidden"], "latent_size": cfg["model"]["latent"]},
        pre=final.pre, policy=final.policy, score_weights=final.score_weights, parity=parity,
        documents={"outputs/evaluation_summary.json": summary, "config/run_config.json": cfg},
        summary={"selection": {"selected_candidate": candidate, "attack_used_for_selection": False,
                               "test_used_for_selection": False},
                 "internal_metrics": {"g1": summary["g1"], "onnx_max_abs_difference": max_diff}})
    print(f"release written: {rel}", flush=True)
    return 0


def main(argv=None) -> int:
    """CLI entry point; 2 on a usage error or an existing output directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    out = Path(args.out)
    if out.exists():
        print(f"{out} exists: every run gets a new output directory", file=sys.stderr)
        return 2
    out.mkdir(parents=True)
    return run(D.load_config(args.config), out)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the Step 2 command. Expected: `7 passed` in under two minutes.

Then run the whole S7 suite: `training/.venv/bin/pytest training/tests/unit/s7comm -q`.
Expected: `43 passed`: Task 2's 2, Task 3's 10, Task 4's 4, Task 5's 6, Task 6's 6, Task 7's 8 and Task 8's 7. None may be skipped here, because `models/S7` is present on this machine.

- [ ] **Step 5: Commit**

```bash
git add training/src/netsec_ml/s7comm/evaluate.py training/src/netsec_ml/s7comm/pipeline.py \
  training/tests/unit/s7comm/synthetic.py training/tests/unit/s7comm/test_evaluate.py \
  training/tests/unit/s7comm/test_pipeline.py
git commit -m "feat(training): S7 evaluation, candidate selection, and the gated pipeline

<attribution lines>"
```

---

### Task 9: Acquiring the captures: `acquire.sh`, the pins, and `run-pipeline.sh`

**Files:**
- Create: `training/s7comm/sources.sha256`
- Create: `training/s7comm/acquire.sh`
- Create: `training/s7comm/run-pipeline.sh`
- Test: `training/s7comm/tests/test_acquire.sh`

**Interfaces:**
- Consumes:
  - the shaded online JAR (`modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar`) and `S7commFeatureExport` (Task 1);
  - the deployed Zeek image's offline policy `/opt/netsec/offline-json.zeek`;
  - `deploy/tests/lib.sh` (`assert_eq`, `finish`).
- Produces:
  - `acquire.sh DATA_DIR ONLINE_JAR [CAPTURE…]` → `DATA_DIR/{pcap,zeek,features,rejects}` plus `features/SHA256SUMS`;
  - `run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME` → `DATA_DIR/<RUN_NAME>/` (Task 8's output).

- [ ] **Step 1: Write the failing test**

`training/s7comm/tests/test_acquire.sh`:
```bash
#!/usr/bin/env bash
# acquire.sh with docker, curl and git stubbed on PATH: pins verified, one CSV
# per capture, cached captures not fetched again, and every failure named. The
# real fetch (git, the netresec share, Zeek, the JAR) runs on server3 (Task 10).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091  # deploy's assertion helpers, by a computed path
. "$HERE/../../../deploy/tests/lib.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Stubs: curl writes a pcap whose content is its URL; docker plays Zeek (an
# s7comm.log in /work) and the exporter (a CSV at --out, under /data); each
# records its call.
mkdir -p "$tmp/bin"
cat > "$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
echo "curl $*" >> "$STUB_LOG"
while [ $# -gt 0 ]; do case "$1" in -o) out="$2"; shift 2 ;; *) url="$1"; shift ;; esac; done
printf 'pcap:%s' "$url" > "$out"
EOF
cat > "$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
echo "docker $*" >> "$STUB_LOG"
work="" data="" out="" entry=""
while [ $# -gt 0 ]; do
  case "$1" in
    --entrypoint) entry="$2"; shift 2 ;;
    -v) case "$2" in *:/work) work="${2%:/work}" ;; *:/data) data="${2%:/data}" ;; esac; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ "$entry" = zeek ] && [ -z "${STUB_NO_S7:-}" ]; then echo '{"uid":"C1"}' > "$work/s7comm.log"; fi
if [ "$entry" = java ]; then printf 'capture,uid\nx,C1\n' > "$data/${out#/data/}"; fi
exit 0
EOF
cat > "$tmp/bin/git" <<'EOF'
#!/usr/bin/env bash
echo "git $*" >> "$STUB_LOG"
EOF
chmod +x "$tmp/bin/"*
export PATH="$tmp/bin:$PATH" STUB_LOG="$tmp/calls"
touch "$tmp/online.jar"

# Two pins: one URL, one local file (a read-only copy source).
printf 'local pcap' > "$tmp/local.pcap"
url_sha="$(printf 'pcap:https://example.invalid/a.pcap' | sha256sum | cut -c1-64)"
local_sha="$(sha256sum < "$tmp/local.pcap" | cut -c1-64)"
cat > "$tmp/pins" <<EOF
# capture  sha256  kind  location
cap-a  $url_sha  url  https://example.invalid/a.pcap
cap-b  $local_sha  local  $tmp/local.pcap
EOF
export S7_PINS="$tmp/pins"

# Every capture fetched, run through Zeek and exported; the sums written.
out="$(bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" 2>&1)"; status=$?
assert_eq 0 "$status" "a clean acquire succeeds: $out"
assert_eq "cap-a.csv cap-b.csv" "$(cd "$tmp/data/features" && printf '%s ' *.csv | sed 's/ $//')" "one CSV per capture"
assert_eq 2 "$(wc -l < "$tmp/data/features/SHA256SUMS")" "the exports' SHA-256s"
assert_eq 2 "$(grep -c -- '--entrypoint zeek' "$tmp/calls")" "Zeek ran per capture"
assert_eq 4 "$(grep -c -- '--network none' "$tmp/calls")" "Zeek and the exporter run without a network"
assert_eq 1 "$(grep -c '^curl' "$tmp/calls")" "only the URL pin is downloaded"
assert_eq "local pcap" "$(cat "$tmp/local.pcap")" "the local source is untouched"

# A second run fetches nothing again and reuses Zeek's output.
: > "$tmp/calls"
bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" >/dev/null 2>&1
assert_eq 0 "$(grep -c '^curl' "$tmp/calls")" "a verified capture is not fetched again"
assert_eq 0 "$(grep -c -- '--entrypoint zeek' "$tmp/calls")" "Zeek's output is reused"
assert_eq 2 "$(grep -c -- '--entrypoint java' "$tmp/calls")" "the exporter always runs (the code may have changed)"

# Only the named captures, when some are named.
: > "$tmp/calls"
bash "$HERE/../acquire.sh" "$tmp/data" "$tmp/online.jar" cap-b >/dev/null 2>&1
assert_eq 1 "$(grep -c -- '--entrypoint java' "$tmp/calls")" "one capture named, one exported"

# A pin that does not match stops the run, naming the capture's file.
sed -i "s/$local_sha/$(printf '0%.0s' {1..64})/" "$tmp/pins"
out="$(bash "$HERE/../acquire.sh" "$tmp/data2" "$tmp/online.jar" 2>&1)"; status=$?
assert_eq 1 "$status" "a changed capture fails"
assert_eq 1 "$(grep -c 'cap-b.pcap does not match its pinned SHA-256' <<< "$out")" "naming the file"

# Zeek writing no s7comm.log stops the run, naming the capture.
sed -i "s/$(printf '0%.0s' {1..64})/$local_sha/" "$tmp/pins"
out="$(STUB_NO_S7=1 bash "$HERE/../acquire.sh" "$tmp/data3" "$tmp/online.jar" 2>&1)"; status=$?
assert_eq 1 "$status" "no S7 records fails"
assert_eq 1 "$(grep -c 'Zeek wrote no s7comm.log for cap-a' <<< "$out")" "naming the capture"

# A missing JAR and an unknown pin kind are named before anything is fetched.
out="$(bash "$HERE/../acquire.sh" "$tmp/data4" "$tmp/nowhere.jar" 2>&1)"; status=$?
assert_eq 1 "$(grep -c 'nowhere.jar is missing' <<< "$out")" "the missing JAR is named"
printf 'cap-c  %s  ftp  somewhere\n' "$local_sha" >> "$tmp/pins"
out="$(bash "$HERE/../acquire.sh" "$tmp/data5" "$tmp/online.jar" cap-c 2>&1)"; status=$?
assert_eq 1 "$(grep -c 'unknown kind ftp for cap-c' <<< "$out")" "the unknown kind is named"

finish
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash training/s7comm/tests/test_acquire.sh`
Expected: FAIL. The first check fails, because `acquire.sh` does not exist yet.

- [ ] **Step 3: Implement**

`training/s7comm/sources.sha256`. The SHA-256 of each capture is the file Zeek reads: a zip's single member, or the Git LFS object itself (which matches the LFS pointer's `oid`). All were measured on server3 on 2026-09-28.
```
# The S7comm detector v2's captures (spec section 4), one per line:
#   capture  sha256  kind  location
# kind: url (downloaded), local (copied read-only from server3), iti (ITI/ICS-Security-Tools
# at ITI_COMMIT), qut-zip (the single member of a zip in qut-infosec/2017QUT_S7comm at QUT_COMMIT).
qut-control           05fc253dd84f5db9ad9ba910ea694ba3ee83b8c477c2dcf91d1ed04aefb1ff51  qut-zip  LabelledDataset/20161219132813_control_set/hmi.pcap.zip
qut-attack            74c8b1744ada2e3f6245976efa951b7e9a945c931e78601a8a394d01cea71b5a  qut-zip  LabelledDataset/20161215163606_s7_process_attacks/hmi.pcap.zip
4sics-151020          8c6ee02dc26b1b5298a7c9b4dc83cc779bd2a3219d5c5cbc51e3d4d325763bc2  url      https://share.netresec.com/s/xYj2qCNbsLEAd6M/download/4SICS-GeekLounge-151020.pcap
4sics-151021          7365b0ea475b76bf79b207fd8f83baa45e4449aead5da6a9214bbcffbc5fa7de  url      https://share.netresec.com/s/camL59aoxbCRyyZ/download/4SICS-GeekLounge-151021.pcap
4sics-151022          82529c23906416dc73d7f1926a0d38b82527f1f2a7ff8c6f755ce3208feb9643  url      https://share.netresec.com/s/gw6Y2QzJHqDD5pr/download/4SICS-GeekLounge-151022.pcap
server3-s7            81fb17992e2de08d59309ba2a48c6596449572c39f9622690e3c4088c50f3e70  local    /root/1405-06-15/Models/data/raw/benign/pcap/s7comm/s7.pcap
server3-s701          d1e1414443ab548d5fe1fbd05245eccb2cee8ad90798537f429bc802b3a8a726  local    /root/1405-06-15/Models/data/raw/benign/pcap/s7comm/s701.pcap
server3-s702          4ba4e3f127f27002d0126b3b6fcee57b5afad38bd910692f9bd75b08797650f1  local    /root/1405-06-15/Models/data/raw/benign/pcap/s7comm/s702.pcap
server3-S7COMM        b9ffbc6812bbeccb777a87216f1f6458da11279a767cf462b2db22470270a41a  local    /root/1405-06-15/Models/data/raw/benign/pcap/s7comm/S7COMM.pcap
libnodave-bench       bdbacb1b09c621f23be1c4c55145aec24930b6fa5541e7f4626dab6a314f1308  iti      pcaps/s7/s7comm_varservice_libnodavedemo_bench.pcap
s7comm-clean          0998a2c0ecdd83e20a0fa166fffb53648ea31f9b62b41cf48841d5419bf127d7  iti      pcaps/s7/S7Comm/s7comm_clean.pcap
cyclic-1s             a5d778c725c34313883849eda83d62e184ffdc712bed18dcca2e96d068257147  url      https://media.githubusercontent.com/media/automayt/ICS-pcap/13b7ae335529146b40535c2d7aa756886040d8ad/S7/2-S7comm-VarService-CyclicData-1s/2-S7comm-VarService-CyclicData-1s.pcap
eng-readDiagData      6ba61ac1ec340e9129007a5a7aec16ae1009f9691eda31172c56b155022e702c  iti      pcaps/s7/step7_s300_readDiagData.pcapng
eng-readVarTab        9cd63c73f2e5715585357f1f278cfb5baeeae9ecaa326bb5fcd4a038afa1ca85  iti      pcaps/s7/step7_s300_readVarTab.pcapng
eng-plc-status        e71f81b471bd67da2fd6e40dc69a7179574ba66771c6150cd7bfe232cc07b8a9  iti      pcaps/s7/s7comm_reading_plc_status.pcap
eng-blocklist         b2e7014362630b803b413dda595e4ba7a3a910105712f448c22dba517ec02851  iti      pcaps/s7/s7comm_program_blocklist_onlineview.pcap
eng-download-db1      48725bd1af7b778821351cd0f50f0ee259e438074a27f8431a8a1a3351dfd3d0  iti      pcaps/s7/s7comm_downloading_block_db1.pcap
eng-plc-time          d74c1eca1f2039dadcccd43c212f649b293acf6a80db1560e9ee22aeabb4c5d4  iti      pcaps/s7/s7comm_reading_setting_plc_time.pcap
eng-snap7-everything  2b91f6a8a203ec83e4f2dbb69d5e61602845f23da0baa31d95d029fb24cbd427  iti      pcaps/s7/snap7_s300_everything.pcapng
```

`training/s7comm/acquire.sh` (then `chmod +x`):
```bash
#!/usr/bin/env bash
# The S7comm detector v2's training data (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md
# sections 4 and 5). Every capture pinned in sources.sha256 is fetched or copied and SHA-256-verified.
# It then runs through the deployed Zeek image offline (the sensor's own packages and JSON naming), and
# then through S7commFeatureExport, the platform's own parser, mapper and feature code.
# Output: DATA_DIR/features/<capture>.csv, the pipeline's only input.
# Runs on server3; the development machine never runs Zeek over these captures.
# Usage: acquire.sh DATA_DIR ONLINE_JAR [CAPTURE ...]   (no CAPTURE: every pinned one)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PINS="${S7_PINS:-$HERE/sources.sha256}"
ZEEK_IMAGE="${ZEEK_IMAGE:-netsec-ml/zeek:1}"
FLINK_IMAGE="${FLINK_IMAGE:-flink:2.2.1-java21}"
QUT_REPO=https://github.com/qut-infosec/2017QUT_S7comm.git
QUT_COMMIT=afac2b6f776bbe0f9f5bc58d0898e8516bc72561
ITI_REPO=https://github.com/ITI/ICS-Security-Tools.git
ITI_COMMIT=9b826091e7ba3fbdd5997d31e116f29e09cbbb48

die() { printf 'acquire: %s\n' "$*" >&2; exit 1; }
log() { printf 'acquire: %s\n' "$*"; }

data="${1:?usage: acquire.sh DATA_DIR ONLINE_JAR [CAPTURE ...]}"
jar="${2:?usage: acquire.sh DATA_DIR ONLINE_JAR [CAPTURE ...]}"
shift 2
[ -f "$jar" ] || die "the online job JAR $jar is missing: build it first (./mvnw package -pl modules/bootstrap-online-job -am -DskipTests)"
mkdir -p "$data"/pcap "$data"/repos "$data"/zeek "$data"/features "$data"/rejects
data="$(cd "$data" && pwd)"
jar_dir="$(cd "$(dirname "$jar")" && pwd)"
jar_name="$(basename "$jar")"

# pinned FILE SHA256: does FILE exist with exactly this SHA-256?
pinned() { [ -f "$1" ] && printf '%s  %s\n' "$2" "$1" | sha256sum -c --quiet - >/dev/null 2>&1; }

# clone_at DIR URL COMMIT: a shallow checkout of exactly COMMIT, reused when already there.
clone_at() {
  if [ "$(git -C "$1" rev-parse HEAD 2>/dev/null || true)" != "$3" ]; then
    rm -rf "$1"
    git init -q "$1"
    git -C "$1" fetch -q --depth 1 "$2" "$3"
    git -C "$1" checkout -q FETCH_HEAD
  fi
}

# fetch CAPTURE SHA KIND LOCATION: DATA/pcap/CAPTURE.pcap, verified (kept when already verified).
fetch() {
  local capture="$1" sha="$2" kind="$3" location="$4" out="$data/pcap/$1.pcap"
  pinned "$out" "$sha" && return 0
  case "$kind" in
    url) curl -fsSL --retry 3 -o "$out.part" "$location" && mv "$out.part" "$out" ;;
    local) cp "$location" "$out" ;;  # another project's file: read, never modified
    iti) clone_at "$data/repos/iti" "$ITI_REPO" "$ITI_COMMIT"; cp "$data/repos/iti/$location" "$out" ;;
    qut-zip)
      clone_at "$data/repos/qut" "$QUT_REPO" "$QUT_COMMIT"
      # The zip's single member (server3 has no unzip; Python's zipfile is enough).
      python3 - "$data/repos/qut/$location" "$out" <<'PY'
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1])
(member,) = z.namelist()
with open(sys.argv[2], "wb") as f:
    f.write(z.read(member))
PY
      ;;
    *) die "unknown kind $kind for $capture" ;;
  esac
  pinned "$out" "$sha" || die "$out does not match its pinned SHA-256 $sha"
}

# run_zeek CAPTURE: the deployed image's offline JSON policy over the capture, no network.
run_zeek() {
  local out="$data/zeek/$1"
  [ -s "$out/s7comm.log" ] && return 0
  rm -rf "$out"
  mkdir -p "$out"
  docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint zeek \
    -v "$data/pcap:/pcap:ro" -v "$out:/work" -w /work \
    "$ZEEK_IMAGE" -C -r "/pcap/$1.pcap" /opt/netsec/offline-json.zeek
  [ -s "$out/s7comm.log" ] || die "Zeek wrote no s7comm.log for $1"
}

# run_export CAPTURE: S7commFeatureExport on the Flink image's Java 21, no network.
run_export() {
  docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint java \
    -v "$jar_dir:/jars:ro" -v "$data:/data" "$FLINK_IMAGE" \
    -cp "/jars/$jar_name" io.netsecml.platform.bootstrap.online.S7commFeatureExport \
    --out "/data/features/$1.csv" --rejects "/data/rejects/$1.jsonl" "$1=/data/zeek/$1/s7comm.log"
}

# The pins, filtered to the named captures when some are named.
selected=0
while read -r capture sha kind location; do
  case "$capture" in ''|'#'*) continue ;; esac
  if [ $# -gt 0 ] && ! printf '%s\n' "$@" | grep -qxF "$capture"; then continue; fi
  selected=$((selected + 1))
  log "$capture: fetch"
  fetch "$capture" "$sha" "$kind" "$location"
  log "$capture: zeek"
  run_zeek "$capture"
  log "$capture: export"
  run_export "$capture"
done < "$PINS"
[ "$selected" -gt 0 ] || die "no pinned capture matches: $*"

# The exports' own SHA-256s, which the run's evaluation summary records again.
(cd "$data/features" && sha256sum -- *.csv > SHA256SUMS)
log "done: $selected captures in $data/features"
```

`training/s7comm/run-pipeline.sh` (then `chmod +x`):
```bash
#!/usr/bin/env bash
# One run end to end (spec section 6): acquire.sh (captures -> Zeek -> the platform's features),
# then the training pipeline in the throwaway CPU-PyTorch image, limited to 16 CPUs and 24 GB so
# the server's other work keeps running. Runs on server3.
# Usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TRAINING="$(cd "$HERE/.." && pwd)"
IMAGE="${S7_TRAIN_IMAGE:-netsec-ml/s7-train:1}"
data="${1:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"
jar="${2:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"
config="${3:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"
run="${4:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"

# The features first; then the image, built once from training/docker/s7comm.Dockerfile.
bash "$HERE/acquire.sh" "$data" "$jar"
docker image inspect "$IMAGE" >/dev/null 2>&1 \
  || docker build -t "$IMAGE" -f "$TRAINING/docker/s7comm.Dockerfile" "$TRAINING/docker"

# The pipeline: config path relative to training/, data at /data, no network.
data="$(cd "$data" && pwd)"
docker run --rm --network none --cpus 16 --memory 24g --user "$(id -u):$(id -g)" \
  -v "$TRAINING:/work:ro" -v "$data:/data" -w /work "$IMAGE" \
  python -m netsec_ml.s7comm.pipeline --config "$config" --out "/data/$run"
```

- [ ] **Step 4: Run the test and shellcheck to verify they pass**

Run:
```bash
chmod +x training/s7comm/acquire.sh training/s7comm/run-pipeline.sh
bash training/s7comm/tests/test_acquire.sh
docker run --rm -v "$PWD:/mnt:ro" -w /mnt koalaman/shellcheck:v0.10.0 \
  training/s7comm/acquire.sh training/s7comm/run-pipeline.sh training/s7comm/tests/test_acquire.sh
```
Expected: `test_acquire.sh: 17 checks, 0 failed`, and shellcheck prints nothing. The shellcheck container is the same one `deploy/tests/run-all.sh` already runs on this machine; no stack is started.

- [ ] **Step 5: Commit**

```bash
git add training/s7comm/sources.sha256 training/s7comm/acquire.sh training/s7comm/run-pipeline.sh \
  training/s7comm/tests/test_acquire.sh
git commit -m "feat(training): pinned S7 capture acquisition and the one-command run

<attribution lines>"
```

---

### Task 10: The run on server3

No code is written in this task. It produces the data, the run and the release, and it ledgers the results. **STOP** at Step 6 if a gate fails: the failures go to the owner (spec section 7), and candidates are never tuned on test data.

**Files:** none in the repository. On server3:
- `/root/s7work` (a `git archive` of this branch);
- `/root/s7data/v2/` (captures, Zeek output, features, runs).

- [ ] **Step 1: Put the branch on server3**

```bash
ssh server3 'test ! -e /root/s7work || { echo "/root/s7work exists: look at it before replacing it"; exit 1; }'
git archive --format=tar HEAD | ssh server3 'mkdir -p /root/s7work && tar -x -C /root/s7work'
```
Expected: no output. If `/root/s7work` exists, look at what is there first. It is this project's own scratch checkout, so move it aside (`mv /root/s7work /root/s7work.old-$(date +%s)`) and re-run.

- [ ] **Step 2: Build the online JAR on server3**

In the pinned Maven image, with the deployed checkout's Maven cache copied (read, never modified):
```bash
ssh server3 'cp -a /root/mvp-project/backup-project/deploy/.m2 /root/s7work/.m2 \
  && docker run --rm -v /root/s7work:/src -w /src -v /root/s7work/.m2:/var/maven/.m2 -e MAVEN_CONFIG=/var/maven/.m2 \
       maven:3.9.9-eclipse-temurin-21 mvn -B -q -Duser.home=/var/maven package -pl modules/bootstrap-online-job -am -DskipTests \
  && ls /root/s7work/modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar'
```
Expected: one `bootstrap-online-job-…-all.jar` path.

- [ ] **Step 3: Acquire**

Background it: the 4SICS downloads are about 375 MB, and 4SICS runs through Zeek for minutes.
```bash
ssh server3 'cd /root/s7work && nohup bash training/s7comm/acquire.sh /root/s7data/v2 \
  "$(ls modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar)" > /root/s7data/v2-acquire.log 2>&1 &'
```
Watch `/root/s7data/v2-acquire.log` until it prints `acquire: done: 19 captures`.

Then:
```bash
ssh server3 'cd /root/s7data/v2 && wc -l features/*.csv && wc -l rejects/*.jsonl'
```
Expected: 19 CSVs, each with more than one line. `qut-control.csv` should hold roughly 238,000 rows, the delivered model's training data. Rejects should be few or none; any rejects file with more than 1% of its capture's rows gets its reasons read and ledgered before going on.

- [ ] **Step 4: Build the training image and run the unit tests in it**

```bash
ssh server3 'cd /root/s7work/training && docker build -q -t netsec-ml/s7-train:1 -f docker/s7comm.Dockerfile docker \
  && docker run --rm --network none --cpus 16 --memory 24g -v /root/s7work/training:/work -w /work \
       netsec-ml/s7-train:1 python -m pytest -q -p no:cacheprovider tests/unit/s7comm'
```
Expected: `42 passed, 1 skipped`. The skip is `test_the_port_matches_the_delivered_code`, because `models/S7` is git-ignored and so not in a `git archive`. `pip install -e` is not needed, because the image sets `PYTHONPATH=/work/src`.

- [ ] **Step 5: Run the pipeline**

Background it, because selection trains 3 candidates × 4 held-out sources plus the final model:
```bash
ssh server3 'cd /root/s7work && nohup bash training/s7comm/run-pipeline.sh /root/s7data/v2 \
  "$(ls modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar)" configs/s7comm-v2.yaml run-1 \
  > /root/s7data/v2-run-1.log 2>&1 &'
```
Watch `/root/s7data/v2-run-1.log`. It ends with the model card and then either `release written: …` (exit 0) or `G1 failed: …` / `G3 failed: …` lines (exit 1).

- [ ] **Step 6: Read the result, and STOP on a failed gate**

```bash
ssh server3 'cat /root/s7data/v2/run-1/MODEL_CARD.md'
```
- **G1 and G3 passed:** ledger the G1 table, the unseen and report rates, and the chosen candidate. Continue.
- **A gate failed:** ledger the failures and the whole card, and **stop**. Report to the owner: which source, its rate, and the per-group table. Do not change the config and re-run against the same test data; the owner decides what happens next.

- [ ] **Step 7: Bring the release home and verify it**

```bash
mkdir -p models/S7v2
scp -r server3:/root/s7data/v2/run-1/release/models models/S7v2/
scp server3:/root/s7data/v2/run-1/evaluation_summary.json server3:/root/s7data/v2/run-1/MODEL_CARD.md models/S7v2/
training/.venv/bin/python -c "from netsec_ml.s7comm.release import verify_release; \
m = verify_release('models/S7v2/models/stage1_anomaly/v2_multisource_r1'); print(len(m['files']), 'files verified')"
```
Expected: `9 files verified`: the seven artifacts, `outputs/evaluation_summary.json` and `config/run_config.json`. The manifest does not list itself. `models/` is git-ignored: the release is data, pinned by its manifest, and never committed.

---

### Task 11: The model card, the docs, and the scoring plan's revision

**Files:**
- Create: `docs/models/s7comm-stage1-detector-v2.md`
- Modify: `training/README.md`
- Modify: `CLAUDE.md` (Implementation state, Verification state, S7comm limits)
- Modify: `docs/superpowers/plans/2026-09-28-s7comm-stage1-scoring.md` (a revision note at the top)
- Memory: `project_s7_scoring_precheck.md` (outside the repository)

- [ ] **Step 1: The model card**

Write `docs/models/s7comm-stage1-detector-v2.md` in two parts.
1. Paste `models/S7v2/MODEL_CARD.md` verbatim as the body. It is generated, and every number in it comes from `evaluation_summary.json`.
2. Put these hand-written sections above it. Each one is taken from the spec or the run:
   - **What it is.** The delivered contract (spec section 3), retrained by this project on four real sources, because the delivered model scored independent benign S7 traffic 0% NORMAL (the pre-check of 2026-09-28).
   - **Release.** The release id, `models/S7v2/models/stage1_anomaly/v2_multisource_r1/`, and the SHA-256 of each of the five scoring files. Copy them from `FROZEN_MANIFEST.json`.
   - **Reproduce.** The Task 10 commands: `acquire.sh`, then `run-pipeline.sh`, with the source pins in `training/s7comm/sources.sha256`.
   - **Not yet measured.** Attack detection (G2) and the comparison with published models are Plan B. Until then, v2 is known to be quiet on normal traffic and nothing more.
   - **Limits.** Spec section 10 verbatim: four real training sources; QUT is the only S7-level attack evidence; ICSNPP logs only the first two PDUs of the WinCC captures.

- [ ] **Step 2: `training/README.md`**

Append:
````markdown
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
````

- [ ] **Step 3: `CLAUDE.md`**

Make three changes.
1. **Implementation state.** After the Modbus scoring unit's paragraph and deliverables, add a paragraph on the S7comm detector v2 unit. It is on `feat/s7comm-scoring`, after the S7 scoring spec and plan (`8fc9d54`, `662549c`) and the v2 spec (`020d0cf`..`aa489b3`). Its deliverables:
   - `S7commFeatureExport`;
   - `training/src/netsec_ml/s7comm/`, with the delivered recipe ported under ruling A6;
   - `acquire.sh` / `run-pipeline.sh` and the pins;
   - the gates G1 and G3, and the release `v2_multisource_r1` with its location.
   - It also states that S7comm scoring is still not implemented: its plan resumes against v2 (Step 4).
2. **Verification state.** Add a dated table of what this unit ran:
   - `bootstrap-online-job` `S7commFeatureExportTest`, with its `Tests run:` count;
   - the Python suite's `passed` line, from this machine and from the server3 image;
   - `test_acquire.sh`'s tally;
   - the Task 10 run: G1's per-source rates, G3's max difference, and the unseen and report rates.
   Name the one skipped test on server3 and why.
3. **S7comm limits and decisions.** Add one item: v2 replaces the delivered model for scoring. Its false-alarm evidence is G1 on four real sources plus the unseen clients. Its attack evidence is not measured yet (Plan B). Cite the model card.

- [ ] **Step 4: The scoring plan's revision note**

Insert directly under the title of `docs/superpowers/plans/2026-09-28-s7comm-stage1-scoring.md`:
```markdown
> **Revision (2026-09-28, detector v2).** Task 1's pre-check stopped this plan: the delivered
> model scores independent benign S7 traffic 0% NORMAL. It resumes against our own detector v2
> (`docs/models/s7comm-stage1-detector-v2.md`, release `v2_multisource_r1`). Its Task 1 re-runs on
> v2 first and must pass now. The bundle is packaged as `models/s7comm-stage1-detector/v2/` and
> committed as this plan's test fixture (spec section 9). Four changes are made when execution
> resumes, and ledgered there:
> 1. **Drop Task 2 (S2) and S1's upper-casing.** v2 was trained on `S7commFeatureExport`'s rows,
>    i.e. on exactly what the online job computes. Those fixes made Java's input look like the
>    delivered training table, and v2 has no such gap.
> 2. **Packaging takes the release directory and file names as arguments** (Plan A ruling A4):
>    `artifacts/model/` and `s7comm_lstm_autoencoder.onnx`, not `artifacts/v4_causal_final_model/`
>    and `…_debiased.onnx`.
> 3. **The loader reads the one-hot width from the bundle** (Plan A ruling A2); v2's may differ
>    from 21.
> 4. **The fixture bundle and every number pinned against the delivered model are regenerated
>    from v2:** the oracle, the thresholds, and the live-check expectations.
```

- [ ] **Step 5: Memory**

Update `project_s7_scoring_precheck.md` in the memory directory:
- the owner's decision changed on 2026-09-28: we train v2 ourselves;
- Plan A is done, with G1 and G3 results and the release id;
- Plan B (attacks and the comparison) is next;
- the scoring plan resumes against v2 per its revision note.

Update its `MEMORY.md` line to match.

- [ ] **Step 6: Commit**

```bash
git add docs/models/s7comm-stage1-detector-v2.md training/README.md CLAUDE.md \
  docs/superpowers/plans/2026-09-28-s7comm-stage1-scoring.md
git commit -m "docs(s7comm): detector v2 model card, and the scoring plan's revision for v2

<attribution lines>"
```
