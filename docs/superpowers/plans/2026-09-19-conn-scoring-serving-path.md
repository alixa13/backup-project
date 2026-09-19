# Conn scoring — serving path (Unit A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Score every `conn` feature vector inside the online job with a pinned ONNX model, publish the prediction to `netsec.prediction.v1`, and archive it to the existing ClickHouse `predictions` table.

**Architecture:** A narrow `ModelScorer` port in `ports`; the decision rule (threshold, identity, timing) in `application`; ONNX and the bundle directory behind two adapters that never import each other; a Flink `flatMap` that emits zero predictions when scoring fails so feature production is never blocked; bootstrap composes them. Proved end to end with a hand-built logistic-regression ONNX file whose expected outputs are known analytically.

**Tech Stack:** Java 21, Flink 2.2.1, ONNX Runtime Java, Jackson 2.17, JUnit 5, Testcontainers (Kafka + ClickHouse), Python 3 with the `onnx` package for fixture generation only.

**Spec:** `docs/superpowers/specs/2026-09-19-conn-scoring-mvp-design.md`

## Global Constraints

- Java package root `io.netsecml.platform`. Java 21: `record` for immutable carriers; records with array components take defensive copies in the compact constructor **and** the accessor.
- **`conn-feature-v1` and its 20 values are frozen** — not the contract file, not its content hash, not `ConnFeatureSchemaV1.java`.
- `contracts/` and `infrastructure/clickhouse/ddl/` are **immutable**. The `predictions` table is used exactly as written. New contracts are new files.
- `domain → ports → application → adapters → bootstrap`. **Adapters never import each other** (the one recorded exception is `adapter-flink → adapter-kafka`; this plan adds no new adapter-to-adapter dependency — that is what `ModelScorerFactory` exists for).
- Every Flink operator carries an explicit, stable `.uid()`. Conn's five existing uids stay byte-identical.
- CPU-only: ONNX Runtime with `intraOpNumThreads = 1` and `interOpNumThreads = 1`. The model loads once in `open()`; no hot reload.
- A model that cannot be loaded, or whose bundle disagrees with the registered schema, fails the job at startup. A per-record scoring failure emits **no** prediction, increments a counter, and never touches the feature-vector branch.
- Comment every block with the reasoning, not the syntax. A comment that misdescribes its code is worse than no comment. Never cite a brief, a ruling id, a task number, or anything under `.superpowers/` — that directory is deleted before the branch lands.
- Build offline: `./mvnw install -DskipTests -q -o`, then `./mvnw test -pl <module> -o`. **Never** combine `-Dtest=X` with `-am`. Always confirm the `Tests run:` count.
- `ClickHouseOutageTest` is OOM-killed on this machine and must never be run or described as passing. Never run `adapter-clickhouse` or either bootstrap module unfiltered.
- Never `git add -A` or `git add .` — the repo root holds ~139 MB of untracked model binaries.

**Baseline test counts before Task 1:** domain 96, application 38, adapter-kafka 71, adapter-flink 30, `InvalidEventRowMapperTest` 9, `OnlineFeatureJobTopologyTest` 5, `ArchiveJobTopologyTest` 8.

---

## File Structure

| File | Responsibility |
|---|---|
| `contracts/stream/prediction-v1.json` | frozen wire shape of a prediction |
| `contracts/model/model-bundle-v1.json` | frozen shape of a model bundle directory |
| `domain/.../model/ModelRef.java` | model identity, threshold, output layout |
| `domain/.../inference/Prediction.java` | the prediction record + deterministic id |
| `ports/.../port/out/ModelScorer.java` | score(float[]) → probability |
| `ports/.../port/out/ModelScorerFactory.java` | serializable factory so operators need no adapter |
| `application/.../usecase/ScoreFeaturesUseCase.java` | schema binding check, timing, threshold, identity |
| `adapter-registry-filesystem/.../registry/*` | read + verify a bundle directory |
| `adapter-onnx/.../runtime/OnnxModelScorer.java` | ONNX Runtime implementation of the port |
| `adapter-kafka/.../sink/Prediction{Serializer,Deserializer}.java` | wire form |
| `adapter-clickhouse/.../row/PredictionRow.java`, `.../mapper/PredictionRowMapper.java` | table row |
| `adapter-flink/.../process/ScoreFeatureVectorFunction.java` | the operator |
| `bootstrap-online-job/.../OnnxScorerFactory.java` | composition of the two adapters |
| `bootstrap-archive-job/.../PredictionRowMapFunction.java` | archive chain stage |
| `tools/fixtures/make_demo_model.py` | generates the test model, committed with its output |
| `tests/fixtures/models/conn-demo-v1/{model.onnx,bundle.json}` | the fixture bundle |

---

### Task 1: ONNX Runtime available offline, and a fixture model that exists

The build runs offline, and `com.microsoft.onnxruntime` is **not** in `~/.m2`. Nothing else in this plan can compile until it is. There is also no ONNX file in the repo to test against, and no Python ML stack installed.

**Files:**
- Modify: `pom.xml` (version property + `dependencyManagement`)
- Modify: `modules/adapter-onnx/pom.xml` (the dependency)
- Create: `tools/fixtures/make_demo_model.py`
- Create: `tests/fixtures/models/conn-demo-v1/model.onnx`, `tests/fixtures/models/conn-demo-v1/bundle.json`
- Test: `modules/adapter-onnx/src/test/java/io/netsecml/platform/adapter/onnx/runtime/FixtureModelLoadsTest.java`

**Interfaces:**
- Produces: a committed bundle directory whose `bundle.json` matches `contracts/model/model-bundle-v1.json` (Task 4 writes that contract; the fields are fixed here and Task 4 must match them).

- [ ] **Step 1: Fetch the dependency once, online**

```bash
# Network is required for this one step only. Pick the newest 1.x that resolves.
./mvnw -q dependency:get -Dartifact=com.microsoft.onnxruntime:onnxruntime:1.20.0
ls ~/.m2/repository/com/microsoft/onnxruntime/onnxruntime/
```

If that exact version does not resolve, try `1.19.2`, then `1.18.0`, and record which one worked. If none resolves, STOP and report BLOCKED with the error — do not vendor a jar by hand.

- [ ] **Step 2: Declare it**

In `pom.xml`, add to `<properties>` beside `<flink.version>`:

```xml
    <onnxruntime.version>1.20.0</onnxruntime.version>
```

and to `<dependencyManagement><dependencies>`:

```xml
      <dependency>
        <groupId>com.microsoft.onnxruntime</groupId>
        <artifactId>onnxruntime</artifactId>
        <version>${onnxruntime.version}</version>
      </dependency>
```

In `modules/adapter-onnx/pom.xml`, inside `<dependencies>`:

```xml
    <dependency>
      <groupId>com.microsoft.onnxruntime</groupId>
      <artifactId>onnxruntime</artifactId>
    </dependency>
```

- [ ] **Step 3: Prove the offline build still works**

Run: `./mvnw install -DskipTests -q -o`
Expected: exit 0. If it fails resolving onnxruntime, the fetch in Step 1 did not land — fix that before continuing.

- [ ] **Step 4: Create the fixture generator**

A hand-built logistic regression: `sigmoid(x·W + b)`. Nothing is trained, so every expected score is computable exactly, which is what makes it a golden fixture rather than a guess.

Create `tools/fixtures/make_demo_model.py`:

```python
"""Generates the demo scoring bundle used by the Java tests.

Hand-built rather than trained: the model is sigmoid(x.W + b) with fixed
weights, so the expected score for any vector is computable in closed form.
That is what lets the Java golden-vector test detect a runtime that computes
something subtly different, which a trained model could not.
"""
import hashlib
import json
import pathlib
import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

OUT = pathlib.Path("tests/fixtures/models/conn-demo-v1")
FEATURES = 20

# Deterministic, spread across positive and negative so no vector saturates.
W = np.array([[0.1 * (i - 10) / 10] for i in range(FEATURES)], dtype=np.float32)
B = np.array([-0.5], dtype=np.float32)

graph = helper.make_graph(
    [
        helper.make_node("MatMul", ["features", "W"], ["logit_raw"]),
        helper.make_node("Add", ["logit_raw", "B"], ["logit"]),
        helper.make_node("Sigmoid", ["logit"], ["probability"]),
    ],
    "conn_demo_logreg",
    [helper.make_tensor_value_info("features", TensorProto.FLOAT, [1, FEATURES])],
    [helper.make_tensor_value_info("probability", TensorProto.FLOAT, [1, 1])],
    [numpy_helper.from_array(W, name="W"), numpy_helper.from_array(B, name="B")],
)
model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
onnx.checker.check_model(model)
payload = model.SerializeToString()

OUT.mkdir(parents=True, exist_ok=True)
(OUT / "model.onnx").write_bytes(payload)

def score(x):
    return float(1.0 / (1.0 + np.exp(-(np.array(x, dtype=np.float32) @ W + B)[0])))

samples = [
    [0.0] * FEATURES,
    [1.0] * FEATURES,
    [float(i) for i in range(FEATURES)],
]

# The schema hash must equal what ConnFeatureSchemaV1 carries. Read that class's
# test first and compute it the same way over the committed contract file.
schema_hash = hashlib.sha256(
    pathlib.Path("contracts/features/conn-feature-schema-v1.json").read_bytes()
).hexdigest()
feature_order = [f["name"] for f in json.loads(
    pathlib.Path("contracts/features/conn-feature-schema-v1.json").read_text())["features"]]

bundle = {
    "name": "conn-demo",
    "version": "v1",
    "schemaId": "conn-feature-v1",
    "schemaHash": schema_hash,
    "featureOrder": feature_order,
    "classes": ["normal", "attack"],
    "threshold": 0.5,
    "modelSha": hashlib.sha256(payload).hexdigest(),
    "outputName": "probability",
    "positiveClassColumn": 0,
    "metrics": {},
    "trainedAt": "2026-09-19T00:00:00Z",
    "provenance": "hand-built fixture, not trained on any data",
    "sampleVectors": [{"values": s, "expectedScore": score(s)} for s in samples],
}
(OUT / "bundle.json").write_text(json.dumps(bundle, indent=2) + "\n")
print("wrote", OUT, "modelSha", bundle["modelSha"])
```

- [ ] **Step 5: Run it**

```bash
python3 -m venv /tmp/onnxgen && /tmp/onnxgen/bin/pip -q install onnx numpy
/tmp/onnxgen/bin/python tools/fixtures/make_demo_model.py
```

If `onnx` has no wheel for the default interpreter, retry with `python3.13 -m venv` or `python3.12 -m venv`. If no interpreter can install `onnx`, STOP and report BLOCKED — do not hand-assemble protobuf bytes.

Verify the recorded hash is the file's real hash:

```bash
sha256sum tests/fixtures/models/conn-demo-v1/model.onnx
python3 -c "import json;print(json.load(open('tests/fixtures/models/conn-demo-v1/bundle.json'))['modelSha'])"
```

Both must print the same digest. Also confirm `schemaHash` equals `ConnFeatureSchemaV1.CONTENT_HASH` — read `ConnFeatureSchemaV1Test` to see exactly how that hash is computed and match it. If the computation differs, fix the generator, not the constant.

- [ ] **Step 6: Write the failing test**

`modules/adapter-onnx/src/test/java/io/netsecml/platform/adapter/onnx/runtime/FixtureModelLoadsTest.java`:

```java
package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Proves two things at once: ONNX Runtime resolves and runs offline, and the
// committed fixture is a valid graph with the shape the scorer will assume.
// Everything later in this unit depends on both.
class FixtureModelLoadsTest {

    private static final Path BUNDLE = Path.of("..", "..", "tests", "fixtures", "models", "conn-demo-v1");

    @Test
    void theFixtureModelExposesTwentyInputsAndOneNamedOutput() throws Exception {
        byte[] model = Files.readAllBytes(BUNDLE.resolve("model.onnx"));
        try (OrtEnvironment env = OrtEnvironment.getEnvironment();
             OrtSession session = env.createSession(model, new OrtSession.SessionOptions())) {
            assertEquals(Set.of("features"), session.getInputNames(),
                "the scorer feeds a tensor named 'features'");
            assertTrue(session.getOutputNames().contains("probability"),
                "bundle.json names 'probability' as the output; the graph must actually have it");
        }
    }
}
```

- [ ] **Step 7: Run it**

Run: `./mvnw test -pl modules/adapter-onnx -o -Dtest=FixtureModelLoadsTest`
Expected: PASS, `Tests run: 1`. A failure here means the dependency or the fixture is wrong — fix before
continuing.

The exact return types of `OrtSession`'s accessors vary between ONNX Runtime versions (some return `long`
where you expect `int`, some `Set<String>` where you expect `List<String>`). This plan was written without
the jar on disk, so if the assertion above does not compile, adjust it to the API you actually have and say
so in your report — do not weaken what it checks.

- [ ] **Step 8: Commit**

```bash
git add pom.xml modules/adapter-onnx/pom.xml tools/fixtures/make_demo_model.py tests/fixtures/models/conn-demo-v1 modules/adapter-onnx/src/test
git commit -m "build: add ONNX Runtime and a hand-built fixture model"
```

---

### Task 2: `ModelRef` and `Prediction` in domain

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/ModelRef.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/Prediction.java` — the
  skeleton already ships this package, described as "prediction and inference result types (score,
  decision, threshold)", and `PILOT_ARCHITECTURE.md` names it for prediction types. `ModelRef` stays in
  `domain.model`, whose own description is model identity and bundle metadata.
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/package-info.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/model/ModelRefTest.java`, `.../PredictionTest.java`

**Interfaces:**
- Produces: `ModelRef(String name, String version, String schemaId, String schemaHash, String modelSha, float threshold, List<String> classes, String outputName, int positiveClassColumn)` implements `Serializable`; `Prediction(String predictionId, String eventId, Instant eventTime, String modelName, String modelVersion, String modelSha, String schemaId, String schemaHash, float score, boolean decision, float threshold, long inferenceMicros, int qualityFlags, Instant producedAt)` with `static String Prediction.deriveId(String eventId, String modelName, String modelVersion)`.

- [ ] **Step 1: Write the failing tests**

`PredictionTest.java`:

```java
@Test
void theIdIsDeterministicAndSixtyFourHexCharacters() {
    String first = Prediction.deriveId("sensor-eu-1:Cabc123XYZ", "conn-demo", "v1");
    String second = Prediction.deriveId("sensor-eu-1:Cabc123XYZ", "conn-demo", "v1");
    assertEquals(first, second, "the same event and model must always produce the same id");
    assertTrue(first.matches("[0-9a-f]{64}"),
        "predictions.prediction_id is FixedString(64) lowercase hex");
}

@Test
void adifferentModelVersionGivesADifferentId() {
    // Rescoring the same event with a new model must not overwrite the old row
    // under ReplacingMergeTree -- the version is part of the identity.
    assertNotEquals(Prediction.deriveId("e", "m", "v1"), Prediction.deriveId("e", "m", "v2"));
}
```

`ModelRefTest.java`:

```java
@Test
void rejectsAHashThatIsNotSixtyFourHexCharacters() {
    assertThrows(IllegalArgumentException.class, () -> new ModelRef(
        "conn-demo", "v1", "conn-feature-v1", "not-a-hash", "a".repeat(64),
        0.5f, List.of("normal", "attack"), "probability", 0));
}

@Test
void rejectsAThresholdOutsideZeroToOne() {
    assertThrows(IllegalArgumentException.class, () -> new ModelRef(
        "conn-demo", "v1", "conn-feature-v1", "a".repeat(64), "b".repeat(64),
        1.5f, List.of("normal", "attack"), "probability", 0));
}

@Test
void classesAreCopiedSoACallerCannotMutateTheRef() {
    List<String> mutable = new ArrayList<>(List.of("normal", "attack"));
    ModelRef ref = new ModelRef("conn-demo", "v1", "conn-feature-v1", "a".repeat(64),
        "b".repeat(64), 0.5f, mutable, "probability", 0);
    mutable.clear();
    assertEquals(2, ref.classes().size(), "the ref must not share the caller's list");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw test -pl modules/domain -o -Dtest=PredictionTest,ModelRefTest` (Surefire takes a
comma-separated list; `+` is not a selector)
Expected: compilation failure — the classes do not exist.

- [ ] **Step 3: Implement**

`ModelRef.java` — compact constructor validating: `name`, `version`, `schemaId`, `outputName` non-blank; `schemaHash` and `modelSha` each matching `[0-9a-f]{64}`; `threshold` within `0.0f..1.0f`; `classes` non-empty and copied with `List.copyOf`; `positiveClassColumn` non-negative — it is a column of the model's output tensor, NOT an index into `classes`, so it is not bounded by that list. Implements `Serializable` because it travels in Flink job configuration.

`Prediction.java`:

```java
public static String deriveId(String eventId, String modelName, String modelVersion) {
    // Deterministic so a replay rewrites the same row rather than adding one:
    // predictions is a ReplacingMergeTree keyed by (model, version, time, event).
    try {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest((eventId + "|" + modelName + "|" + modelVersion)
            .getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(64);
        for (byte b : hash) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("SHA-256 is required by every JVM", e);
    }
}
```

The compact constructor rejects a blank `eventId` and a `predictionId` that is not 64 hex characters.

- [ ] **Step 4: Run to verify they pass**

Run: `./mvnw test -pl modules/domain -o`
Expected: at least `Tests run: 101` (96 + the 5 shown here), 0 failures. Add a test for every validation
rule you implement — the count above is a floor, not a target, and a rule with no failing test behind it is
not a rule. Report the number you actually reach.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src
git commit -m "feat(domain): add ModelRef and Prediction"
```

---

### Task 3: The `ModelScorer` port and `ScoreFeaturesUseCase`

**Files:**
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/out/ModelScorer.java`
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/out/ModelScorerFactory.java`
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/ScoreFeaturesUseCase.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/ScoreFeaturesUseCaseTest.java`

**Interfaces:**
- Consumes: `ModelRef`, `Prediction` (Task 2), `FeatureVector`.
- Produces: `interface ModelScorer extends AutoCloseable { double score(float[] featureValues); ModelRef ref(); @Override void close(); }`; `interface ModelScorerFactory extends Serializable { ModelScorer create(); }`; `ScoreFeaturesUseCase(ModelScorer scorer, Clock clock)` with `Prediction score(FeatureVector vector)`.

- [ ] **Step 1: Write the failing test**

Use a stub scorer — the decision rule must be testable with no ONNX on the classpath, which is the whole reason the port is this narrow.

```java
private static ModelScorer stub(double probability, ModelRef ref) {
    return new ModelScorer() {
        @Override public double score(float[] values) { return probability; }
        @Override public ModelRef ref() { return ref; }
        @Override public void close() { }
    };
}

@Test
void aScoreAtOrAboveTheThresholdDecidesAttack() {
    ModelRef ref = ref(0.5f);
    Prediction p = new ScoreFeaturesUseCase(stub(0.5, ref), FIXED_CLOCK).score(vector(QualityFlags.NONE));
    assertTrue(p.decision(), "score == threshold must decide attack, not fall through");
    assertEquals(0.5f, p.score(), 1e-6f);
    assertEquals(0.5f, p.threshold(), 1e-6f, "the threshold travels on the prediction, from the bundle");
}

@Test
void aVectorFromADifferentSchemaIsRejected() {
    // A model scoring a schema it was not trained on is a deployment error.
    ModelRef ref = ref(0.5f);
    FeatureVector other = vectorWithSchema("dns-feature-v1", DnsFeatureSchemaV1.CONTENT_HASH);
    assertThrows(IllegalStateException.class,
        () -> new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(other));
}

@Test
void theVectorsQualityFlagsRideOntoThePrediction() {
    // A prediction made on a vector with absent enrichment must stay visibly
    // degraded downstream; dropping the flags would launder that away.
    ModelRef ref = ref(0.5f);
    Prediction p = new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK)
        .score(vector(QualityFlags.CONN_ENRICHMENT_ABSENT));
    assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT, p.qualityFlags());
}

@Test
void theIdentityFieldsComeFromTheVectorAndTheModelRef() {
    ModelRef ref = ref(0.5f);
    Prediction p = new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(vector(QualityFlags.NONE));
    assertEquals(Prediction.deriveId(p.eventId(), "conn-demo", "v1"), p.predictionId());
    assertEquals("conn-feature-v1", p.schemaId());
    assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, p.schemaHash());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/application -o -Dtest=ScoreFeaturesUseCaseTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

```java
public Prediction score(FeatureVector vector) {
    ModelRef ref = scorer.ref();
    // The bundle names the schema it was trained against. A mismatch means the
    // deployment paired a model with the wrong feature order, which would score
    // confidently and wrongly -- so it throws rather than degrading.
    if (!ref.schemaId().equals(vector.schemaId()) || !ref.schemaHash().equals(vector.schemaHash())) {
        throw new IllegalStateException("model " + ref.name() + ":" + ref.version()
            + " expects schema " + ref.schemaId() + "/" + ref.schemaHash()
            + " but the vector carries " + vector.schemaId() + "/" + vector.schemaHash());
    }
    long startNanos = System.nanoTime();
    float score = (float) scorer.score(vector.values());
    long inferenceMicros = (System.nanoTime() - startNanos) / 1_000L;
    return new Prediction(
        Prediction.deriveId(vector.eventId(), ref.name(), ref.version()),
        vector.eventId(), vector.eventTime(), ref.name(), ref.version(), ref.modelSha(),
        vector.schemaId(), vector.schemaHash(), score, score >= ref.threshold(), ref.threshold(),
        inferenceMicros, vector.qualityFlags(), clock.instant());
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/application -o`
Expected: `Tests run: 42` (38 + 4), 0 failures.

- [ ] **Step 5: Commit**

```bash
git add modules/ports/src modules/application/src
git commit -m "feat: add the ModelScorer port and the scoring use case"
```

---

### Task 4: The two contracts and the filesystem registry

**Files:**
- Create: `contracts/model/model-bundle-v1.json`, `contracts/stream/prediction-v1.json`
- Modify: `contracts/model/README.md`, `contracts/stream/README.md`
- Modify: `modules/adapter-registry-filesystem/pom.xml` (add `jackson-databind`, matching how `adapter-kafka` declares it)
- Create: `modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/{LoadedModel,SampleVector,FilesystemModelRegistry}.java`
- Test: `modules/adapter-registry-filesystem/src/test/java/io/netsecml/platform/adapter/registry/FilesystemModelRegistryTest.java`

**Interfaces:**
- Produces: `record LoadedModel(ModelRef ref, byte[] onnx, List<SampleVector> samples)`; `record SampleVector(float[] values, double expectedScore)`; `FilesystemModelRegistry.load(Path bundleDir) → LoadedModel`.

`model-bundle-v1.json` describes exactly the fields Task 1's generator writes: `name`, `version`, `schemaId`, `schemaHash`, `featureOrder`, `classes`, `threshold`, `modelSha`, `outputName`, `positiveClassColumn`, `metrics`, `trainedAt`, `provenance`, `sampleVectors`. Document `positiveClassColumn` explicitly: it is the column of the named output tensor that carries P(positive class) — 0 for a single-column graph like the fixture's, and 1 for a scikit-learn probability output exported with `zipmap=False`. It does not index `classes`. `prediction-v1.json` mirrors `feature-vector-v1.json`'s style with topic `netsec.prediction.v1` and the fields of `Prediction`.

- [ ] **Step 1: Write the failing tests**

```java
private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models", "conn-demo-v1");

@Test
void loadsTheFixtureBundleAndItsModelBytes() throws Exception {
    LoadedModel loaded = FilesystemModelRegistry.load(FIXTURE);
    assertEquals("conn-demo", loaded.ref().name());
    assertEquals("conn-feature-v1", loaded.ref().schemaId());
    assertEquals(0.5f, loaded.ref().threshold(), 1e-6f);
    assertTrue(loaded.onnx().length > 0);
    assertEquals(3, loaded.samples().size(), "the golden vectors must survive loading");
}

@Test
void aModelFileThatDoesNotMatchItsRecordedShaIsRejected(@TempDir Path tmp) throws Exception {
    // A truncated or swapped model must never reach the runtime: it would score
    // silently and wrongly rather than failing.
    Files.copy(FIXTURE.resolve("bundle.json"), tmp.resolve("bundle.json"));
    Files.write(tmp.resolve("model.onnx"), "not the model".getBytes(StandardCharsets.UTF_8));
    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> FilesystemModelRegistry.load(tmp));
    assertTrue(thrown.getMessage().contains("modelSha"), () -> "unhelpful message: " + thrown.getMessage());
}

@Test
void aMissingBundleFileIsRejected(@TempDir Path tmp) {
    assertThrows(IOException.class, () -> FilesystemModelRegistry.load(tmp));
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw test -pl modules/adapter-registry-filesystem -o -Dtest=FilesystemModelRegistryTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`FilesystemModelRegistry.load` reads `bundle.json` with Jackson into the `ModelRef` fields plus `sampleVectors`, reads `model.onnx`, computes its SHA-256 and compares with `modelSha` (message must name `modelSha`), and returns `LoadedModel`. `SampleVector` and `LoadedModel` take defensive copies of their arrays in the compact constructor and in the accessor.

- [ ] **Step 4: Run to verify they pass**

Run: `./mvnw test -pl modules/adapter-registry-filesystem -o`
Expected: `Tests run: 3`, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add contracts modules/adapter-registry-filesystem
git commit -m "feat(registry): read and verify a model bundle directory"
```

---

### Task 5: `OnnxModelScorer` and the golden-vector guard

**Files:**
- Create: `modules/adapter-onnx/src/main/java/io/netsecml/platform/adapter/onnx/runtime/OnnxModelScorer.java`
- Test: `modules/adapter-onnx/src/test/java/io/netsecml/platform/adapter/onnx/runtime/OnnxModelScorerTest.java`
- Modify: `modules/adapter-onnx/pom.xml` — add a test-scoped dependency on `adapter-registry-filesystem` **only if** the test loads the bundle through it; otherwise parse the fixture's JSON in the test. Prefer the second: adapters must not depend on each other even in test scope.

**Interfaces:**
- Consumes: `ModelScorer`, `ModelRef`.
- Produces: `OnnxModelScorer(byte[] onnxModel, ModelRef ref, int expectedFeatureCount) implements ModelScorer`.

The test needs two helpers; define them in the test class rather than reaching for the registry adapter
(adapters must not import each other, test scope included):

```java
private static final Path BUNDLE = Path.of("..", "..", "tests", "fixtures", "models", "conn-demo-v1");

private static JsonNode bundle() throws IOException {
    return new ObjectMapper().readTree(BUNDLE.resolve("bundle.json").toFile());
}

private static ModelRef refFrom(JsonNode b) {
    List<String> classes = new ArrayList<>();
    b.get("classes").forEach(c -> classes.add(c.asText()));
    return new ModelRef(b.get("name").asText(), b.get("version").asText(), b.get("schemaId").asText(),
        b.get("schemaHash").asText(), b.get("modelSha").asText(), (float) b.get("threshold").asDouble(),
        classes, b.get("outputName").asText(), b.get("positiveClassColumn").asInt());
}
```

- [ ] **Step 1: Write the failing test — the golden vectors**

```java
@Test
void reproducesEveryGoldenScoreFromTheBundle() throws Exception {
    // The fixture is sigmoid(x.W + b) with fixed weights, and bundle.json records
    // what that evaluates to. If ONNX Runtime computed something subtly different
    // -- a changed opset, a different accumulation order -- these disagree. No
    // trained model could give us that guarantee.
    JsonNode bundle = new ObjectMapper().readTree(BUNDLE.resolve("bundle.json").toFile());
    byte[] model = Files.readAllBytes(BUNDLE.resolve("model.onnx"));
    try (OnnxModelScorer scorer = new OnnxModelScorer(model, refFrom(bundle), 20)) {
        for (JsonNode sample : bundle.get("sampleVectors")) {
            float[] values = new float[20];
            for (int i = 0; i < 20; i++) {
                values[i] = (float) sample.get("values").get(i).asDouble();
            }
            assertEquals(sample.get("expectedScore").asDouble(), scorer.score(values), 1e-5,
                () -> "golden vector disagreed: " + sample.get("values"));
        }
    }
}

@Test
void aGraphWhoseInputWidthDiffersFromTheSchemaIsRejectedAtConstruction() throws Exception {
    byte[] model = Files.readAllBytes(BUNDLE.resolve("model.onnx"));
    // 24 is dns-feature-v1's width: pairing this model with that schema must fail
    // loudly at startup rather than throwing on the first record in production.
    assertThrows(IllegalStateException.class, () -> new OnnxModelScorer(model, refFrom(bundle()), 24));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-onnx -o -Dtest=OnnxModelScorerTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

The constructor builds `OrtEnvironment.getEnvironment()` and a session with:

```java
OrtSession.SessionOptions options = new OrtSession.SessionOptions();
// CPU-only deployment: one thread per subtask, because Flink already runs one
// subtask per core and a runtime that spawns its own pool oversubscribes them.
options.setIntraOpNumThreads(1);
options.setInterOpNumThreads(1);
```

then reads the single input's shape, and throws `IllegalStateException` if its last dimension differs from `expectedFeatureCount`. `score(float[])` creates a `[1, n]` tensor, runs the session, reads the output named `ref.outputName()`, and returns the value at `ref.positiveClassColumn()`. `close()` closes the session (the shared environment is not closed).

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-onnx -o`
Expected: `Tests run: 3` (1 from Task 1 + 2), 0 failures.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-onnx
git commit -m "feat(onnx): score a feature vector through a pinned ONNX model"
```

---

### Task 6: Prediction wire form

**Files:**
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/PredictionSerializer.java`, `.../PredictionDeserializer.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/PredictionSerializerTest.java`

**Interfaces:**
- Produces: `PredictionSerializer.serialize(String topic, Prediction) → byte[]`; `PredictionDeserializer.deserialize(String topic, byte[]) → Prediction`. Both mirror `FeatureVectorSerializer`/`FeatureVectorDeserializer` exactly, including their `Serializable` declaration.

- [ ] **Step 1: Write the failing test**

```java
@Test
void everyFieldSurvivesARoundTrip() {
    Prediction original = prediction();
    Prediction back = new PredictionDeserializer().deserialize(TOPIC,
        new PredictionSerializer().serialize(TOPIC, original));
    assertEquals(original, back);
}

@Test
void theWireFormCarriesExactlyTheContractsFieldNames() throws Exception {
    // contracts/stream/prediction-v1.json is frozen; a renamed field would break
    // the archive reader silently, since JSON ignores what it does not recognise.
    JsonNode wire = new ObjectMapper().readTree(new PredictionSerializer().serialize(TOPIC, prediction()));
    JsonNode contract = new ObjectMapper().readTree(
        Path.of("..", "..", "contracts", "stream", "prediction-v1.json").toFile());
    Set<String> expected = new LinkedHashSet<>();
    contract.get("fields").forEach(f -> expected.add(f.get("name").asText()));
    Set<String> actual = new LinkedHashSet<>();
    wire.fieldNames().forEachRemaining(actual::add);
    assertEquals(expected, actual);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-kafka -o -Dtest=PredictionSerializerTest`
Expected: compilation failure.

- [ ] **Step 3: Implement** — mirroring `FeatureVectorSerializer`'s shape: an `ObjectNode` built field by field in contract order, `decision` written as a boolean, timestamps as ISO-8601 strings.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-kafka -o`
Expected: `Tests run: 73` (71 + 2), 0 failures.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-kafka
git commit -m "feat(adapter-kafka): serialize predictions on the frozen wire contract"
```

---

### Task 7: The ClickHouse row

**Files:**
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/PredictionRow.java`, `.../mapper/PredictionRowMapper.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/PredictionRowMapperTest.java`

**Interfaces:**
- Produces: `PredictionRow` with `@JsonProperty` names matching the `predictions` table column for column; `PredictionRowMapper.toRow(Prediction) → PredictionRow`, `Serializable`.

- [ ] **Step 1: Write the failing test**

```java
@Test
void serializesToExactlyTheClickHouseColumnNames() throws Exception {
    // The DDL is immutable and the inserter rejects unknown columns, so a
    // mismatch here fails every insert at runtime. Read the column list from the
    // committed DDL rather than restating it.
    // created_at is DEFAULTed by the server, so the row must NOT send it; every
    // other column must be present and spelled exactly as the DDL spells it.
    Set<String> expected = new LinkedHashSet<>(List.of(
        "prediction_id", "event_id", "event_time", "model_name", "model_version", "model_sha",
        "schema_hash", "score", "decision", "threshold", "inference_us", "quality_flags", "row_version"));
    JsonNode row = new ObjectMapper().valueToTree(new PredictionRowMapper().toRow(prediction()));
    Set<String> actual = new LinkedHashSet<>();
    row.fieldNames().forEachRemaining(actual::add);
    assertEquals(expected, actual);
}

@Test
void thatColumnListStillMatchesTheCommittedDdl() throws Exception {
    // The list above is written out for readability; this test is what stops it
    // drifting from the immutable DDL if the table is ever versioned.
    String ddl = Files.readString(
        Path.of("..", "..", "infrastructure", "clickhouse", "ddl", "001_mvp_tables.sql"));
    String block = ddl.substring(ddl.indexOf("CREATE TABLE IF NOT EXISTS predictions"));
    block = block.substring(0, block.indexOf(") ENGINE"));
    Set<String> inDdl = new LinkedHashSet<>();
    Matcher m = Pattern.compile("^\\s{2}(\\w+)\\s", Pattern.MULTILINE).matcher(block);
    while (m.find()) {
        inDdl.add(m.group(1));
    }
    inDdl.remove("created_at");
    JsonNode row = new ObjectMapper().valueToTree(new PredictionRowMapper().toRow(prediction()));
    Set<String> actual = new LinkedHashSet<>();
    row.fieldNames().forEachRemaining(actual::add);
    assertEquals(inDdl, actual);
}

@Test
void decisionBecomesZeroOrOne() {
    // decision is UInt8 in ClickHouse; a boolean would be rejected by the inserter.
    assertEquals(1, new PredictionRowMapper().toRow(prediction(true)).decision());
    assertEquals(0, new PredictionRowMapper().toRow(prediction(false)).decision());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-clickhouse -o -Dtest=PredictionRowMapperTest`
Expected: compilation failure. (Never run this module unfiltered — it holds container tests.)

- [ ] **Step 3: Implement** — `PredictionRow` mirrors `FeatureVectorRow`'s style; `PredictionRowMapper` formats both timestamps with `ClickHouseTimestamps.format` and maps `producedAt → row_version`.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-clickhouse -o -Dtest=PredictionRowMapperTest`
Expected: `Tests run: 2`, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-clickhouse
git commit -m "feat(adapter-clickhouse): map a prediction onto the predictions table"
```

---

### Task 8: The scoring operator

**Files:**
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ScoreFeatureVectorFunction.java`
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ScoreFeatureVectorFunctionTest.java`

**Interfaces:**
- Consumes: `ModelScorerFactory`, `ScoreFeaturesUseCase`.
- Produces: `ScoreFeatureVectorFunction(ModelScorerFactory factory) extends RichFlatMapFunction<FeatureVector, Prediction>`.

**Why a factory rather than the adapters:** `adapter-flink` must not import `adapter-onnx` or `adapter-registry-filesystem`. The operator depends only on the port; bootstrap supplies the implementation.

- [ ] **Step 1: Write the failing test**

```java
@Test
void aVectorBecomesExactlyOnePrediction() throws Exception {
    OneInputStreamOperatorTestHarness<FeatureVector, Prediction> harness =
        new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(
            new ScoreFeatureVectorFunction(new StubFactory(0.9))));
    harness.open();
    harness.processElement(new StreamRecord<>(vector()));
    List<Prediction> out = harness.extractOutputValues();
    assertEquals(1, out.size());
    assertTrue(out.get(0).decision());
    harness.close();
}

@Test
void aScoringFailureEmitsNothingAndDoesNotFailTheJob() throws Exception {
    // Feature production is the platform's primary obligation. A model that
    // throws on one record must not take the pipeline down or invent a score.
    OneInputStreamOperatorTestHarness<FeatureVector, Prediction> harness =
        new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(
            new ScoreFeatureVectorFunction(new ThrowingFactory())));
    harness.open();
    harness.processElement(new StreamRecord<>(vector()));
    assertEquals(0, harness.extractOutputValues().size(), "a failed score must emit no prediction");
    harness.close();
}

@Test
void theFactoryIsSerializable() throws Exception {
    // Flink ships the function to the TaskManager; a non-serializable factory
    // fails at submission with an error that points nowhere near this class.
    new ObjectOutputStream(OutputStream.nullOutputStream())
        .writeObject(new ScoreFeatureVectorFunction(new StubFactory(0.5)));
}
```

`StubFactory` and `ThrowingFactory` are static nested classes in the test implementing `ModelScorerFactory` — not lambdas, so serialization behaviour is explicit.

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-flink -o -Dtest=ScoreFeatureVectorFunctionTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

```java
@Override
public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    scorer = factory.create();
    useCase = new ScoreFeaturesUseCase(scorer, Clock.systemUTC());
    // Counted rather than logged per record: a model failing on every record
    // would otherwise be invisible until someone noticed an empty topic.
    scoringFailures = getRuntimeContext().getMetricGroup().counter("scoringFailures");
}

@Override
public void flatMap(FeatureVector vector, Collector<Prediction> out) {
    try {
        out.collect(useCase.score(vector));
    } catch (RuntimeException e) {
        scoringFailures.inc();
    }
}
```

`close()` closes the scorer when it is non-null.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-flink -o`
Expected: `Tests run: 33` (30 + 3), 0 failures.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-flink
git commit -m "feat(adapter-flink): score feature vectors without blocking their branch"
```

---

### Task 9: Wiring both jobs

**Files:**
- Create: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnnxScorerFactory.java`
- Modify: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`
- Create: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/PredictionRowMapFunction.java`
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Modify: `.env.example`
- Test: `OnlineFeatureJobTopologyTest`, `ArchiveJobTopologyTest`

**Interfaces:**
- Produces: `OnlineFeatureJob.ScoringConfig(String bundleDir, String predictionTopic)` with `static ScoringConfig disabled()` and `boolean enabled()`; `ArchiveJob.predictionChain(String topic) → LogTypeChain<PredictionRow>`.

**Scoring is optional by configuration.** With no bundle directory the topology is exactly today's, so `OnlineFeatureJobE2ETest` and `ClickHouseOutageTest` keep passing untouched. New uids: `conn-scoring`, `prediction-sink` (online); `prediction-source`, `prediction-row`, `predictions-clickhouse-sink` (archive). Conn's five existing online uids and six archive uids stay byte-identical.

- [ ] **Step 1: Write the failing topology tests**

`OnlineFeatureJobTopologyTest` already has a `buildJob()` helper that calls the two-protocol overload and
returns the `StreamGraph`; extend it to take a `ScoringConfig` rather than writing a second builder, and
reuse the class's existing uid-collecting helper.

```java
@Test
void withoutABundleTheTopologyIsUnchanged() {
    // The conn-only callers pass no model; adding a mandatory scoring stage
    // would break them, so absence of configuration must mean absence of stage.
    Set<String> uids = uidsOf(buildTwoProtocol(ScoringConfig.disabled()));
    assertFalse(uids.contains("conn-scoring"));
    assertFalse(uids.contains("prediction-sink"));
}

@Test
void withABundleTheScoringStageAndItsSinkAppear() {
    Set<String> uids = uidsOf(buildTwoProtocol(
        new ScoringConfig("/models/conn-demo-v1", "netsec.prediction.v1")));
    assertTrue(uids.containsAll(Set.of("conn-scoring", "prediction-sink")));
    assertTrue(uids.containsAll(CONN_UIDS), "conn's five historical uids must be untouched");
}
```

and for the archive job:

```java
@Test
void fiveChainsProduceFifteenDistinctUidsIncludingThePredictionChain() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    List<LogTypeChain<?>> chains = new ArrayList<>(ArchiveJob.connAndDnsChains(
        "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
        "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1"));
    chains.add(ArchiveJob.predictionChain("netsec.prediction.v1"));
    ArchiveJob.build(env, "localhost:9092", List.copyOf(chains), config());
    Set<String> uids = uidsOf(env);
    assertEquals(15, uids.size(), "a uid collision would silently merge two chains' state");
    assertTrue(uids.containsAll(Set.of("prediction-source", "prediction-row", "predictions-clickhouse-sink")));
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw test -pl modules/bootstrap-online-job -o -Dtest=OnlineFeatureJobTopologyTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`OnnxScorerFactory implements ModelScorerFactory` holds `bundleDir` and `featureCount` as `String`/`int` fields — a named class, not a lambda, because this project has already been bitten by lambdas that erase what Flink needs at submission:

```java
@Override
public ModelScorer create() {
    LoadedModel loaded = FilesystemModelRegistry.load(Path.of(bundleDir));
    return new OnnxModelScorer(loaded.onnx(), loaded.ref(), featureCount);
}
```

In `connChain`, when `scoring.enabled()`:

```java
DataStream<Prediction> predictions = connFeatureVectors
    .flatMap(new ScoreFeatureVectorFunction(
        new OnnxScorerFactory(scoring.bundleDir(), FeatureSchemaRegistry.byLogType(LogType.CONN).featureCount())))
    .name("conn-scoring")
    .uid("conn-scoring");
sinkPredictions(predictions, bootstrapServers, scoring.predictionTopic(), "prediction-sink");
```

`sinkPredictions` mirrors `sinkFeatureVectors`, with an **anonymous** `SerializationSchema` — never a lambda.

`ArchiveJob.predictionChain(String topic)` returns `new LogTypeChain<>(topic, new PredictionRowMapFunction(topic), "predictions", "prediction-source", "prediction-row", "predictions-clickhouse-sink")`; `main()` appends it to `connAndDnsChains(...)`.

`.env.example` gains `MODEL_BUNDLE_DIR=` (empty means scoring off) and `PREDICTION_TOPIC=netsec.prediction.v1`.

- [ ] **Step 4: Run to verify they pass**

```
./mvnw test -pl modules/bootstrap-online-job -o -Dtest=OnlineFeatureJobTopologyTest   # 7
./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobTopologyTest        # 9
./mvnw test-compile -pl modules/bootstrap-online-job,modules/bootstrap-archive-job -o
```

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-online-job modules/bootstrap-archive-job .env.example
git commit -m "feat: wire optional scoring into both jobs"
```

---

### Task 10: End-to-end proof and documentation

**Files:**
- Modify: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java`
- Modify: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobE2ETest.java`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the online end-to-end case**

A third method: publish the conn fixture, build the two-protocol topology with `new ScoringConfig(<fixture bundle path>, "netsec.prediction.v1")`, and consume the prediction topic. Assert, through `PredictionDeserializer`: the prediction's `eventId` equals the conn vector's; `modelName` is `conn-demo` and `modelVersion` `v1`; `schemaHash` equals `ConnFeatureSchemaV1.CONTENT_HASH`; `threshold` is 0.5; and `decision` equals `score >= threshold` — computed from the returned score, not hardcoded, because the fixture's weights decide it and a hardcoded expectation would hide a scoring change.

Keep the existing two methods byte-identical in their assertions. Prove it: `git diff -w <base> -- <file> | grep -E '^-.*assert'` must print nothing.

- [ ] **Step 2: Add the archive end-to-end case**

Extend the four-chain method's topic set with a fifth chain, publish one serialized `Prediction`, and assert the row lands in `predictions` with `decision`, `threshold`, `model_name`, `model_version` and a 64-character `prediction_id`. Use its own database name, as the four-chain method already does.

- [ ] **Step 3: Run both, staged**

```
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/bootstrap-online-job -o -Dtest=OnlineFeatureJobE2ETest    # 3, 0 skipped
./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobE2ETest         # 3, 0 skipped
```

A SKIPPED test is not a passing one. Never run `ClickHouseOutageTest`.

- [ ] **Step 4: Update CLAUDE.md**

Record what is now true: ONNX inference and predictions exist for conn; the model bundle is a directory plus a frozen contract, pinned by configuration, with no hot reload; scoring is optional and its absence leaves the topology unchanged; and the verification table gains the new suite counts and the two end-to-end cases. Keep `ClickHouseOutageTest` listed as never executed.

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-online-job modules/bootstrap-archive-job CLAUDE.md
git commit -m "test: prove scoring end to end, and record it"
```

---

## Self-Review

**Spec coverage.** §4.1 domain → Task 2. §4.2 port → Task 3. §4.3 use case → Task 3. §4.4 registry + bundle contract → Task 4. §4.5 ONNX scorer → Task 5. §4.6 operator → Task 8. §4.7 Kafka + prediction contract → Tasks 4 and 6. §4.8 ClickHouse row → Task 7. §4.9 bootstrap + env → Task 9. §7.1 fail-fast tests → Tasks 3, 4, 5. §7.2 golden vectors → Tasks 1 and 5. §7.3 end to end → Task 10. §8 failure behaviour → Tasks 8 and 9 (startup failure comes free: `open()` throws). §4.10 `training/` and §5–§6 belong to Unit B and are deliberately absent.

**Ordering risk that is real.** Task 1 is a prerequisite for everything and depends on two things outside the repo: a reachable Maven repository and an installable `onnx` wheel. Both have explicit STOP instructions rather than workarounds, because a hand-made ONNX file or a vendored jar would be worse than a blocked task.

**Type consistency.** `ModelRef`'s nine components are fixed in Task 2 and consumed unchanged in Tasks 3, 4, 5 and 9. `LoadedModel.onnx()` returns `byte[]`, which is what `OnnxModelScorer`'s constructor takes. `ScoreFeaturesUseCase.score` returns `Prediction`, which is what the operator collects, what `PredictionSerializer` writes, and what `PredictionRowMapper` maps. `featureCount` reaches the scorer from `FeatureSchemaRegistry`, never from a literal 20 in production code — the literal appears only in tests.

**What this plan deliberately does not do.** No multi-class output, no DNS or HTTP scoring, no hot reload, no registry service, no drift monitoring. Unit B replaces the fixture bundle with a trained one and should need no Java change; if it does, that is a finding about these interfaces.
