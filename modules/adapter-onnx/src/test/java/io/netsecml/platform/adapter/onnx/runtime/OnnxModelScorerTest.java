package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.model.ModelRef;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Proves OnnxModelScorer reproduces the fixture's known, hand-computed scores
// (not merely "runs without throwing"), and that it fails loudly rather than
// silently at every boundary a streaming job would otherwise hit at runtime:
// a mismatched graph/schema width at construction, and a mismatched
// caller-supplied vector width at score() time.
class OnnxModelScorerTest {

    private static final Path BUNDLE = Path.of("..", "..", "tests", "fixtures", "models", "conn-demo-v1");

    private static JsonNode bundle() throws IOException {
        return new ObjectMapper().readTree(BUNDLE.resolve("bundle.json").toFile());
    }

    private static byte[] model() throws IOException {
        return Files.readAllBytes(BUNDLE.resolve("model.onnx"));
    }

    private static ModelRef refFrom(JsonNode b) {
        List<String> classes = new ArrayList<>();
        b.get("classes").forEach(c -> classes.add(c.asText()));
        return new ModelRef(b.get("name").asText(), b.get("version").asText(), b.get("schemaId").asText(),
                b.get("schemaHash").asText(), b.get("modelSha").asText(), (float) b.get("threshold").asDouble(),
                classes, b.get("outputName").asText(), b.get("positiveClassColumn").asInt());
    }

    @Test
    void reproducesEveryGoldenScoreFromTheBundle() throws Exception {
        // The fixture is sigmoid(x.W + b) with fixed weights, and bundle.json records
        // what that evaluates to. If ONNX Runtime computed something subtly different
        // -- a changed opset, a different accumulation order -- these disagree. No
        // trained model could give us that guarantee.
        JsonNode bundle = bundle();
        byte[] model = model();
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
        byte[] model = model();
        // 24 is dns-feature-v1's width: pairing this model with that schema must fail
        // loudly at startup rather than throwing on the first record in production.
        assertThrows(IllegalStateException.class, () -> new OnnxModelScorer(model, refFrom(bundle()), 24));
    }

    @Test
    void scoreRejectsAVectorWhoseWidthDiffersFromTheGraphsExpectedWidth() throws Exception {
        // Width is enforced by the scorer, not the caller: ScoreFeaturesUseCase
        // only binds schema identity (schemaId/schemaHash), never array length.
        // A wrong-width array reaching score() must fail with a message an
        // operator can act on, not an opaque OrtException from tensor creation.
        try (OnnxModelScorer scorer = new OnnxModelScorer(model(), refFrom(bundle()), 20)) {
            float[] tooShort = new float[19];
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> scorer.score(tooShort));
            assertTrue(ex.getMessage().contains("20"), "message should name the expected width");
            assertTrue(ex.getMessage().contains("19"), "message should name the width actually received");
        }
    }

    @Test
    void closeIsIdempotentAndDoesNotThrow() throws Exception {
        // ModelScorer narrows AutoCloseable.close() to throw nothing at all, and a
        // use case may legitimately call close() once via try-with-resources with
        // no further interaction -- closing twice must not surface a native
        // use-after-free as an unchecked exception either.
        OnnxModelScorer scorer = new OnnxModelScorer(model(), refFrom(bundle()), 20);
        assertDoesNotThrow(scorer::close);
        assertDoesNotThrow(scorer::close);
    }

    @Test
    void aRefNamingAnOutputTheGraphDoesNotHaveIsRejectedAtConstruction() throws Exception {
        // The fixture graph's only output is "probability". A ref naming an
        // output the graph doesn't have is a ref/graph pairing error exactly
        // like the input-width check above: it is invariant across every
        // record, so it must fail here rather than throwing an opaque failure
        // on the first call to score() in production.
        JsonNode bundle = bundle();
        List<String> classes = new ArrayList<>();
        bundle.get("classes").forEach(c -> classes.add(c.asText()));
        ModelRef wrongOutputRef = new ModelRef(bundle.get("name").asText(), bundle.get("version").asText(),
                bundle.get("schemaId").asText(), bundle.get("schemaHash").asText(), bundle.get("modelSha").asText(),
                (float) bundle.get("threshold").asDouble(), classes, "not_a_real_output",
                bundle.get("positiveClassColumn").asInt());
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new OnnxModelScorer(model(), wrongOutputRef, 20));
        assertTrue(ex.getMessage().contains("not_a_real_output"), "message should name the wanted output");
        assertTrue(ex.getMessage().contains("probability"), "message should list what the graph actually has");
    }

    @Test
    void aPositiveClassColumnBeyondTheOutputTensorsWidthIsRejectedAtConstruction() throws Exception {
        // The fixture graph's "probability" output is [1, 1] -- one column.
        // A bundle naming column 1 against it is a ref/graph pairing error
        // exactly like the two checks above: it must fail here, not throw
        // ArrayIndexOutOfBoundsException on the first scored record.
        JsonNode bundle = bundle();
        List<String> classes = new ArrayList<>();
        bundle.get("classes").forEach(c -> classes.add(c.asText()));
        ModelRef wrongColumnRef = new ModelRef(bundle.get("name").asText(), bundle.get("version").asText(),
                bundle.get("schemaId").asText(), bundle.get("schemaHash").asText(), bundle.get("modelSha").asText(),
                (float) bundle.get("threshold").asDouble(), classes, bundle.get("outputName").asText(), 1);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new OnnxModelScorer(model(), wrongColumnRef, 20));
        assertTrue(ex.getMessage().contains("positiveClassColumn"), "message should name the field at fault");
        assertTrue(ex.getMessage().contains("1"), "message should name the requested column");
        assertTrue(ex.getMessage().contains("probability"), "message should name the output it was checked against");
    }

    @Test
    void requireFloatTensorOutputRejectsANonFloatTensor() throws Exception {
        // Exercises the element-type guard directly, using ONNX Runtime's own
        // TensorInfo for a real (if unrelated) int64 tensor rather than a
        // second .onnx fixture whose graph would need to produce a non-float
        // output. Without this guard, a graph whose named output is an int64
        // label tensor -- or a scikit-learn zipmap sequence-of-maps output --
        // would reach the `(float[][]) ... getValue()` cast in score() and
        // throw ClassCastException on the first scored record instead of
        // failing here, at construction.
        try (OnnxTensor longTensor = OnnxTensor.createTensor(OrtEnvironment.getEnvironment(), new long[][] {{1L}})) {
            NodeInfo intOutput = new NodeInfo("label", longTensor.getInfo());
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> OnnxModelScorer.requireFloatTensorOutput("label", intOutput));
            assertTrue(ex.getMessage().contains("label"), "message should name the output");
            assertTrue(ex.getMessage().contains("INT64"), "message should name the element type actually found");
        }
    }

    @Test
    void refReturnsExactlyTheRefPassedToTheConstructor() throws Exception {
        // ScoreFeaturesUseCase binds schema identity and threshold by reading
        // ref() once per score() call -- if the scorer normalized, copied, or
        // otherwise diverged from the ref it was given, that binding would be
        // silently wrong.
        ModelRef ref = refFrom(bundle());
        try (OnnxModelScorer scorer = new OnnxModelScorer(model(), ref, 20)) {
            assertEquals(ref, scorer.ref());
        }
    }
}
