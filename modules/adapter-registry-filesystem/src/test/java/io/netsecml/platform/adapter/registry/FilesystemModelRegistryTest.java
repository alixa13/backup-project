package io.netsecml.platform.adapter.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilesystemModelRegistryTest {

    // Surefire runs with the working directory set to this module's basedir,
    // so this resolves to the repo root's tests/fixtures/models/conn-demo-v1/
    // (both bundle.json and model.onnx are tracked in git there).
    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models", "conn-demo-v1");

    @Test
    void loadsTheFixtureBundleAndItsModelBytes() throws Exception {
        LoadedModel loaded = FilesystemModelRegistry.load(FIXTURE);
        assertEquals("conn-demo", loaded.ref().name());
        assertEquals("conn-feature-v1", loaded.ref().schemaId());
        assertEquals(0.5f, loaded.ref().threshold(), 1e-6f);
        assertTrue(loaded.onnx().length > 0);
        // bundle.json's own sampleVectors carries 4 entries (expected scores
        // 0.3775406777858734, 0.3543437123298645, 0.994513750076294,
        // 0.4987500011920929) -- not 3.
        assertEquals(4, loaded.samples().size(), "the golden vectors must survive loading");
    }

    @Test
    void aModelFileThatDoesNotMatchItsRecordedShaIsRejected(@TempDir Path tmp) throws Exception {
        // A truncated or swapped model must never reach the runtime: it would
        // score silently and wrongly rather than failing.
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

    @Test
    void aMissingModelFileIsRejected(@TempDir Path tmp) throws Exception {
        // Same rule as the missing-bundle case, but the other file: bundle.json
        // present, model.onnx absent. Files.readAllBytes must surface this as
        // an IOException too, not an unchecked NPE or a silently empty model.
        Files.copy(FIXTURE.resolve("bundle.json"), tmp.resolve("bundle.json"));
        assertThrows(IOException.class, () -> FilesystemModelRegistry.load(tmp));
    }

    @Test
    void loadedSampleVectorsCarryTheBundlesExpectedScores() throws Exception {
        // Guards against a mapping-order or off-by-one bug in the
        // bundle.json-to-SampleVector conversion: each of the 4 fixture
        // vectors' expectedScore must survive in bundle.json's own order, and
        // each vector's length must match featureOrder's 20 entries.
        LoadedModel loaded = FilesystemModelRegistry.load(FIXTURE);
        List<Double> expectedScores =
                List.of(0.3775406777858734, 0.3543437123298645, 0.994513750076294, 0.4987500011920929);
        for (int i = 0; i < expectedScores.size(); i++) {
            assertEquals(expectedScores.get(i), loaded.samples().get(i).expectedScore(), 1e-12,
                    "sample " + i + " expectedScore");
            assertEquals(20, loaded.samples().get(i).values().length, "sample " + i + " value count");
        }
    }

    @Test
    void loadedOnnxBytesAreIndependentOfSubsequentMutation() throws Exception {
        // LoadedModel.onnx must defend against a caller mutating the array it
        // was handed back -- two successive calls to onnx() must not alias the
        // same backing array, or a caller could corrupt the bundle's own bytes
        // for every future reader.
        LoadedModel loaded = FilesystemModelRegistry.load(FIXTURE);
        byte[] first = loaded.onnx();
        first[0] = (byte) ~first[0];
        byte[] second = loaded.onnx();
        assertFalse(java.util.Arrays.equals(first, second), "mutating one onnx() copy must not affect another");
    }

    @Test
    void loadedSamplesListIsUnmodifiable() throws Exception {
        // LoadedModel.samples must not hand out a list a caller can grow or
        // shrink -- List.copyOf's own contract is what this pins.
        LoadedModel loaded = FilesystemModelRegistry.load(FIXTURE);
        assertThrows(UnsupportedOperationException.class,
                () -> loaded.samples().add(new SampleVector(new float[20], 0.0)));
    }

    // Loads the real fixture's bundle.json into a mutable JSON tree so a test
    // can corrupt exactly one field and still exercise the production
    // bundle.json-to-LoadedModel path, rather than a hand-built JSON string
    // that could drift from the real fixture's shape.
    private static ObjectNode mutableFixtureBundle() throws IOException {
        return (ObjectNode) new ObjectMapper().readTree(FIXTURE.resolve("bundle.json").toFile());
    }

    // Writes a (possibly mutated) bundle tree plus the fixture's own
    // model.onnx into tmp, so FilesystemModelRegistry.load(tmp) sees a
    // directory shaped exactly like a real bundle except for the one edited
    // field.
    private static void writeBundle(Path tmp, ObjectNode bundle) throws IOException {
        Files.copy(FIXTURE.resolve("model.onnx"), tmp.resolve("model.onnx"));
        new ObjectMapper().writeValue(tmp.resolve("bundle.json").toFile(), bundle);
    }

    @Test
    void explicitNullThresholdIsRejected(@TempDir Path tmp) throws Exception {
        // Without DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, this
        // exact bundle.json would load with threshold silently coerced to
        // 0.0, which passes ModelRef's 0..1 range check and makes
        // `score >= threshold` true for every prediction -- see
        // FilesystemModelRegistry.MAPPER's comment. This pins the fix.
        ObjectNode bundle = mutableFixtureBundle();
        bundle.putNull("threshold");
        writeBundle(tmp, bundle);
        IOException thrown = assertThrows(IOException.class, () -> FilesystemModelRegistry.load(tmp));
        assertTrue(thrown.getMessage().contains("threshold"), () -> "unhelpful message: " + thrown.getMessage());
    }

    @Test
    void explicitNullPositiveClassColumnIsRejected(@TempDir Path tmp) throws Exception {
        // Same defect shape as threshold above, for the platform's other
        // required primitive field.
        ObjectNode bundle = mutableFixtureBundle();
        bundle.putNull("positiveClassColumn");
        writeBundle(tmp, bundle);
        IOException thrown = assertThrows(IOException.class, () -> FilesystemModelRegistry.load(tmp));
        assertTrue(thrown.getMessage().contains("positiveClassColumn"),
                () -> "unhelpful message: " + thrown.getMessage());
    }

    @Test
    void aBundleJsonMissingARequiredFieldIsRejected(@TempDir Path tmp) throws Exception {
        // The precedent this follows: adapter-kafka's ZeekDnsEvent uses the
        // same @JsonProperty(required = true) pattern and is pinned by
        // JsonZeekDnsParserTest.missingTransIdFailsToParse. Nothing in this
        // module previously pinned the absent-key half of the same rule for
        // BundleJson.
        ObjectNode bundle = mutableFixtureBundle();
        bundle.remove("threshold");
        writeBundle(tmp, bundle);
        assertThrows(IOException.class, () -> FilesystemModelRegistry.load(tmp));
    }

    @Test
    void aNullElementInsideASampleVectorsValuesArrayIsRejected(@TempDir Path tmp) throws Exception {
        // Jackson binds a JSON null inside a List<Double> silently -- no
        // exception at parse time -- so without toFloatArray's own check this
        // would previously reach `.floatValue()` and throw a bare,
        // unhelpful NullPointerException instead of naming the sample and
        // index.
        ObjectNode bundle = mutableFixtureBundle();
        ArrayNode values = (ArrayNode) bundle.get("sampleVectors").get(0).get("values");
        values.set(2, NullNode.getInstance());
        writeBundle(tmp, bundle);
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> FilesystemModelRegistry.load(tmp));
        assertTrue(thrown.getMessage().contains("sampleVectors[0].values[2]"),
                () -> "unhelpful message: " + thrown.getMessage());
    }

    @Test
    void aNullSampleVectorsListIsRejected(@TempDir Path tmp) throws Exception {
        // sampleVectors is a required List, a reference type that
        // FAIL_ON_NULL_FOR_PRIMITIVES does not cover -- toSampleVectors's own
        // null check is what stops "sampleVectors": null from reaching
        // .stream() as a bare NullPointerException.
        ObjectNode bundle = mutableFixtureBundle();
        bundle.putNull("sampleVectors");
        writeBundle(tmp, bundle);
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> FilesystemModelRegistry.load(tmp));
        assertTrue(thrown.getMessage().contains("sampleVectors"),
                () -> "unhelpful message: " + thrown.getMessage());
    }
}
