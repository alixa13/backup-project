package io.netsecml.platform.adapter.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
