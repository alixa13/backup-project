package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The fixture bundle (the real delivery, packaged) loads, and every check the
// spec lists (section 7) refuses a bundle that fails it, naming the failure.
class SequenceDetectorBundleLoaderTest {

    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "modbus-stage1-detector", "v1");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // A realistic READ_HOLDING_REGISTERS response vector and its preprocessing,
    // computed by the contract's own transform_definitions in Python.
    private static final float[] RAW = {1, 0, 0, 1, 0, 0, 0, 0, 100, 1, 2, 1, 1, 0, 0, 0, 0, 0, 1, 2, 7, 9, 8, 1,
        0.25f, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0.25f, 2, 0.2f, 0.0333333333f, 1, 1, 1, 0};
    private static final float[] PREPROCESSED = {1, 0, 0, 1, 0, 0, 0, 0, 11.1887788772583f, 1, 2, 1, 1, 0, 0, 0, 0,
        0, 1, 2, -0.05083692446351051f, 0.011502460576593876f, -0.017931997776031494f, 1, -0.17137247323989868f,
        0, 1, 8.680343307787552e-05f, 1, 0, 0.23271413147449493f, 0, 0, 1, 0.2231435477733612f,
        1.0986123085021973f, 0.18232156336307526f, 0.03278982266783714f, -9.88362979888916f,
        -4.549434185028076f, 1, 0};

    @Test
    void theFixtureBundleLoadsWithTheDeliveredValues() throws IOException {
        LoadedSequenceDetector loaded = SequenceDetectorBundleLoader.load(FIXTURE);
        SequenceDetectorBundle b = loaded.bundle();
        assertEquals("modbus-stage1-detector/v1", b.bundleId());
        assertEquals("b5f28fec103bddceb9b1bfcf1f6cc2e36d0780eba5340bd4a663be04bb72bf4f", b.modelSha());
        assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, b.schemaHash(), "the registered schema's hash");
        assertEquals(0.2483385056257248, b.denseThreshold());
        assertEquals(0.4121147692203522, b.temporalThreshold());
        assertEquals(20, b.sequenceLength());
        assertEquals("sequence_20x42", b.inputName());
        assertEquals(Files.size(FIXTURE.resolve("model.onnx")), loaded.model().length);
    }

    // The loader's preprocessing reproduces the contract's own arithmetic.
    @Test
    void thePreprocessingMatchesPython() throws IOException {
        float[] out = SequenceDetectorBundleLoader.load(FIXTURE).bundle().preprocessing().apply(RAW);
        for (int i = 0; i < out.length; i++) {
            assertEquals(PREPROCESSED[i], out[i], 1e-6f, "feature " + i);
        }
    }

    @Test
    void aModelThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.write(b.resolve("model.onnx"), new byte[]{1, 2, 3});
        assertRefused(b, "model.onnx");
    }

    @Test
    void aPreprocessingFileThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.writeString(b.resolve("preprocessing.json"), Files.readString(b.resolve("preprocessing.json")) + " ");
        assertRefused(b, "preprocessing.json");
    }

    @Test
    void aThresholdsFileThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.writeString(b.resolve("thresholds.json"), Files.readString(b.resolve("thresholds.json")) + " ");
        assertRefused(b, "thresholds.json");
    }

    // A preprocessing contract whose feature order differs from the schema's --
    // two features swapped, and its SHA updated so only the order is wrong.
    @Test
    void aFeatureOrderThatDiffersFromTheSchemaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode prep = (ObjectNode) new ObjectMapper()
            .enable(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
            .readTree(b.resolve("preprocessing.json").toFile());
        ArrayNode order = (ArrayNode) prep.get("feature_order");
        String first = order.get(0).asText();
        order.set(0, order.get(1));
        order.set(1, MAPPER.getNodeFactory().textNode(first));
        Files.writeString(b.resolve("preprocessing.json"), prep.toString());
        rewriteSha(b, "preprocessingSha", "preprocessing.json");
        assertRefused(b, "feature order");
    }

    @Test
    void aMissingFileIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.delete(b.resolve("thresholds.json"));
        assertThrows(IOException.class, () -> SequenceDetectorBundleLoader.load(b));
    }

    @Test
    void anUnknownSchemaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode bundle = (ObjectNode) MAPPER.readTree(b.resolve("bundle.json").toFile());
        bundle.put("schemaId", "no-such-schema");
        Files.writeString(b.resolve("bundle.json"), bundle.toString());
        assertThrows(IllegalArgumentException.class, () -> SequenceDetectorBundleLoader.load(b));
    }

    private static void assertRefused(Path bundle, String named) {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> SequenceDetectorBundleLoader.load(bundle));
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
