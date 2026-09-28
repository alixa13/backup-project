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

// The fixture bundle (detector v2's release, packaged) loads with v2's values,
// its preprocessing reproduces v2's Python preprocessor, and every check of spec
// section 7 refuses a bundle that fails it, naming it (spec amendment A1).
class S7commDetectorBundleLoaderTest {

    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v2");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Three raw s7comm-feature-v1 vectors and v2's preprocessor.joblib output for
    // them, computed in Python (scikit-learn 1.9.1, training/.venv) with the
    // categories exactly as S7commCategories decodes them: a WRITE_VAR request, a
    // user-data response (operation code 0 -> FUNCTION_0x00, which v2 never saw:
    // all operation columns 0) and an ACK carrying PLC_STOP (ROSCTR 2 and
    // PLC_STOP are both unseen categories: all zeros).
    private static final float[][] RAW = {
        {1, 0.5f, 1, 1617, 1, 0.5f, 1, 0, 0, 0, 1, 1, 1, 0, 1, 5},
        {3, 2.5f, 0.25f, 2, 4, 0.75f, 0.25f, 0.5f, 0.625f, 0.375f, 0.5f, 0.875f, 0, 1, 7, 0},
        {0, 0, 0, 50000, 40, 0, 0, 1, 1, 1, 0, 0.03125f, 1, 1, 2, 0x29},
    };
    private static final float[][] PREPROCESSED = {
        {0.0f, -0.8333333134651184f, 1.0f, 0.008354293182492256f, 0.0f, 0.5f, 1.0f, 0.0f, 0.0f, 0.0f, 1.0f, 1.0f,
            1.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f},
        {1.0f, 1.8333333730697632f, 0.25f, -0.027721064165234566f, 3.0f, 0.75f, 0.25f, 0.5f, 0.625f, 0.375f, 0.5f,
            0.875f, 0.0f, 1.0f, 0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
        {-0.5f, -1.5f, 0.0f, 1.0891183614730835f, 20.0f, 0.0f, 0.0f, 1.0f, 1.0f, 1.0f, 0.0f, 0.03125f, 1.0f, 1.0f,
            0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
    };

    @Test
    void theFixtureBundleLoadsWithV2sValues() throws IOException {
        LoadedS7commDetector loaded = S7commDetectorBundleLoader.load(FIXTURE);
        S7commDetectorBundle b = loaded.bundle();
        assertEquals("s7comm-stage1-detector/v2", b.bundleId());
        assertEquals("59992382178b1f367e6f189e41f69ceaee0b96d69f228b8a3d631bc5c2571002", b.modelSha());
        assertEquals(S7commFeatureSchemaV1.CONTENT_HASH, b.schemaHash(), "the registered schema's hash");
        assertEquals(16, b.sequenceLength());
        assertEquals(22, b.featureCount());
        assertEquals("input", b.inputName());
        assertEquals("reconstruction", b.outputName());
        assertEquals(Files.size(FIXTURE.resolve("model.onnx")), loaded.model().length);
        // Σw = 17: the five s7_operation columns weigh nothing.
        float sum = 0f;
        for (float w : b.scoreWeights()) {
            sum += w;
        }
        assertEquals(17f, sum);
        assertEquals(0f, b.scoreWeights()[17]);
    }

    @Test
    void thePolicyIsV2s() throws IOException {
        S7commConformalPolicy p = S7commDetectorBundleLoader.load(FIXTURE).bundle().policy();
        assertEquals(32760, p.calibrationSize(S7commScoreGroup.RESPONSE));
        assertEquals(31978, p.calibrationSize(S7commScoreGroup.READ_REQUEST));
        assertEquals(782, p.calibrationSize(S7commScoreGroup.WRITE_REQUEST));
        assertEquals(32760 + 31978 + 782, p.calibrationSize(S7commScoreGroup.OTHER_REQUEST),
            "no scores: every group pooled");
        assertEquals(0.001, p.alpha(S7commScoreGroup.RESPONSE));
        assertEquals(0.001, p.alpha(S7commScoreGroup.READ_REQUEST));
        assertEquals(1.0 / 783, p.alpha(S7commScoreGroup.WRITE_REQUEST), "782 scores: the smallest p-value, 1/783");
        assertEquals(0.001, p.alpha(S7commScoreGroup.OTHER_REQUEST), "the fallback");
        // The largest WRITE calibration score is 0.007234219461679459.
        assertFalse(p.anomalous(S7commScoreGroup.WRITE_REQUEST, 0.007234219461679459));
        assertTrue(p.anomalous(S7commScoreGroup.WRITE_REQUEST, 0.00724));
    }

    // The loader's preprocessing reproduces v2's Python preprocessor.
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
        Path b = dir.resolve("v2");
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
