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

// Detector v2's graph, run from Java: its last-row reconstruction equals Python
// ONNX Runtime's (1.30) for the same windows, to 1e-5, and a bundle whose names
// or window length do not fit the graph is refused (spec amendment A1).
class OnnxReconstructionScorerTest {

    private static final Path MODEL = Path.of("..", "..", "tests", "fixtures", "models", "s7comm-stage1-detector",
        "v2", "model.onnx");

    // Python ONNX Runtime 1.30: the last row's reconstruction of an all-zero window
    // (v2 trained its operation columns at weight 0, so they reconstruct as ~0).
    private static final float[] ZERO_LAST = {-0.04589303955435753f, -0.19972345232963562f, 0.8403432369232178f,
        0.14997804164886475f, 0.421733558177948f, 0.4204365015029907f, 0.28379422426223755f,
        -0.11769122630357742f, -0.09661537408828735f, -0.181127667427063f, 0.2642660439014435f,
        0.057057902216911316f, 0.047090642154216766f, 0.002314627170562744f, 0.045590534806251526f,
        0.7935324907302856f, -8.935624464356806e-06f, 0f, 0f, 0f, 0f, 0f};
    // A user-data response's preprocessed row (S7commDetectorBundleLoaderTest's
    // second row), repeated 16 times, and the last row's reconstruction.
    private static final float[] ROW = {1.0f, 1.8333333730697632f, 0.25f, -0.027721064165234566f, 3.0f, 0.75f,
        0.25f, 0.5f, 0.625f, 0.375f, 0.5f, 0.875f, 0.0f, 1.0f, 0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
    private static final float[] REPEAT_LAST = {0.04154300317168236f, 0.3370157480239868f, 0.9361931085586548f,
        -0.18560682237148285f, 1.0426274538040161f, 0.46938419342041016f, 0.7857968807220459f,
        0.04017895460128784f, 0.0536237508058548f, 0.08892036974430084f, 0.7749016880989075f,
        0.02085326611995697f, 0.04137092083692551f, 0.049833089113235474f, 0.04261518269777298f,
        0.8930628299713135f, 1.4687198927276768e-05f, 0f, 0f, 0f, 0f, 0f};

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
            List.of("FUNCTION_0x44", "FUNCTION_0x84", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            scores.put(g, new double[]{0.1});
        }
        return new S7commDetectorBundle("s7comm-stage1-detector", "v2", "b".repeat(64),
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, sequenceLength, 22, input, output,
            p, p.scoreWeights(List.of("s7_operation")), new S7commConformalPolicy(scores, Map.of(), 0.001));
    }

    private static S7commDetectorBundle released() {
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
        try (OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), released())) {
            assertRow(ZERO_LAST, scorer.reconstructLast(new float[16][22]));
        }
    }

    @Test
    void aRepeatedRowReconstructsAsPythonDoes() throws IOException {
        float[][] window = new float[16][];
        for (int i = 0; i < 16; i++) {
            window[i] = ROW.clone();
        }
        try (OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), released())) {
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
        try (OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), released())) {
            assertThrows(IllegalArgumentException.class, () -> scorer.reconstructLast(new float[15][22]));
            assertThrows(IllegalArgumentException.class, () -> scorer.reconstructLast(new float[16][21]));
        }
    }

    @Test
    void closeIsIdempotent() throws IOException {
        OnnxReconstructionScorer scorer = new OnnxReconstructionScorer(model(), released());
        scorer.close();
        assertDoesNotThrow(scorer::close);
    }
}
