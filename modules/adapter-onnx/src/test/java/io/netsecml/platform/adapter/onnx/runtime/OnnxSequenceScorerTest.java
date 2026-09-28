package io.netsecml.platform.adapter.onnx.runtime;

import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The delivered graph, run from Java: both scores equal Python ONNX Runtime's
// (1.30) for the same windows, to 1e-5, and a bundle whose names or shapes do
// not fit the graph is refused.
class OnnxSequenceScorerTest {

    private static final Path MODEL = Path.of("..", "..", "tests", "fixtures", "models", "modbus-stage1-detector",
        "v1", "model.onnx");

    // One preprocessed READ_HOLDING_REGISTERS response (see
    // SequenceDetectorBundleLoaderTest.PREPROCESSED), repeated 20 times.
    private static final float[] ROW = {1, 0, 0, 1, 0, 0, 0, 0, 11.1887788772583f, 1, 2, 1, 1, 0, 0, 0, 0, 0, 1, 2,
        -0.05083692446351051f, 0.011502460576593876f, -0.017931997776031494f, 1, -0.17137247323989868f, 0, 1,
        8.680343307787552e-05f, 1, 0, 0.23271413147449493f, 0, 0, 1, 0.2231435477733612f, 1.0986123085021973f,
        0.18232156336307526f, 0.03278982266783714f, -9.88362979888916f, -4.549434185028076f, 1, 0};

    private static byte[] model() throws IOException {
        return Files.readAllBytes(MODEL);
    }

    private static SequenceDetectorBundle bundle(int sequenceLength, String input, String dense, String temporal) {
        return new SequenceDetectorBundle("modbus-stage1-detector", "v1", "b".repeat(64),
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, sequenceLength, 42, input, dense,
            temporal, 0.2483385056257248, 0.4121147692203522, new ModbusPreprocessing(IntStream.range(0, 42)
                .mapToObj(i -> new FeaturePreprocessing("f" + i, PreprocessingPolicy.PASSTHROUGH_BINARY, -1,
                    Double.NaN, Double.NaN)).toList()));
    }

    private static SequenceDetectorBundle delivered() {
        return bundle(20, "sequence_20x42", "modbus_dense_autoencoder", "modbus_causal_next_event_predictor");
    }

    @Test
    void anAllZeroWindowScoresAsPythonDoes() throws IOException {
        try (OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered())) {
            DetectorScores s = scorer.score(new float[20][42]);
            assertEquals(0.3135979175567627, s.dense(), 1e-5);
            assertEquals(0.3785625994205475, s.temporal(), 1e-5);
        }
    }

    @Test
    void aRealisticWindowScoresAsPythonDoes() throws IOException {
        float[][] window = new float[20][];
        for (int i = 0; i < 20; i++) {
            window[i] = ROW.clone();
        }
        try (OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered())) {
            DetectorScores s = scorer.score(window);
            assertEquals(0.8671411275863647, s.dense(), 1e-5);
            assertEquals(0.7443642616271973, s.temporal(), 1e-5);
        }
    }

    @Test
    void aWindowOfTheWrongShapeIsRejected() throws IOException {
        try (OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered())) {
            assertThrows(IllegalArgumentException.class, () -> scorer.score(new float[19][42]));
            assertThrows(IllegalArgumentException.class, () -> scorer.score(new float[20][41]));
        }
    }

    @Test
    void aBundleNamingAnotherInputIsRefused() {
        assertThrows(IllegalStateException.class, () -> new OnnxSequenceScorer(model(),
            bundle(20, "input", "modbus_dense_autoencoder", "modbus_causal_next_event_predictor")));
    }

    @Test
    void aBundleNamingAnotherOutputIsRefused() {
        assertThrows(IllegalStateException.class, () -> new OnnxSequenceScorer(model(),
            bundle(20, "sequence_20x42", "reconstruction", "modbus_causal_next_event_predictor")));
    }

    @Test
    void aBundleWhoseSequenceLengthDiffersFromTheGraphIsRefused() {
        assertThrows(IllegalStateException.class, () -> new OnnxSequenceScorer(model(),
            bundle(16, "sequence_20x42", "modbus_dense_autoencoder", "modbus_causal_next_event_predictor")));
    }

    @Test
    void closingTwiceIsHarmless() throws IOException {
        OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered());
        scorer.close();
        assertDoesNotThrow(scorer::close);
    }
}
