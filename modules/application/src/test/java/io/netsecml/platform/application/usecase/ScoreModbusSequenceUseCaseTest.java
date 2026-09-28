package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The window rules, the decision rule and the prediction's fields, with a stub
// scorer: no ONNX Runtime here. The preprocessing is 42 passthroughs except
// feature 35 (event_rate_1s), GLOBAL_LOG1P_ONLY, so a negative value there
// yields NaN on demand.
class ScoreModbusSequenceUseCaseTest {

    private static final int PREV_EVENT_AVAILABLE = 23;
    private static final ModbusEntityKey KEY = new ModbusEntityKey(new SensorId("s"), "10.0.0.5", "10.0.0.9", "1");

    // A stub that records every sequence it was given and returns fixed scores.
    private static final class StubScorer implements SequenceScorer {
        private final SequenceDetectorBundle bundle;
        final List<float[][]> seen = new ArrayList<>();
        DetectorScores next = new DetectorScores(0.1, 0.1);

        StubScorer(String version) {
            this.bundle = testBundle(version);
        }

        @Override
        public SequenceDetectorBundle bundle() {
            return bundle;
        }

        @Override
        public DetectorScores score(float[][] sequence) {
            seen.add(sequence);
            return next;
        }

        @Override
        public void close() {
        }
    }

    private static SequenceDetectorBundle testBundle(String version) {
        List<FeaturePreprocessing> features = IntStream.range(0, 42)
            .mapToObj(i -> new FeaturePreprocessing("f" + i,
                i == 35 ? PreprocessingPolicy.GLOBAL_LOG1P_ONLY : PreprocessingPolicy.PASSTHROUGH_BINARY,
                -1, Double.NaN, Double.NaN))
            .toList();
        return new SequenceDetectorBundle("modbus-stage1-detector", version, "b".repeat(64),
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, 20, 42, "sequence_20x42",
            "modbus_dense_autoencoder", "modbus_causal_next_event_predictor", 0.5, 0.5,
            new ModbusPreprocessing(features));
    }

    // Event i of a stream: feature 0 carries i (so sequences are recognisable),
    // prev_event_available is 1 except where a segment starts.
    private static FeatureVector vector(int i, boolean segmentStart, int flags) {
        float[] values = new float[42];
        values[0] = i;
        values[PREV_EVENT_AVAILABLE] = segmentStart ? 0f : 1f;
        return new FeatureVector("s:u:" + i + ":REQUEST:" + i, Instant.ofEpochSecond(1000 + i), new SensorId("s"),
            LogType.MODBUS, "u", ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, values,
            flags, Instant.ofEpochSecond(1000 + i));
    }

    private final Clock clock = Clock.fixed(Instant.parse("2026-09-26T12:00:00Z"), ZoneOffset.UTC);

    // Runs events 0..n-1 of one segment through the use case.
    private static List<ModbusDetectorPrediction> run(ScoreModbusSequenceUseCase useCase, int n) {
        List<ModbusDetectorPrediction> out = new ArrayList<>();
        ModbusScoreWindow window = null;
        for (int i = 0; i < n; i++) {
            ModbusScoringResult result = useCase.score(KEY, vector(i, i == 0, 0), window);
            window = result.window();
            out.add(result.prediction());
        }
        return out;
    }

    @Test
    void theFirstNineteenEventsWarmUpAndTheTwentiethIsScored() {
        StubScorer scorer = new StubScorer("v1");
        List<ModbusDetectorPrediction> out = run(new ScoreModbusSequenceUseCase(scorer, clock), 20);
        for (int i = 0; i < 19; i++) {
            assertEquals(DetectorVerdict.WARMUP, out.get(i).verdict());
            assertEquals(i + 1, out.get(i).windowEvents());
            assertNull(out.get(i).denseScore());
            assertEquals(0L, out.get(i).inferenceMicros());
        }
        assertEquals(DetectorVerdict.NORMAL, out.get(19).verdict());
        assertEquals(20, out.get(19).windowEvents());
        assertEquals(1, scorer.seen.size(), "the detector runs only on a full window");
    }

    // The detector sees the last 20 preprocessed rows, oldest first.
    @Test
    void theDetectorSeesTheLastTwentyRowsOldestFirst() {
        StubScorer scorer = new StubScorer("v1");
        run(new ScoreModbusSequenceUseCase(scorer, clock), 25);
        float[][] last = scorer.seen.get(scorer.seen.size() - 1);
        assertEquals(20, last.length);
        assertEquals(5f, last[0][0]);
        assertEquals(24f, last[19][0]);
    }

    // Strictly greater: a score equal to its threshold is not an anomaly; the
    // trigger names the head that exceeded its threshold.
    @Test
    void theThresholdsAreStrictAndTheTriggerNamesTheHead() {
        StubScorer scorer = new StubScorer("v1");
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(scorer, clock);
        ModbusScoreWindow window = null;
        for (int i = 0; i < 19; i++) {
            window = useCase.score(KEY, vector(i, i == 0, 0), window).window();
        }
        double[][] cases = {{0.5, 0.5}, {0.6, 0.1}, {0.1, 0.6}, {0.6, 0.6}};
        DetectorTrigger[] triggers = {DetectorTrigger.NONE, DetectorTrigger.DENSE, DetectorTrigger.TEMPORAL,
            DetectorTrigger.BOTH};
        for (int c = 0; c < cases.length; c++) {
            scorer.next = new DetectorScores(cases[c][0], cases[c][1]);
            ModbusScoringResult r = useCase.score(KEY, vector(19 + c, false, 0), window);
            window = r.window();
            assertEquals(triggers[c], r.prediction().trigger());
            assertEquals(c == 0 ? DetectorVerdict.NORMAL : DetectorVerdict.ANOMALY, r.prediction().verdict());
            assertEquals((float) cases[c][0], r.prediction().denseScore());
        }
    }

    // A segment start empties the window: the stream warms up again.
    @Test
    void aSegmentStartReWarms() {
        StubScorer scorer = new StubScorer("v1");
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(scorer, clock);
        ModbusScoreWindow window = null;
        for (int i = 0; i < 20; i++) {
            window = useCase.score(KEY, vector(i, i == 0, 0), window).window();
        }
        ModbusScoringResult restart = useCase.score(KEY, vector(20, true, 0), window);
        assertEquals(DetectorVerdict.WARMUP, restart.prediction().verdict());
        assertEquals(1, restart.prediction().windowEvents());
    }

    // A non-finite preprocessed vector is UNSCORABLE and never enters the window.
    @Test
    void aNonFiniteVectorIsUnscorableAndLeavesTheWindowAsItWas() {
        StubScorer scorer = new StubScorer("v1");
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(scorer, clock);
        ModbusScoreWindow window = useCase.score(KEY, vector(0, true, 0), null).window();
        FeatureVector bad = vector(1, false, 0);
        float[] values = bad.values();
        values[35] = -2f;
        FeatureVector poisoned = new FeatureVector(bad.eventId(), bad.eventTime(), bad.sensor(), bad.logType(),
            bad.connectionUid(), bad.schemaId(), bad.schemaHash(), values, 0, bad.producedAt());
        ModbusScoringResult r = useCase.score(KEY, poisoned, window);
        assertEquals(DetectorVerdict.UNSCORABLE, r.prediction().verdict());
        assertEquals(1, r.prediction().windowEvents(), "the window still holds only event 0");
        assertNull(r.prediction().denseScore());
    }

    // Review Focus 4: a window filled under another bundle is never scored.
    @Test
    void aWindowFromAnotherBundleIsEmptiedFirst() {
        ModbusScoreWindow fromV1 = null;
        ScoreModbusSequenceUseCase v1 = new ScoreModbusSequenceUseCase(new StubScorer("v1"), clock);
        for (int i = 0; i < 19; i++) {
            fromV1 = v1.score(KEY, vector(i, i == 0, 0), fromV1).window();
        }
        StubScorer v2Scorer = new StubScorer("v2");
        ModbusScoringResult r = new ScoreModbusSequenceUseCase(v2Scorer, clock).score(KEY, vector(19, false, 0), fromV1);
        assertEquals(DetectorVerdict.WARMUP, r.prediction().verdict());
        assertEquals(1, r.prediction().windowEvents());
        assertEquals("modbus-stage1-detector/v2", r.window().bundleId());
        assertTrue(v2Scorer.seen.isEmpty());
    }

    // qualityFlags is the OR of this vector's flags and the window's.
    @Test
    void theFlagsAreOredOverTheWindow() {
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(new StubScorer("v1"), clock);
        ModbusScoreWindow window = useCase.score(KEY, vector(0, true, 8), null).window();
        ModbusScoringResult r = useCase.score(KEY, vector(1, false, 4), window);
        assertEquals(12, r.prediction().qualityFlags());
    }

    // The prediction names the device, the model and the event it came from.
    @Test
    void thePredictionCarriesTheKeyTheModelAndADeterministicId() {
        ModbusDetectorPrediction p = new ScoreModbusSequenceUseCase(new StubScorer("v1"), clock)
            .score(KEY, vector(0, true, 0), null).prediction();
        assertEquals("10.0.0.5", p.clientIp());
        assertEquals("10.0.0.9", p.serverIp());
        assertEquals("1", p.unitId());
        assertEquals("modbus-stage1-detector", p.modelName());
        assertEquals(Prediction.deriveId("s:u:0:REQUEST:0", "modbus-stage1-detector", "v1"), p.predictionId());
        assertEquals(0.5f, p.denseThreshold());
        assertEquals(Instant.parse("2026-09-26T12:00:00Z"), p.producedAt());
    }
}
