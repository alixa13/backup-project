package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;
import io.netsecml.platform.port.out.ReconstructionScorer;
import io.netsecml.platform.port.out.ReconstructionScorerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The scoring operator in a Flink harness, with a stub detector: the 64-event
// warm-up (scoring spec amendment A1), a fresh connection, the idle TTL, a restore
// mid-window giving exactly an uninterrupted run's predictions, and scoring
// switched off and on.
class S7commScoringProcessFunctionTest {

    private static final S7commConnectionKey KEY = new S7commConnectionKey(new SensorId("s"), "C1");

    // Identity continuous transforms and a one-score policy: the stub decides the score.
    static S7commDetectorBundle bundle() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("s" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        S7commPreprocessing p = new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"),
            List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            scores.put(g, new double[]{0.5});
        }
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64),
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, 16, 21, "input",
            "reconstruction", p, p.scoreWeights(List.of("s7_operation")),
            new S7commConformalPolicy(scores, Map.of(), 0.001));
    }

    // Serializable, as the operator requires; builds its stub on the
    // "TaskManager". The stub adds the OLDEST row's feature 0 to the last row's,
    // so each score depends on the whole window: a window restored wrongly
    // scores differently.
    static final class StubFactory implements ReconstructionScorerFactory {
        @Override
        public ReconstructionScorer create() {
            S7commDetectorBundle bundle = bundle();
            return new ReconstructionScorer() {
                @Override
                public S7commDetectorBundle bundle() {
                    return bundle;
                }

                @Override
                public float[] reconstructLast(float[][] window) {
                    float[] row = window[window.length - 1].clone();
                    row[0] += window[0][0];
                    return row;
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static OneInputStreamOperatorTestHarness<KeyedS7commVector, S7commDetectorPrediction> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new S7commScoringProcessFunction(new StubFactory(), Duration.ofHours(1))),
            new KeyedS7commVectorKeySelector(), TypeInformation.of(S7commConnectionKey.class));
    }

    private static OneInputStreamOperatorTestHarness<KeyedS7commVector, S7commDetectorPrediction> disabledHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(S7commScoringProcessFunction.disabled(Duration.ofHours(1))),
            new KeyedS7commVectorKeySelector(), TypeInformation.of(S7commConnectionKey.class));
    }

    // Event i: a response whose feature 0 is i / 100.
    private static KeyedS7commVector event(int i, boolean fresh) {
        float[] v = new float[16];
        v[0] = i / 100f;
        v[14] = 3f;
        v[15] = 4f;
        return new KeyedS7commVector(KEY, new FeatureVector("s:C1:" + i + ":RESPONSE:" + i,
            Instant.ofEpochSecond(1000 + i), new SensorId("s"), LogType.S7COMM, "C1",
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, v, 0,
            Instant.ofEpochSecond(1000 + i)), fresh, "10.0.0.5", "10.0.0.9");
    }

    // Amendment A1: the first 64 events are WARMUP, the 65th is the first scored.
    @Test
    void itWarmsUpThenScores() throws Exception {
        var harness = harness();
        harness.open();
        for (int i = 0; i < 66; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        List<S7commDetectorPrediction> out = harness.extractOutputValues();
        assertEquals(66, out.size());
        assertEquals(DetectorVerdict.WARMUP, out.get(63).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(64).verdict());
        assertEquals(0.49f * 0.49f / 17f, out.get(64).score(), 1e-6f, "the oldest row is event 49: 0.49^2 / 17");
        assertEquals(0.5f * 0.5f / 17f, out.get(65).score(), 1e-6f, "the oldest row is event 50: 0.5^2 / 17");
        harness.close();
    }

    @Test
    void aFreshConnectionRewarms() throws Exception {
        var harness = harness();
        harness.open();
        for (int i = 0; i < 65; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        harness.processElement(new StreamRecord<>(event(65, true)));
        S7commDetectorPrediction last = harness.extractOutputValues().get(65);
        assertEquals(DetectorVerdict.WARMUP, last.verdict());
        assertEquals(1, last.eventsSinceReset());
        harness.close();
    }

    @Test
    void anIdleWindowExpiresAfterTheTtl() throws Exception {
        var harness = harness();
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        for (int i = 0; i < 64; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        harness.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        harness.processElement(new StreamRecord<>(event(64, false)));
        S7commDetectorPrediction last = harness.extractOutputValues().get(64);
        assertEquals(DetectorVerdict.WARMUP, last.verdict(), "the 64 earlier rows expired");
        assertEquals(1, last.eventsSinceReset());
        harness.close();
    }

    @Test
    void aRestoreMidWindowGivesTheUninterruptedPredictions() throws Exception {
        var uninterrupted = harness();
        uninterrupted.open();
        for (int i = 0; i < 90; i++) {
            uninterrupted.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        List<S7commDetectorPrediction> expected = uninterrupted.extractOutputValues();
        uninterrupted.close();

        var first = harness();
        first.open();
        for (int i = 0; i < 70; i++) {
            first.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
        first.close();

        var second = harness();
        second.initializeState(snapshot);
        second.open();
        for (int i = 70; i < 90; i++) {
            second.processElement(new StreamRecord<>(event(i, false)));
        }
        List<S7commDetectorPrediction> actual = second.extractOutputValues();
        second.close();

        assertEquals(20, actual.size());
        for (int i = 0; i < actual.size(); i++) {
            S7commDetectorPrediction e = expected.get(70 + i);
            assertEquals(DetectorVerdict.NORMAL, e.verdict(), "past the warm-up: scored");
            assertEquals(e.verdict(), actual.get(i).verdict(), "event " + (70 + i));
            assertEquals(e.eventsSinceReset(), actual.get(i).eventsSinceReset(), "event " + (70 + i));
            assertEquals(e.score(), actual.get(i).score(), "event " + (70 + i));
        }
    }

    // Scoring off (an empty S7COMM_DETECTOR_BUNDLE): the operator stays in the
    // job, so no savepoint state is orphaned, but it emits nothing.
    @Test
    void aDisabledOperatorEmitsNothing() throws Exception {
        var harness = disabledHarness();
        harness.open();
        for (int i = 0; i < 20; i++) {
            harness.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        assertEquals(0, harness.extractOutputValues().size());
        harness.close();
    }

    // On -> off -> on through savepoints: each restore succeeds, and the window
    // a connection held before scoring was switched off is gone, so the
    // detector never reads vectors from both sides of the gap as consecutive.
    @Test
    void scoringSwitchedOffAndOnAgainRestoresAndRewarms() throws Exception {
        var on = harness();
        on.open();
        for (int i = 0; i < 12; i++) {
            on.processElement(new StreamRecord<>(event(i, i == 0)));
        }
        OperatorSubtaskState whileOn = on.snapshot(1L, 1L);
        on.close();

        var off = disabledHarness();
        off.initializeState(whileOn);
        off.open();
        for (int i = 12; i < 18; i++) {
            off.processElement(new StreamRecord<>(event(i, false)));
        }
        assertEquals(0, off.extractOutputValues().size());
        OperatorSubtaskState whileOff = off.snapshot(2L, 2L);
        off.close();

        var again = harness();
        again.initializeState(whileOff);
        again.open();
        for (int i = 18; i < 84; i++) {
            again.processElement(new StreamRecord<>(event(i, false)));
        }
        List<S7commDetectorPrediction> out = again.extractOutputValues();
        assertEquals(1, out.get(0).eventsSinceReset(), "the window held before scoring was off is gone");
        assertEquals(DetectorVerdict.WARMUP, out.get(63).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(64).verdict());
        again.close();
    }
}
