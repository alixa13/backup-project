package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;
import io.netsecml.platform.port.out.SequenceScorerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The scoring operator in a Flink harness, with a stub scorer whose dense
// score is the last row's feature 0: warm-up, the idle TTL, and a restore
// mid-window giving exactly an uninterrupted run's predictions.
class ModbusScoringProcessFunctionTest {

    private static final ModbusEntityKey KEY = new ModbusEntityKey(new SensorId("s"), "10.0.0.5", "10.0.0.9", "1");

    // Serializable, as the operator requires; builds its stub on the "TaskManager".
    static final class StubFactory implements SequenceScorerFactory {
        @Override
        public SequenceScorer create() {
            SequenceDetectorBundle bundle = new SequenceDetectorBundle("modbus-stage1-detector", "v1",
                "b".repeat(64), ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, 20, 42,
                "sequence_20x42", "d", "t", 0.5, 0.5, new ModbusPreprocessing(IntStream.range(0, 42)
                    .mapToObj(i -> new FeaturePreprocessing("f" + i, PreprocessingPolicy.PASSTHROUGH_BINARY, -1,
                        Double.NaN, Double.NaN)).toList()));
            return new SequenceScorer() {
                @Override
                public SequenceDetectorBundle bundle() {
                    return bundle;
                }

                @Override
                public DetectorScores score(float[][] sequence) {
                    return new DetectorScores(sequence[sequence.length - 1][0], 0.0);
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static OneInputStreamOperatorTestHarness<KeyedModbusVector, ModbusDetectorPrediction> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ModbusScoringProcessFunction(new StubFactory(), Duration.ofHours(1))),
            new KeyedModbusVectorKeySelector(), TypeInformation.of(ModbusEntityKey.class));
    }

    // Event i: feature 0 is i / 100 (so later events score higher), segment start only at 0.
    private static KeyedModbusVector event(int i) {
        float[] values = new float[42];
        values[0] = i / 100f;
        values[23] = i == 0 ? 0f : 1f;
        return new KeyedModbusVector(KEY, new FeatureVector("s:u:" + i + ":REQUEST:" + i,
            Instant.ofEpochSecond(1000 + i), new SensorId("s"), LogType.MODBUS, "u",
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, values, 0,
            Instant.ofEpochSecond(1000 + i)));
    }

    @Test
    void itWarmsUpThenScores() throws Exception {
        var harness = harness();
        harness.open();
        for (int i = 0; i < 21; i++) {
            harness.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> out = harness.extractOutputValues();
        assertEquals(21, out.size());
        assertEquals(DetectorVerdict.WARMUP, out.get(18).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(19).verdict(), "0.19 is not above 0.5");
        assertEquals(0.20f, out.get(20).denseScore(), 1e-6f);
        harness.close();
    }

    @Test
    void anIdleWindowExpiresAfterTheTtl() throws Exception {
        var harness = harness();
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        for (int i = 0; i < 19; i++) {
            harness.processElement(new StreamRecord<>(event(i)));
        }
        harness.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        harness.processElement(new StreamRecord<>(event(19)));
        ModbusDetectorPrediction last = harness.extractOutputValues().get(19);
        assertEquals(DetectorVerdict.WARMUP, last.verdict(), "the 19 earlier rows expired");
        assertEquals(1, last.windowEvents());
        harness.close();
    }

    @Test
    void aRestoreMidWindowGivesTheUninterruptedPredictions() throws Exception {
        var uninterrupted = harness();
        uninterrupted.open();
        for (int i = 0; i < 25; i++) {
            uninterrupted.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> expected = uninterrupted.extractOutputValues();
        uninterrupted.close();

        var first = harness();
        first.open();
        for (int i = 0; i < 12; i++) {
            first.processElement(new StreamRecord<>(event(i)));
        }
        OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
        first.close();

        var second = harness();
        second.initializeState(snapshot);
        second.open();
        for (int i = 12; i < 25; i++) {
            second.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> actual = second.extractOutputValues();
        second.close();

        for (int i = 0; i < actual.size(); i++) {
            ModbusDetectorPrediction e = expected.get(12 + i);
            assertEquals(e.verdict(), actual.get(i).verdict(), "event " + (12 + i));
            assertEquals(e.windowEvents(), actual.get(i).windowEvents(), "event " + (12 + i));
            assertEquals(e.denseScore(), actual.get(i).denseScore(), "event " + (12 + i));
        }
    }

    private static OneInputStreamOperatorTestHarness<KeyedModbusVector, ModbusDetectorPrediction> disabledHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(ModbusScoringProcessFunction.disabled(Duration.ofHours(1))),
            new KeyedModbusVectorKeySelector(), TypeInformation.of(ModbusEntityKey.class));
    }

    // Scoring off (an empty MODBUS_DETECTOR_BUNDLE): the operator stays in the
    // job, so no savepoint state is orphaned, but it emits nothing.
    @Test
    void aDisabledOperatorEmitsNothing() throws Exception {
        var harness = disabledHarness();
        harness.open();
        for (int i = 0; i < 25; i++) {
            harness.processElement(new StreamRecord<>(event(i)));
        }
        assertEquals(0, harness.extractOutputValues().size());
        harness.close();
    }

    // On -> off -> on through savepoints: each restore succeeds, and the
    // window a stream held before scoring was switched off is gone, so the
    // detector never reads vectors from either side of the gap as consecutive.
    @Test
    void scoringSwitchedOffAndOnAgainRestoresAndRewarms() throws Exception {
        var on = harness();
        on.open();
        for (int i = 0; i < 12; i++) {
            on.processElement(new StreamRecord<>(event(i)));
        }
        OperatorSubtaskState whileOn = on.snapshot(1L, 1L);
        on.close();

        var off = disabledHarness();
        off.initializeState(whileOn);
        off.open();
        for (int i = 12; i < 18; i++) {
            off.processElement(new StreamRecord<>(event(i)));
        }
        assertEquals(0, off.extractOutputValues().size());
        OperatorSubtaskState whileOff = off.snapshot(2L, 2L);
        off.close();

        var again = harness();
        again.initializeState(whileOff);
        again.open();
        for (int i = 18; i < 38; i++) {
            again.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> out = again.extractOutputValues();
        assertEquals(1, out.get(0).windowEvents(), "the window held before scoring was off is gone");
        assertEquals(DetectorVerdict.WARMUP, out.get(18).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(19).verdict());
        again.close();
    }
}
