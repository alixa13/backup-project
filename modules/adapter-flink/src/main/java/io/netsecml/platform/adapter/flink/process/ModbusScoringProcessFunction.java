package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ModbusScoringResult;
import io.netsecml.platform.application.usecase.ScoreModbusSequenceUseCase;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.port.out.SequenceScorer;
import io.netsecml.platform.port.out.SequenceScorerFactory;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

// modbus-score (spec section 4): holds each stream's window of preprocessed
// vectors and emits one prediction per Modbus event. The scorer is created per
// subtask in open() and closed in close().
public final class ModbusScoringProcessFunction
        extends KeyedProcessFunction<ModbusEntityKey, KeyedModbusVector, ModbusDetectorPrediction> {

    private final SequenceScorerFactory factory;
    private final Duration stateTtl;

    private transient ValueState<ModbusScoreWindow> windowState;
    private transient SequenceScorer scorer;
    private transient ScoreModbusSequenceUseCase useCase;
    private transient Counter unscorable;

    public ModbusScoringProcessFunction(SequenceScorerFactory factory, Duration stateTtl) {
        this.factory = Objects.requireNonNull(factory, "factory");
        Objects.requireNonNull(stateTtl, "stateTtl");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) {
        // "modbus-score-window": a state name is checkpoint identity; never
        // rename it. The same idle TTL as the feature state (spec section 5).
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();
        ValueStateDescriptor<ModbusScoreWindow> descriptor = new ValueStateDescriptor<>(
            "modbus-score-window", TypeInformation.of(ModbusScoreWindow.class));
        descriptor.enableTimeToLive(ttl);
        windowState = getRuntimeContext().getState(descriptor);
        // The model is loaded here, once per subtask; a bad bundle fails the job.
        scorer = factory.create();
        useCase = new ScoreModbusSequenceUseCase(scorer, Clock.systemUTC());
        unscorable = getRuntimeContext().getMetricGroup().counter("unscorable");
    }

    @Override
    public void processElement(KeyedModbusVector in, Context ctx, Collector<ModbusDetectorPrediction> out)
            throws Exception {
        // Read through value() on every call, never cached, as the feature state is.
        ModbusScoringResult result = useCase.score(ctx.getCurrentKey(), in.vector(), windowState.value());
        windowState.update(result.window());
        if (result.prediction().verdict() == DetectorVerdict.UNSCORABLE) {
            unscorable.inc();
        }
        out.collect(result.prediction());
    }

    @Override
    public void close() throws Exception {
        if (scorer != null) {
            scorer.close();
        }
        super.close();
    }
}
