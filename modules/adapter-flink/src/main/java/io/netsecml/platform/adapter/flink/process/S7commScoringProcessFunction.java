package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.S7commScoringResult;
import io.netsecml.platform.application.usecase.ScoreS7commSequenceUseCase;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.port.out.ReconstructionScorer;
import io.netsecml.platform.port.out.ReconstructionScorerFactory;
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

// s7comm-score (scoring design section 4): holds each connection's window of
// preprocessed vectors and emits one prediction per S7comm event. The scorer
// is created per subtask in open() and closed in close().
//
// disabled() is the same operator with scoring switched off (an empty
// S7COMM_DETECTOR_BUNDLE): it keeps its uid and its state descriptor in the
// job, so a savepoint taken while scoring ran restores without orphaned state,
// but it loads no model, emits nothing and clears each connection's window as
// its events pass -- so switching scoring back on re-warms every connection.
public final class S7commScoringProcessFunction
        extends KeyedProcessFunction<S7commConnectionKey, KeyedS7commVector, S7commDetectorPrediction> {

    // null only when disabled.
    private final ReconstructionScorerFactory factory;
    private final Duration stateTtl;

    private transient ValueState<S7commScoreWindow> windowState;
    private transient ReconstructionScorer scorer;
    private transient ScoreS7commSequenceUseCase useCase;
    private transient Counter unscorable;

    public S7commScoringProcessFunction(ReconstructionScorerFactory factory, Duration stateTtl) {
        this(factory, stateTtl, true);
    }

    // Scoring switched off: the operator and its state stay, nothing is scored.
    public static S7commScoringProcessFunction disabled(Duration stateTtl) {
        return new S7commScoringProcessFunction(null, stateTtl, false);
    }

    private S7commScoringProcessFunction(ReconstructionScorerFactory factory, Duration stateTtl, boolean enabled) {
        this.factory = enabled ? Objects.requireNonNull(factory, "factory") : null;
        Objects.requireNonNull(stateTtl, "stateTtl");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) {
        // "s7comm-score-window": a state name is checkpoint identity; never
        // rename it. The same idle TTL as the feature state (spec section 5).
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();
        ValueStateDescriptor<S7commScoreWindow> descriptor = new ValueStateDescriptor<>(
            "s7comm-score-window", TypeInformation.of(S7commScoreWindow.class));
        descriptor.enableTimeToLive(ttl);
        windowState = getRuntimeContext().getState(descriptor);
        // Disabled: no model, no use case -- the state above is all it keeps.
        if (factory == null) {
            return;
        }
        // The model is loaded here, once per subtask; a bad bundle fails the job.
        scorer = factory.create();
        useCase = new ScoreS7commSequenceUseCase(scorer, Clock.systemUTC());
        unscorable = getRuntimeContext().getMetricGroup().counter("unscorable");
    }

    @Override
    public void processElement(KeyedS7commVector in, Context ctx, Collector<S7commDetectorPrediction> out)
            throws Exception {
        // Disabled: drop this connection's window, emit nothing.
        if (factory == null) {
            windowState.clear();
            return;
        }
        // Read through value() on every call, never cached, as the feature state is.
        S7commScoringResult result = useCase.score(in.vector(), in.freshState(), in.clientIp(), in.serverIp(),
            windowState.value());
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
