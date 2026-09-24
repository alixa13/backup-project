package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Objects;

// s7comm's feature operator: holds one S7commConnectionState per
// (sensor, uid) in Flink state, delegates each event to
// S7commBuildFeaturesUseCase, stores the advanced state and emits the vector
// -- ModbusFeatureProcessFunction's shape, typed on S7commEvent because the
// chain narrows once at its boundary. Unlike every other feature operator in
// this job, its state carries a TTL from the start: it is keyed per
// connection, so without one the key set would grow with every S7 connection
// ever seen.
public final class S7commFeatureProcessFunction
        extends KeyedProcessFunction<S7commConnectionKey, S7commEvent, FeatureVector> {

    // One hour of processing time with no write. Parity-safe: Zeek starts a new
    // uid once a connection has been idle for its TCP inactivity timeout (5
    // minutes by default), so state idle for an hour can never receive another
    // record; and a replay compresses event time, so this processing-time TTL
    // can only fire late, never early. See
    // docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 7.
    public static final Duration DEFAULT_STATE_TTL = Duration.ofHours(1);

    private final Duration stateTtl;

    private transient ValueState<S7commConnectionState> connectionState;
    private transient BuildFeaturesUseCase<S7commEvent, S7commConnectionState> useCase;

    public S7commFeatureProcessFunction() {
        this(DEFAULT_STATE_TTL);
    }

    public S7commFeatureProcessFunction(Duration stateTtl) {
        Objects.requireNonNull(stateTtl, "stateTtl must not be null");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) {
        // OnCreateAndWrite: every event writes the state (update() below), so
        // each event restarts the clock and only a connection idle for the
        // whole TTL expires. NeverReturnExpired: an expired state reads as
        // absent, so the next record starts a fresh connection. Cleanup runs
        // incrementally (the heap backend's default) and on full snapshots.
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();

        // "s7comm-connection-state": a state name is checkpoint identity, so it
        // must never be renamed.
        ValueStateDescriptor<S7commConnectionState> descriptor = new ValueStateDescriptor<>(
            "s7comm-connection-state", TypeInformation.of(S7commConnectionState.class));
        descriptor.enableTimeToLive(ttl);
        connectionState = getRuntimeContext().getState(descriptor);
        useCase = new S7commBuildFeaturesUseCase();
    }

    @Override
    public void processElement(S7commEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        // Read through value() on every call, never cached: on the heap backend
        // that is what makes in-place mutation safe while a checkpoint runs.
        S7commConnectionState state = connectionState.value();
        if (state == null) {
            state = S7commConnectionState.empty();
        }

        // build() advances the state in place and returns it; update() is still
        // required -- it stores a new key's state, keeps RocksDB/ForSt correct,
        // and is the write that refreshes the TTL.
        FeatureBuildResult<S7commConnectionState> result = useCase.build(event, state);
        connectionState.update(result.newState());
        out.collect(result.vector());
    }
}
