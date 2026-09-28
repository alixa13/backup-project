package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

    // One hour of processing time with no write. Choose it above two bounds.
    // First, Zeek's TCP inactivity timeout (5 minutes by default): after it, Zeek
    // starts a new uid, so a connection with no traffic for the whole TTL
    // normally never sees another record -- except one Zeek keeps open with
    // keepalives but no S7 PDUs, whose next PDU then starts from empty state.
    // Second, the longest outage the job may suffer: the TTL counts PROCESSING
    // time and every key's last-write time is restored with the checkpoint, so
    // after downtime (or a stall) longer than the TTL every connection is expired
    // on its next record and restarts from empty state -- its next responses
    // count as unmatched -- silently, with no quality bit. Only during catch-up
    // after a replay, which compresses event time, does it fire late rather than
    // early. See docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 7.
    public static final Duration DEFAULT_STATE_TTL = Duration.ofHours(1);

    // What s7comm-score reads (scoring design section 4): each vector with its
    // connection key, whether the connection started from empty state, and
    // its endpoints. An anonymous subclass so Flink keeps the element type.
    public static final OutputTag<KeyedS7commVector> SCORING_TAG = new OutputTag<>("s7comm-scoring") {};

    // Events after which a connection's state starts again (scoring spec
    // amendment A1, item 4): s7_same_function_run_length has no upper bound, and
    // detector v2 flags every event once it passes about 73,000. 16,384 keeps it far
    // below that; measured on v2, every gated source stays at 99.46-100% NORMAL past
    // each segment's 64th event, with 0.4% of events inside a segment's warm-up.
    public static final long RESTART_EVENTS = 16_384L;

    private final Duration stateTtl;
    private final long restartEvents;

    private transient ValueState<S7commConnectionState> connectionState;
    private transient ValueState<Long> eventsSinceRestart;
    private transient BuildFeaturesUseCase<S7commEvent, S7commConnectionState> useCase;

    public S7commFeatureProcessFunction() {
        this(DEFAULT_STATE_TTL);
    }

    public S7commFeatureProcessFunction(Duration stateTtl) {
        this(stateTtl, RESTART_EVENTS);
    }

    public S7commFeatureProcessFunction(Duration stateTtl, long restartEvents) {
        Objects.requireNonNull(stateTtl, "stateTtl must not be null");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        if (restartEvents < 1) {
            throw new IllegalArgumentException("restartEvents must be at least 1, was " + restartEvents);
        }
        this.stateTtl = stateTtl;
        this.restartEvents = restartEvents;
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
        // "s7comm-events-since-restart" (amendment A1, item 4): its own state, so the
        // deployed s7comm-connection-state keeps its layout and restores unchanged.
        ValueStateDescriptor<Long> counter = new ValueStateDescriptor<>("s7comm-events-since-restart", Types.LONG);
        counter.enableTimeToLive(ttl);
        eventsSinceRestart = getRuntimeContext().getState(counter);
        useCase = new S7commBuildFeaturesUseCase();
    }

    @Override
    public void processElement(S7commEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        // Read through value() on every call, never cached: on the heap backend
        // that is what makes in-place mutation safe while a checkpoint runs.
        S7commConnectionState state = connectionState.value();
        Long counted = eventsSinceRestart.value();
        // A connection that has run restartEvents events starts again from empty
        // state: its run lengths are unbounded, and the detector flags a connection
        // that runs too long (amendment A1, item 4). A key restored without a
        // counter counts from here.
        boolean restart = state != null && counted != null && counted >= restartEvents;
        // Empty state -- a new connection, a TTL expiry, a restore without state,
        // or a restart -- is where the scorer's window must start again (spec section 5).
        boolean freshState = state == null || restart;
        if (freshState) {
            state = S7commConnectionState.empty();
        }

        // build() advances the state in place and returns it; update() is still
        // required -- it stores a new key's state, keeps RocksDB/ForSt correct,
        // and is the write that refreshes the TTL.
        FeatureBuildResult<S7commConnectionState> result = useCase.build(event, state);
        connectionState.update(result.newState());
        eventsSinceRestart.update(freshState || counted == null ? 1L : counted + 1);
        FeatureVector vector = restart ? withFlag(result.vector(), QualityFlags.S7COMM_RESTARTED) : result.vector();
        out.collect(vector);

        // The client sends to port 102, as the feature engine's direction rule
        // says; a response's endpoints are the other way round.
        String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
        String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
        ctx.output(SCORING_TAG, new KeyedS7commVector(ctx.getCurrentKey(), vector, freshState, client, server));
    }

    // The same vector with one more quality flag; every other component as it is.
    private static FeatureVector withFlag(FeatureVector v, int flag) {
        return new FeatureVector(v.eventId(), v.eventTime(), v.sensor(), v.logType(), v.connectionUid(),
            v.schemaId(), v.schemaHash(), v.values(), v.qualityFlags() | flag, v.producedAt());
    }
}
