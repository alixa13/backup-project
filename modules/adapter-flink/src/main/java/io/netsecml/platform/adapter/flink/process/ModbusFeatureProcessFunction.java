package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusEntityState;
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

// Modbus's own windowing operator; mirrors ConnFeatureProcessFunction and
// DnsFeatureProcessFunction's shape and reasoning -- hold this key's bounded
// state in Flink state, delegate to the log type's BuildFeaturesUseCase,
// store what comes back. Keyed and typed directly on ModbusEvent rather than
// NetworkEvent, unlike its two siblings: the modbus chain is a separate
// pipeline end to end (own topics, own DLQ, own key -- see
// ModbusEntityKeySelector's own comment), so there is no NetworkEvent to
// narrow here the way Dns/ConnFeatureProcessFunction's switches do. Its
// state carries an idle TTL, as S7commFeatureProcessFunction's does: without
// one the key set -- (sensor, client, server, unit) -- kept every key ever
// seen, forever (added 2026-09-25, on the deployed stack).
public final class ModbusFeatureProcessFunction
        extends KeyedProcessFunction<ModbusEntityKey, ModbusEvent, FeatureVector> {

    // One hour of processing time with no write. The TTL changes no feature
    // for live traffic: an event more than 15 s after its key's previous one
    // already starts a new segment and resets the whole state
    // (ModbusEntityState.startsNewSegment and reset()), and an expired state
    // reads as absent, which starts the same fresh segment. Two cases differ.
    // First, the TTL counts PROCESSING time and every key's last-write time is
    // restored with the checkpoint, so after an outage or stall longer than the
    // TTL a key restarts from empty state even when its event time has no gap:
    // requests pending across the outage are forgotten and their responses
    // count as unmatched, silently, with no quality bit -- S7comm's ruling R4
    // accepts the same. Second, the first event after an expiry is never
    // flagged MODBUS_OUT_OF_ORDER, having no earlier timestamp to compare
    // with. Choose the TTL above the longest expected outage
    // (MODBUS_STATE_TTL_MINUTES).
    public static final Duration DEFAULT_STATE_TTL = Duration.ofHours(1);

    private final Duration stateTtl;

    private transient ValueState<ModbusEntityState> entityState;
    private transient BuildFeaturesUseCase<ModbusEvent, ModbusEntityState> useCase;

    public ModbusFeatureProcessFunction() {
        this(DEFAULT_STATE_TTL);
    }

    public ModbusFeatureProcessFunction(Duration stateTtl) {
        Objects.requireNonNull(stateTtl, "stateTtl must not be null");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) {
        // Named "modbus-entity-state" and nothing else: a state name IS
        // checkpoint identity (keyed state is addressed by (operator uid,
        // state name) together), so a later rename would silently orphan
        // every key's restored state on the next restore -- the job would
        // start, find nothing under the new name, and every key would begin
        // from empty with no error anywhere. Its siblings are
        // "rolling-counters" (ConnFeatureProcessFunction) and
        // "dns-window-state" (DnsFeatureProcessFunction).
        //
        // OnCreateAndWrite: every event writes the state (update() below), so
        // each event restarts the clock and only a key idle for the whole TTL
        // expires. NeverReturnExpired: an expired state reads as absent.
        // Cleanup runs incrementally (the heap backend's default) and on full
        // snapshots, so an idle key leaves memory and checkpoints without
        // another event of its own. Enabling the TTL kept the state's name:
        // Flink 2.2.1 restores a savepoint written without TTL into it
        // (ModbusFeatureProcessFunctionTest pins that).
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();
        ValueStateDescriptor<ModbusEntityState> descriptor = new ValueStateDescriptor<>(
            "modbus-entity-state", TypeInformation.of(ModbusEntityState.class));
        descriptor.enableTimeToLive(ttl);
        entityState = getRuntimeContext().getState(descriptor);
        useCase = new ModbusBuildFeaturesUseCase();
    }

    @Override
    public void processElement(ModbusEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        ModbusEntityState currentState = entityState.value();
        if (currentState == null) {
            currentState = ModbusEntityState.empty();
        }

        // The use case owns reading event.tsSeconds() (the causal engine's
        // clock; see ModbusEvent's own javadoc) and the whole decide segment
        // / reset / extract / advance sequence; this operator does nothing
        // with timestamps itself, matching Conn/DnsFeatureProcessFunction's
        // own division of labour between operator and use case.
        FeatureBuildResult<ModbusEntityState> result = useCase.build(event, currentState);

        // build() mutates currentState in place and returns it as newState,
        // and this update() is still required: it is what stores a fresh key's
        // new state at all, and what keeps this operator correct on a state
        // backend that hands out a deserialized copy from value() (RocksDB/
        // ForSt), where the mutation alone would be lost. On the default heap
        // backend, mutating the object value() returned is safe while a
        // checkpoint runs: CopyOnWriteStateMap.get hands out a serializer copy
        // whenever a running snapshot still holds the stored object, so the
        // snapshot never sees a half-mutated state. That guarantee holds only
        // because the state is read through value() on every call, here --
        // never cache it in a field across calls.
        entityState.update(result.newState());
        out.collect(result.vector());
    }
}
