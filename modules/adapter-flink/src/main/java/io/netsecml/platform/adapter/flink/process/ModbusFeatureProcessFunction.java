package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// Modbus's own windowing operator; mirrors ConnFeatureProcessFunction and
// DnsFeatureProcessFunction's shape and reasoning -- hold this key's bounded
// state in Flink state, delegate to the log type's BuildFeaturesUseCase,
// store what comes back. Keyed and typed directly on ModbusEvent rather than
// NetworkEvent, unlike its two siblings: the modbus chain is a separate
// pipeline end to end (own topics, own DLQ, own key -- see
// ModbusEntityKeySelector's own comment), so there is no NetworkEvent to
// narrow here the way Dns/ConnFeatureProcessFunction's switches do.
public final class ModbusFeatureProcessFunction
        extends KeyedProcessFunction<ModbusEntityKey, ModbusEvent, FeatureVector> {
    private transient ValueState<ModbusEntityState> entityState;
    private transient BuildFeaturesUseCase<ModbusEvent, ModbusEntityState> useCase;

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
        ValueStateDescriptor<ModbusEntityState> descriptor = new ValueStateDescriptor<>(
            "modbus-entity-state", TypeInformation.of(ModbusEntityState.class));
        entityState = getRuntimeContext().getState(descriptor);
        useCase = new ModbusBuildFeaturesUseCase();
    }

    @Override
    public void processElement(ModbusEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        ModbusEntityState currentState = entityState.value();
        if (currentState == null) {
            currentState = ModbusEntityState.empty();
        }

        // The use case owns the Instant-to-epoch-seconds conversion
        // (ModbusFeatureExtractor.epochSeconds) and the whole decide segment
        // / reset / extract / advance sequence; this operator does nothing
        // with timestamps itself, matching Conn/DnsFeatureProcessFunction's
        // own division of labour between operator and use case.
        FeatureBuildResult<ModbusEntityState> result = useCase.build(event, currentState);
        entityState.update(result.newState());
        out.collect(result.vector());
    }
}
