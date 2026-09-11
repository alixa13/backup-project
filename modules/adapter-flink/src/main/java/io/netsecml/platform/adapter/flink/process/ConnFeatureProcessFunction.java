package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.BuildFeaturesUseCaseImpl;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ConnWindowState;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class ConnFeatureProcessFunction extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector> {
    private transient ValueState<ConnWindowState> windowState;
    private transient BuildFeaturesUseCaseImpl useCase;

    @Override
    public void open(OpenContext openContext) {
        // The state name stays "source-window-state" even though the type is now
        // ConnWindowState. A state name is identity, the same as an operator uid,
        // and this project's established position (see Unit 1's uid decisions) is
        // that identity strings survive a naming-scheme change even when the type
        // that outgrew its old name does not. The type rename alone already
        // invalidates Kryo-serialized checkpoint state for this key (accepted: the
        // window is five one-minute buckets, so a restore is cold for at most five
        // minutes, not wrong). Renaming this string too would compound that
        // unavoidable break with an avoidable one, and would cost the ability to
        // recognize this state in tooling and metrics across the change. Do not
        // "tidy" this to match the type name.
        ValueStateDescriptor<ConnWindowState> descriptor = new ValueStateDescriptor<>(
            "source-window-state", TypeInformation.of(ConnWindowState.class));
        windowState = getRuntimeContext().getState(descriptor);
        useCase = new BuildFeaturesUseCaseImpl();
    }

    @Override
    public void processElement(NetworkEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        ConnWindowState currentState = windowState.value();
        if (currentState == null) {
            currentState = ConnWindowState.empty();
        }

        FeatureBuildResult result = useCase.build(event, currentState);
        windowState.update(result.newState());
        out.collect(result.vector());
    }
}
