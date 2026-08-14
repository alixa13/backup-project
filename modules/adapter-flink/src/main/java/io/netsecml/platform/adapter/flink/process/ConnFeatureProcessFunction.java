package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.BuildFeaturesUseCaseImpl;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceKey;
import io.netsecml.platform.domain.feature.SourceWindowState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class ConnFeatureProcessFunction extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector> {
    private transient ValueState<SourceWindowState> windowState;
    private transient BuildFeaturesUseCaseImpl useCase;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<SourceWindowState> descriptor = new ValueStateDescriptor<>(
            "source-window-state", TypeInformation.of(SourceWindowState.class));
        windowState = getRuntimeContext().getState(descriptor);
        useCase = new BuildFeaturesUseCaseImpl();
    }

    @Override
    public void processElement(NetworkEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        SourceWindowState currentState = windowState.value();
        if (currentState == null) {
            currentState = SourceWindowState.empty();
        }

        FeatureBuildResult result = useCase.build(event, currentState);
        windowState.update(result.newState());
        out.collect(result.vector());
    }
}
