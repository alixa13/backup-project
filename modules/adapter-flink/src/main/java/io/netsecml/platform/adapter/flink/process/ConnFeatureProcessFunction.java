package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ConnBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.RollingCounters;
import io.netsecml.platform.domain.feature.SourceKey;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class ConnFeatureProcessFunction extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector> {
    private transient ValueState<RollingCounters> windowState;
    private transient BuildFeaturesUseCase<ConnEvent, RollingCounters> useCase;

    @Override
    public void open(OpenContext openContext) {
        // The state name changes to match the type this time, unlike the
        // ConnWindowState rename that kept "source-window-state". That rename
        // gained nothing by breaking the name, so it kept it. This extraction
        // breaks checkpoint compatibility anyway -- Kryo embeds the class name,
        // and RollingCounters is a different class -- and it is being taken while
        // the break is free: main cannot run this job, the branch chain is
        // unmerged, and production is air-gapped and not deployed, so no
        // checkpoint plausibly exists. Paying for a correct name now is cheaper
        // than carrying a wrong one past the first deployment.
        //
        // A restore across this change does NOT run cold -- it fails. The uid and
        // the old state name would both still match, so Flink locates the state,
        // finds a snapshot naming a class that no longer exists, and fails with
        // StateMigrationException. --allowNonRestoredState does not cover a
        // serializer incompatibility on matching state. Recovery is a fresh start,
        // which replays from OffsetsInitializer.earliest(). Reasoned from
        // documented restore semantics, not from an executed savepoint test.
        ValueStateDescriptor<RollingCounters> descriptor = new ValueStateDescriptor<>(
            "rolling-counters", TypeInformation.of(RollingCounters.class));
        windowState = getRuntimeContext().getState(descriptor);
        useCase = new ConnBuildFeaturesUseCase();
    }

    @Override
    public void processElement(NetworkEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        RollingCounters currentState = windowState.value();
        if (currentState == null) {
            currentState = RollingCounters.empty();
        }

        // The stream is DataStream<NetworkEvent> and KeyedProcessFunction's input
        // type follows it, so this cannot take ConnEvent directly the way the use
        // case does -- same constraint as SourceKeySelector. It narrows here
        // instead, and the compiler will flag this site when a second log type
        // joins the hierarchy and this function has to decide what it means.
        //
        // Same rule as SourceKeySelector's switch: resolve a compile break here
        // with an explicit case DnsEvent -> ... arm, NEVER with a `default ->`
        // catch-all -- a default arm silently gives up the exhaustiveness check
        // for every protocol after the next one, not just this one.
        ConnEvent conn = switch (event) {
            case ConnEvent c -> c;
            // Resolving this compile break with an explicit arm rather than a
            // default, per the comment above: a DnsEvent reaching this function
            // is a wiring error (the DNS chain is a separate pipeline per spec
            // section 6.3, never routed through ConnFeatureProcessFunction), not
            // a runtime condition to degrade gracefully from -- so it throws
            // rather than being silently skipped or defaulted.
            case DnsEvent ignored -> throw new IllegalStateException(
                "ConnFeatureProcessFunction received a DnsEvent; the DNS chain is separate by design "
                + "(spec section 6.3) and this is a wiring error, not a runtime condition");
        };

        FeatureBuildResult<RollingCounters> result = useCase.build(conn, currentState);
        windowState.update(result.newState());
        out.collect(result.vector());
    }
}
