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
        // What a restore across this change actually does, which is NOT what the
        // same-name rename would have done. Keyed state is addressed by (operator
        // uid, state name). The uid is unchanged, but this name is new, so Flink
        // finds nothing under "rolling-counters" and starts the window empty --
        // a cold window for at most five minutes, which is the bounded cost.
        // The old "source-window-state" entry stays in the checkpoint, orphaned
        // and never read; unreferenced state WITHIN an operator that still exists
        // is not an error, unlike a missing operator, which is what
        // --allowNonRestoredState is actually for.
        //
        // Keeping the old name is the worse option, not the conservative one: the
        // name would then match while the Kryo snapshot still named the deleted
        // ConnWindowState class, so Flink would locate the state, resolve the
        // serializer incompatible, and fail the restore with
        // StateMigrationException -- the job would not start at all. Renaming the
        // state alongside the type is what turns a hard failure into a cold start.
        // Reasoned from documented restore semantics, not from an executed
        // savepoint test.
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
            // is a wiring error (the DNS chain is a separate pipeline per
            // docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
            // section 6.3 -- cited by its file path rather than a bare section
            // number, because this repo has three spec documents whose section
            // numbers collide -- never routed through ConnFeatureProcessFunction),
            // not a runtime condition to degrade gracefully from -- so it throws
            // rather than being silently skipped or defaulted.
            case DnsEvent ignored -> throw new IllegalStateException(
                "ConnFeatureProcessFunction received a DnsEvent; the DNS chain is separate by design "
                + "(docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md section 6.3) "
                + "and this is a wiring error, not a runtime condition");
        };

        FeatureBuildResult<RollingCounters> result = useCase.build(conn, currentState);
        windowState.update(result.newState());
        out.collect(result.vector());
    }
}
