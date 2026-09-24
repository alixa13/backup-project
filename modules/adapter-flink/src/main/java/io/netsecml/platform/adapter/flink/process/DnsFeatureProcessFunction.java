package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.DnsBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.DnsWindowState;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceKey;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// DNS's half of the two per-log-type windowing operators; mirrors
// ConnFeatureProcessFunction exactly -- same shape, same reasoning -- because
// both are the same pattern (hold this key's bounded window in Flink state,
// narrow the event, delegate to the log type's BuildFeaturesUseCase) applied to
// a different log type, not two independently-invented ones.
public final class DnsFeatureProcessFunction extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector> {
    private transient ValueState<DnsWindowState> windowState;
    private transient BuildFeaturesUseCase<DnsEvent, DnsWindowState> useCase;

    @Override
    public void open(OpenContext openContext) {
        // Unlike ConnFeatureProcessFunction's "rolling-counters" -- itself a
        // rename away from "source-window-state" once RollingCounters was
        // extracted; see that class's open() for the full story -- this state
        // has no rename history to tell: DnsWindowState is the only type this
        // window has ever used. The name is still chosen deliberately and fixed
        // from day one, because a state name IS checkpoint identity (keyed state
        // is addressed by (operator uid, state name) together): changing it
        // after a real checkpoint exists would discard every key's window on
        // restore. ConnFeatureProcessFunction's rename could accept that cost
        // only because no checkpoint could plausibly exist yet; a rename made
        // to THIS name after this ships would have no such excuse.
        ValueStateDescriptor<DnsWindowState> descriptor = new ValueStateDescriptor<>(
            "dns-window-state", TypeInformation.of(DnsWindowState.class));
        windowState = getRuntimeContext().getState(descriptor);
        useCase = new DnsBuildFeaturesUseCase();
    }

    @Override
    public void processElement(NetworkEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        DnsWindowState currentState = windowState.value();
        if (currentState == null) {
            currentState = DnsWindowState.empty();
        }

        // The stream is DataStream<NetworkEvent> and KeyedProcessFunction's input
        // type follows it, so this cannot take DnsEvent directly the way the use
        // case does -- same constraint as ConnFeatureProcessFunction and
        // SourceKeySelector before it. It narrows here instead.
        //
        // Resolved with an explicit case ConnEvent arm, NEVER a `default ->`
        // catch-all -- the same rule ConnFeatureProcessFunction's own switch
        // states: a default arm here would silently disable the exhaustiveness
        // check for every log type still to come (HTTP, SSH, Modbus, S7comm),
        // not just excuse skipping ConnEvent this once.
        DnsEvent dns = switch (event) {
            case DnsEvent d -> d;
            // A ConnEvent reaching this function is a wiring error, not a
            // runtime condition to degrade from: the conn chain and the DNS
            // chain are separate pipelines by design
            // (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
            // section 6.3 -- cited by its file path rather than a bare section
            // number, because this repo has three spec documents whose section
            // numbers collide), never merged into one KeyedProcessFunction. So
            // this throws rather than being silently skipped or defaulted.
            case ConnEvent ignored -> throw new IllegalStateException(
                "DnsFeatureProcessFunction received a ConnEvent; the conn chain is separate by design "
                + "(docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md section 6.3) "
                + "and this is a wiring error, not a runtime condition");
            // Same rule, for modbus: it has its own Flink operator, keyed
            // state, topics and DLQ (docs/superpowers/specs/2026-09-21-modbus-
            // stage1-design.md section 10), never routed through this
            // dns-only operator, so reaching here is a wiring error too.
            case ModbusEvent ignored -> throw new IllegalStateException(
                "DnsFeatureProcessFunction received a ModbusEvent; the modbus chain is separate by design "
                + "(docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 10) "
                + "and this is a wiring error, not a runtime condition");
            case S7commEvent ignored -> throw new IllegalStateException(
                "DnsFeatureProcessFunction received an S7commEvent; the s7comm chain is separate by design "
                + "(docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 10) "
                + "and this is a wiring error, not a runtime condition");
        };

        FeatureBuildResult<DnsWindowState> result = useCase.build(dns, currentState);
        windowState.update(result.newState());
        out.collect(result.vector());
    }
}
