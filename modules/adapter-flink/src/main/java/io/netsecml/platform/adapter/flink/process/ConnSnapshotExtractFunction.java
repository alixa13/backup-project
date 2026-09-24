package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.feature.ConnSnapshots;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.Optional;

// The producer half of the conn.log enrichment left join
// (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2): sits on the conn parse/map/validate chain's NetworkEvent output
// and emits the ConnSnapshot stream ConnSnapshotJoinFunction's second input
// consumes.
//
// A FlatMapFunction rather than a MapFunction because ConnSnapshots.fromConnEvent
// returns Optional<ConnSnapshot> -- the empty case (a blank connectionUid,
// unreachable today but not forbidden by ConnEvent's own type) must emit NOTHING
// downstream, and flatMap is the shape that can emit zero elements without a
// null or sentinel value standing in for "nothing here".
public final class ConnSnapshotExtractFunction implements FlatMapFunction<NetworkEvent, ConnSnapshot> {
    @Override
    public void flatMap(NetworkEvent event, Collector<ConnSnapshot> out) {
        // The stream this reads is DataStream<NetworkEvent> (the shared
        // parse/map/validate output), so this cannot take ConnEvent directly.
        // Resolved with an explicit case DnsEvent arm, NEVER a `default` --
        // this project's rule (ConnFeatureProcessFunction, DnsFeatureProcessFunction,
        // SourceKeySelector): a default arm here would silently disable the
        // exhaustiveness check for every future log type, not just excuse
        // skipping DnsEvent this once.
        switch (event) {
            case ConnEvent c -> {
                Optional<ConnSnapshot> snapshot = ConnSnapshots.fromConnEvent(c);
                // isPresent()/get() rather than a lambda-based ifPresent: no
                // lambdas anywhere in this unit's adapter-flink classes (see
                // ConnSnapshotJoinFunction's javadoc for why).
                if (snapshot.isPresent()) {
                    out.collect(snapshot.get());
                }
            }
            // A DnsEvent reaching this function is a wiring error, not a
            // runtime condition: this extractor sits only on the conn.log
            // chain, per how OnlineFeatureJob's two-protocol build() wires
            // it -- flatMapping connParsed, never dnsParsed -- so this throws
            // rather than silently skipping or defaulting.
            case DnsEvent ignored -> throw new IllegalStateException(
                "ConnSnapshotExtractFunction received a DnsEvent; this operator sits only on the conn.log "
                + "chain (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md section "
                + "6.2) and this is a wiring error, not a runtime condition");
            // Same rule, for modbus: it has its own Flink operator, keyed
            // state, topics and DLQ (docs/superpowers/specs/2026-09-21-modbus-
            // stage1-design.md section 10), never routed through this
            // conn.log-only extractor, so reaching here is a wiring error too.
            case ModbusEvent ignored -> throw new IllegalStateException(
                "ConnSnapshotExtractFunction received a ModbusEvent; the modbus chain is separate by design "
                + "(docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 10) "
                + "and this is a wiring error, not a runtime condition");
        }
    }
}
