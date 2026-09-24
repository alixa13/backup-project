package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;

// The conn.log enrichment left join
// (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2): a two-input keyed operator joining each DNS record (input 1)
// against the latest conn.log snapshot (input 2) observed for the same
// connection uid, re-emitting the record carrying the snapshot's delta.
//
// NEVER BLOCKS, NEVER BUFFERS. Spec section 6.2 is explicit that blocking on
// conn.log is a correctness failure, not merely a latency cost: a connection's
// first snapshot does not exist until it has been alive five minutes, so
// waiting for one would stall every record from every new connection and hold
// unbounded per-connection state. Both processElement1 and processElement2
// read or update keyed state and emit synchronously -- neither ever registers
// a timer to wait for the other input.
//
// POINT IN TIME. processElement1 refuses to enrich a DNS record with a
// snapshot observed AFTER that record's own event time. These vectors become
// training data; enriching with future information is label leakage -- the
// model would learn from a signal it can never have at inference time. See the
// check itself for the comment on the exact comparison.
//
// PROCESSING-ORDER DEPENDENT -- A KNOWN LIMIT, not merely the POINT-IN-TIME
// rule's flip side. Both of this job's Kafka sources use
// WatermarkStrategy.noWatermarks() (see OnlineFeatureJob.rawSource), and
// KeyedCoProcessFunction drains its two inputs in the order Flink happens to
// read them, not event-time order. POINT IN TIME above correctly refuses a
// snapshot from a record's future, but nothing guarantees a snapshot from its
// genuine past has already arrived by the time this runs -- so whether a given
// dns record ends up enriched can depend on read timing, not only on what Zeek
// actually wrote. Consequence worth stating plainly: reprocessing the identical
// Kafka data can produce a DIFFERENT vector for the same record, and these
// vectors are training data. Do NOT fix this by making the join blocking --
// the NEVER BLOCKS paragraph above is exactly why that would be worse.
//
// AN HONEST LIMIT. For DNS over UDP, Zeek writes a flow's conn.log line only
// after the flow's inactivity timeout -- which is AFTER Zeek has already
// written that flow's dns.log record. So for nearly all UDP DNS, no snapshot
// exists yet when this join runs for a given record, and
// QualityFlags.CONN_ENRICHMENT_ABSENT (set by DnsBuildFeaturesUseCase whenever
// DnsEvent.enrichment() is null) will be set on almost every DNS vector this
// operator ever produces. The join is still correct -- it is exactly the
// mechanism long-lived OT connections (Modbus, S7comm), which stay open long
// enough to accumulate snapshots, will actually benefit from.
public final class ConnSnapshotJoinFunction
        extends KeyedCoProcessFunction<String, NetworkEvent, ConnSnapshot, NetworkEvent> {

    private transient ValueState<ConnEnrichment> enrichmentState;

    @Override
    public void open(OpenContext openContext) {
        // "conn-enrichment": this codebase's FIRST TTL (grep StateTtlConfig /
        // enableTimeToLive across modules/ before this task -- nothing).
        // OnlineFeatureJob's own KNOWN GAP note (ConnFeatureProcessFunction's
        // rolling-window state has no TTL, so its key set -- every distinct
        // (sensor, sourceIp) ever seen -- grows unbounded) is a SEPARATE,
        // still-open issue this task does not fix; it concerns a different
        // operator's different state entirely.
        //
        // 30 minutes, OnCreateAndWrite, NeverReturnExpired. Sensors flush an
        // interim conn.log record every 5 minutes for an open connection, so
        // 30 minutes with no snapshot WRITE is six missed intervals -- past
        // any plausible flush delay -- and means the connection is gone.
        //
        // OnCreateAndWrite, NOT OnReadAndWrite: a busy DNS uid keeps reading
        // this state (processElement1 runs on every DNS record for the
        // connection) long after the connection itself may have closed.
        // OnReadAndWrite would let those reads alone keep a stale snapshot
        // "alive" in state forever; only a genuine new snapshot arriving on
        // input 2 -- a WRITE -- may reset the clock.
        //
        // A KNOWN LIMIT: this TTL is PROCESSING-time, not event-time, while the
        // 30-minute reasoning above is a wall-clock one. OnlineFeatureJob's
        // Kafka sources start at OffsetsInitializer.earliest(), so replaying a
        // topic's retention window from the start compresses hours of EVENT
        // time into minutes of PROCESSING time -- this TTL clock runs on the
        // latter, so it may never fire during a replay, and this operator's
        // uid-keyed state then grows with one entry per distinct connection uid
        // seen in the replayed history. This is a DIFFERENT, separate gap from
        // the rolling-window key set's own missing TTL (OnlineFeatureJob's
        // KNOWN GAP comment): that state has no TTL at all, while this one has
        // a TTL that simply does not fire under replay.
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofMinutes(30))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        ValueStateDescriptor<ConnEnrichment> descriptor = new ValueStateDescriptor<>(
            "conn-enrichment", TypeInformation.of(ConnEnrichment.class));
        descriptor.enableTimeToLive(ttlConfig);
        enrichmentState = getRuntimeContext().getState(descriptor);
    }

    // Input 1: a protocol record to (maybe) enrich. Emits exactly once, always
    // -- never buffered, never dropped.
    @Override
    public void processElement1(NetworkEvent event, Context ctx, Collector<NetworkEvent> out) throws Exception {
        // The stream is DataStream<NetworkEvent>, so this cannot take DnsEvent
        // directly. Resolved with an explicit case ConnEvent arm, NEVER a
        // `default` -- same rule as every other narrowing site in this
        // package (ConnFeatureProcessFunction, DnsFeatureProcessFunction,
        // SourceKeySelector, ConnSnapshotExtractFunction).
        DnsEvent dns = switch (event) {
            case DnsEvent d -> d;
            // A ConnEvent reaching input 1 is a wiring error: this operator's
            // first input is the DNS parse chain only, per how
            // OnlineFeatureJob's two-protocol build() wires this operator's
            // .connect(...).keyBy(...) call -- conn records reach this
            // operator solely through input 2, via ConnSnapshotExtractFunction
            // on the separate conn chain. So this throws rather than silently
            // skipping or defaulting.
            case ConnEvent ignored -> throw new IllegalStateException(
                "ConnSnapshotJoinFunction received a ConnEvent on input 1; this operator's first input is "
                + "the DNS chain only "
                + "(docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md section 6.2) "
                + "and this is a wiring error, not a runtime condition");
            // Same rule, for modbus: it has its own Flink operator, keyed
            // state, topics and DLQ (docs/superpowers/specs/2026-09-21-modbus-
            // stage1-design.md section 10), never routed through this
            // dns-only join's first input, so reaching here is a wiring error too.
            case ModbusEvent ignored -> throw new IllegalStateException(
                "ConnSnapshotJoinFunction received a ModbusEvent on input 1; the modbus chain is separate "
                + "by design (docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 10) "
                + "and this is a wiring error, not a runtime condition");
            case S7commEvent ignored -> throw new IllegalStateException(
                "ConnSnapshotJoinFunction received an S7commEvent on input 1; the s7comm chain is separate "
                + "by design (docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 10) "
                + "and this is a wiring error, not a runtime condition");
        };

        // A blank uid cannot be looked up meaningfully, and worse: every
        // blank-uid record would land on the SAME keyed partition (the empty
        // string is still a valid key), leaking one connection's enrichment
        // onto an unrelated record. Unreachable today -- DnsEventMapper
        // derives its uid the same non-blank-enforcing way EventMapper does
        // for conn -- but this operator must not assume an invariant that
        // only the mapper happens to enforce, so it degrades explicitly:
        // emit unchanged, state untouched, rather than look anything up.
        String uid = dns.connectionUid();
        if (uid == null || uid.isBlank()) {
            out.collect(dns);
            return;
        }

        ConnEnrichment current = enrichmentState.value();

        // POINT-IN-TIME RULE (see class javadoc). No snapshot held yet, or the
        // only one held was observed AFTER this record's own event time,
        // means using it would hand the model information from this record's
        // future -- label leakage. Emit unchanged; enrichment stays null, and
        // DnsBuildFeaturesUseCase already sets QualityFlags.CONN_ENRICHMENT_ABSENT
        // for exactly this null case.
        if (current == null || current.latest().observedAt().isAfter(dns.eventTime())) {
            out.collect(dns);
            return;
        }

        out.collect(dns.withEnrichment(current.delta()));
    }

    // Input 2: a conn.log snapshot arrives. Never emits anything itself --
    // this input only ever updates state for processElement1 to read later.
    @Override
    public void processElement2(ConnSnapshot snapshot, Context ctx, Collector<NetworkEvent> out) throws Exception {
        ConnEnrichment current = enrichmentState.value();

        // OUT-OF-ORDER / DUPLICATE GUARD. "Not after" (<=), not "before" (<):
        // a snapshot whose observedAt is EQUAL to the one already held -- an
        // exact duplicate resend, e.g. an upstream at-least-once retry -- must
        // be rejected the same as a genuinely older one. Accepting a duplicate
        // would call deltaFrom against itself and silently reset the delta to
        // zero, which reads as "traffic stopped" rather than "nothing new
        // arrived".
        if (current != null && !snapshot.observedAt().isAfter(current.latest().observedAt())) {
            return;
        }

        // The first snapshot ever seen for this uid has no predecessor to
        // diff against -- conn.log's counters are cumulative from connection
        // start, so its own raw counters already ARE the delta
        // (ConnSnapshot.asInitialDelta's own javadoc). Every later snapshot
        // that passes the guard above is diffed against the one it replaces.
        ConnSnapshotDelta delta = current == null
            ? snapshot.asInitialDelta()
            : snapshot.deltaFrom(current.latest());
        enrichmentState.update(new ConnEnrichment(snapshot, delta));
    }
}
