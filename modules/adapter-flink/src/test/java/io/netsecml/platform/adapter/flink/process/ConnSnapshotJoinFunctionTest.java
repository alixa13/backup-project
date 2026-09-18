package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// ConnSnapshotJoinFunction is the conn.log enrichment left join
// (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2): a two-input keyed operator joining DNS records (input 1)
// against conn.log snapshots (input 2) by connection uid. These tests pin the
// four semantics that matter -- non-blocking, point-in-time, out-of-order
// safety, and TTL -- each named in this task's brief as needing its own test.
class ConnSnapshotJoinFunctionTest {

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final Instant T0 = Instant.parse("2026-09-10T10:00:00Z");

    // A fresh, already-open harness around a fresh ConnSnapshotJoinFunction --
    // every test starts from empty state, mirroring how ConnFeatureProcessFunctionTest
    // and DnsFeatureProcessFunctionTest each build a new harness per test rather
    // than sharing one.
    private KeyedTwoInputStreamOperatorTestHarness<String, NetworkEvent, ConnSnapshot, NetworkEvent> harness()
            throws Exception {
        return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
            new ConnSnapshotJoinFunction(), new EventUidKeySelector(), new SnapshotUidKeySelector(),
            TypeInformation.of(String.class));
    }

    // Builds a minimal, valid dns.log event for a given uid and event time.
    // query/response are fixed because no test in this class cares about their
    // shape -- only uid and eventTime vary -- mirroring the reduction
    // DnsFeatureProcessFunctionTest's own dnsEvent() helper makes for the same
    // reason. enrichment is always null going in: it is what this operator's
    // output is being tested FOR.
    private NetworkEvent dnsEvent(String uid, Instant eventTime) {
        DnsQuery query = new DnsQuery("example.com", DnsQType.A, 42, DnsQType.A.code());
        DnsResponse response = new DnsResponse(DnsRcode.NOERROR, false, true, false, 1, 300L, DnsRcode.NOERROR.code());
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, uid + eventTime), eventTime, SENSOR,
            LogType.DNS, uid);
        return new DnsEvent(envelope, query, response, "10.0.0.5", true, null);
    }

    // A ConnEvent fixture, needed only for aConnEventOnInputOneIsAWiringError --
    // copied from ConnFeatureProcessFunctionTest's own event() helper, per this
    // codebase's established convention of duplicating small fixtures per test
    // class (DnsFeatureProcessFunctionTest does the same) rather than inventing
    // shared test utilities.
    private NetworkEvent connEvent(String uid, Instant eventTime) {
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, 100, 0, 1, 1, 0);
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, uid + eventTime), eventTime, SENSOR,
            LogType.CONN, uid);
        return new ConnEvent(envelope, tuple, measurements, new ConnectionLocality(null, null));
    }

    // Narrows the operator's NetworkEvent output down to DnsEvent so enrichment()
    // is readable -- explicit arms, no default, same rule production narrowing
    // sites in this package follow. A ConnEvent surfacing here would mean this
    // operator emitted the wrong type, which is a test bug worth a clear
    // AssertionError rather than a ClassCastException.
    private DnsEvent asDns(NetworkEvent event) {
        return switch (event) {
            case DnsEvent d -> d;
            case ConnEvent ignored -> throw new AssertionError("expected a DnsEvent output, got a ConnEvent");
        };
    }

    // Semantic 1: NEVER BLOCK, NEVER BUFFER. A DNS record with no snapshot for
    // its uid must still be emitted immediately, with enrichment left null --
    // not held back waiting for one that may never come.
    @Test
    void dnsRecordWithNoSnapshotYetIsEmittedImmediatelyWithNullEnrichment() throws Exception {
        var harness = harness();

        harness.processElement1(dnsEvent("Cabc", T0), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size(), "the record must be emitted immediately, never buffered");
        assertNull(asDns(output.get(0)).enrichment());

        harness.close();
    }

    // A connection's first-ever snapshot has no predecessor: its own raw
    // counters already ARE the delta (ConnSnapshot.asInitialDelta()).
    @Test
    void firstSnapshotEnrichesALaterDnsRecordWithItsInitialDelta() throws Exception {
        var harness = harness();
        ConnSnapshot snap = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        harness.processElement2(snap, 0L);
        harness.processElement1(dnsEvent("Cabc", T0.plusSeconds(310)), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals(snap.asInitialDelta(), asDns(output.get(0)).enrichment());

        harness.close();
    }

    // A second snapshot's delta is against the first, not its own raw counters
    // -- proves the join carries state forward across snapshots, not just the
    // single most recent one in isolation.
    @Test
    void secondSnapshotEnrichesWithTheDeltaAgainstTheFirst() throws Exception {
        var harness = harness();
        ConnSnapshot first = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        ConnSnapshot second = new ConnSnapshot("Cabc", T0, T0.plusSeconds(600), 1500L, 2600L, 14L, 27L);

        harness.processElement2(first, 0L);
        harness.processElement2(second, 0L);
        harness.processElement1(dnsEvent("Cabc", T0.plusSeconds(610)), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals(second.deltaFrom(first), asDns(output.get(0)).enrichment());

        harness.close();
    }

    // Keyed state isolation: a snapshot for one uid must never leak onto a
    // DIFFERENT uid's DNS record, even within the same harness / same run.
    @Test
    void snapshotForOneUidNeverEnrichesAnotherUidsDnsRecord() throws Exception {
        var harness = harness();
        ConnSnapshot snapForA = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        harness.processElement2(snapForA, 0L);
        harness.processElement1(dnsEvent("Cxyz", T0.plusSeconds(310)), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertNull(asDns(output.get(0)).enrichment(), "a different uid's keyed partition must be untouched");

        harness.close();
    }

    // The blank-uid branch (processElement1's own nine-line defensive comment)
    // had no test: the empty string is still a valid Flink key, so a harness
    // test for it is cheap, not hypothetical. A snapshot exists for a REAL uid
    // first, so a leak onto the wrong partition would be visible if it
    // happened -- the blank-uid record must come back with null enrichment
    // regardless.
    @Test
    void blankUidRecordIsEmittedUnchangedWithoutLookingUpState() throws Exception {
        var harness = harness();
        ConnSnapshot snap = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        harness.processElement2(snap, 0L);

        harness.processElement1(dnsEvent("", T0.plusSeconds(310)), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size(), "a blank-uid record must still be emitted, never dropped");
        assertNull(asDns(output.get(0)).enrichment(),
            "a blank uid cannot be looked up meaningfully, so this must emit unchanged rather than risk "
                + "leaking an unrelated connection's enrichment onto the empty-string partition");

        harness.close();
    }

    // Semantic 3a (out of order): a late-arriving OLDER snapshot must not
    // replace a newer one already held. The state after both processElement2
    // calls must still be `newer`, proven by the delta a later DNS record gets
    // being newer's OWN initial delta (newer was the first snapshot accepted),
    // not any delta derived from `older`.
    @Test
    void aLateArrivingOlderSnapshotDoesNotReplaceTheNewerOne() throws Exception {
        var harness = harness();
        ConnSnapshot newer = new ConnSnapshot("Cabc", T0, T0.plusSeconds(600), 1500L, 2600L, 14L, 27L);
        ConnSnapshot older = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        harness.processElement2(newer, 0L);
        harness.processElement2(older, 0L);
        harness.processElement1(dnsEvent("Cabc", T0.plusSeconds(610)), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals(newer.asInitialDelta(), asDns(output.get(0)).enrichment(),
            "the late older snapshot must be ignored, so held state is still `newer` as the first ever accepted");

        harness.close();
    }

    // Semantic 3b (duplicate): the exact same snapshot delivered twice -- e.g.
    // an upstream at-least-once retry -- has observedAt EQUAL to, not before,
    // the one already held. Accepting it would call deltaFrom against itself
    // and silently reset the delta to zero; the guard must reject it the same
    // way it rejects a genuinely older one.
    @Test
    void aDuplicateSnapshotResendDoesNotResetTheDeltaToZero() throws Exception {
        var harness = harness();
        ConnSnapshot snap = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        harness.processElement2(snap, 0L);
        harness.processElement2(snap, 0L);
        harness.processElement1(dnsEvent("Cabc", T0.plusSeconds(310)), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals(snap.asInitialDelta(), asDns(output.get(0)).enrichment(),
            "a duplicate resend must not recompute a zero delta against itself");

        harness.close();
    }

    // Semantic 2 (point in time): a snapshot observed AFTER the DNS record's
    // own event time must never enrich it, even though it is the only snapshot
    // held for this uid -- using it would be label leakage from the record's
    // future.
    @Test
    void aSnapshotObservedAfterTheDnsRecordsEventTimeIsNotUsed() throws Exception {
        var harness = harness();
        Instant dnsTime = T0.plusSeconds(100);
        ConnSnapshot future = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        harness.processElement2(future, 0L);
        harness.processElement1(dnsEvent("Cabc", dnsTime), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertNull(asDns(output.get(0)).enrichment(),
            "a snapshot observed after the record's own event time must never enrich it");

        harness.close();
    }

    // Semantic 4 (TTL): proves BOTH halves of OnCreateAndWrite in one test, as
    // the brief requires -- state is still alive at 20 minutes (not expired
    // too early), AND a READ at 20 minutes does not itself extend the TTL, so
    // state IS expired at 35 minutes (30 minutes after the WRITE at time 0,
    // with no intervening write). OnReadAndWrite would have kept it alive here
    // through the 20-minute read alone; this test is what tells them apart.
    @Test
    void ttlExpiresThirtyMinutesAfterTheWriteAndTheTwentyMinuteReadDoesNotExtendIt() throws Exception {
        var harness = harness();
        harness.setStateTtlProcessingTime(0L);

        ConnSnapshot snap = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        harness.processElement2(snap, 0L);

        // 20 minutes in: not yet expired, so this record IS enriched. This
        // read must NOT push the TTL clock forward from here.
        harness.setStateTtlProcessingTime(Duration.ofMinutes(20).toMillis());
        harness.processElement1(dnsEvent("Cabc", T0.plusSeconds(Duration.ofMinutes(20).toSeconds())), 0L);

        // 35 minutes in: 30 minutes have passed since the WRITE at time 0,
        // with only a READ at 20 minutes in between. Under OnCreateAndWrite
        // that read did not count, so state must now be expired.
        harness.setStateTtlProcessingTime(Duration.ofMinutes(35).toMillis());
        harness.processElement1(dnsEvent("Cabc", T0.plusSeconds(Duration.ofMinutes(35).toSeconds())), 0L);

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(2, output.size());
        assertEquals(snap.asInitialDelta(), asDns(output.get(0)).enrichment(),
            "20 minutes in, the snapshot must not yet have expired");
        assertNull(asDns(output.get(1)).enrichment(),
            "35 minutes after the write -- with no intervening write, only a read at 20 minutes -- the state "
                + "must have expired; OnReadAndWrite would have kept it alive here instead");

        harness.close();
    }

    // A ConnEvent reaching input 1 is a wiring error (this operator's first
    // input is the DNS chain only), not a runtime condition, so it must throw
    // rather than silently skip or default the record. As
    // DnsFeatureProcessFunctionTest's own equivalent test notes: Flink's
    // KeyedCoProcessOperator does not wrap what the user function throws, so
    // this asserts the concrete type, not just "some exception".
    @Test
    void aConnEventOnInputOneIsAWiringError() throws Exception {
        var harness = harness();
        NetworkEvent conn = connEvent("Cabc", T0);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
            () -> harness.processElement1(conn, 0L));
        assertTrue(thrown.getMessage().contains("ConnEvent"),
            "exception message should name the offending type: " + thrown.getMessage());

        harness.close();
    }

    // THE point of this task, per the brief: prove ConnEnrichment -- a record
    // holding a ConnSnapshot (a String connectionUid and two Instants) plus a
    // ConnSnapshotDelta -- actually survives Flink's serialize/restore path,
    // not just a Java-level build()-to-build() handoff. Nothing in this
    // repository has snapshotted a String or an Instant through Flink state
    // before this.
    //
    // harness A is built through ProcessFunctionTestHarnesses, which opens it
    // internally. harness B is built through the direct
    // KeyedTwoInputStreamOperatorTestHarness constructor around a freshly
    // constructed KeyedCoProcessOperator, because a restore must happen BEFORE
    // open() -- forKeyedCoProcessFunction's internal open() call would make
    // that impossible, mirroring DnsFeatureProcessFunctionTest's own restore
    // test for exactly the same reason.
    //
    // If this throws, that is the answer: report the exception verbatim, do
    // not work around it.
    @Test
    void enrichmentStateSurvivesASnapshotRestoreRoundTrip() throws Exception {
        var harnessA = harness();
        ConnSnapshot snap = new ConnSnapshot("Cabc", T0, T0.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        harnessA.processElement2(snap, 0L);

        OperatorSubtaskState snapshot = harnessA.snapshot(0L, 0L);
        harnessA.close();

        KeyedTwoInputStreamOperatorTestHarness<String, NetworkEvent, ConnSnapshot, NetworkEvent> harnessB =
            new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(new ConnSnapshotJoinFunction()),
                new EventUidKeySelector(), new SnapshotUidKeySelector(), TypeInformation.of(String.class));
        harnessB.initializeState(snapshot);
        harnessB.open();

        harnessB.processElement1(dnsEvent("Cabc", T0.plusSeconds(310)), 0L);

        List<NetworkEvent> output = harnessB.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals(snap.asInitialDelta(), asDns(output.get(0)).enrichment(),
            "the restored ConnEnrichment must carry the same delta the pre-restore state held");

        harnessB.close();
    }

    // Which serializer Flink picks for the join's keyed state is invisible at
    // runtime and decides whether a later field change can be restored at all.
    // TypeExtractor checks Modifier.isPublic BEFORE its record branch, so a
    // non-public record silently falls back to GenericTypeInfo -> Kryo, which
    // gives Flink no state schema evolution: adding a field to ConnEnrichment,
    // ConnSnapshot or ConnSnapshotDelta would then break restore from an older
    // savepoint. As a public record whose components are records of basic types,
    // it gets the POJO/record serializer, which can migrate. This test turns that
    // invisible choice into a failure the moment someone makes the record
    // non-public again or gives it a field Flink cannot analyse as a POJO.
    @Test
    void enrichmentStateUsesThePojoSerializerNotKryo() {
        TypeInformation<ConnEnrichment> typeInfo = TypeInformation.of(ConnEnrichment.class);

        assertInstanceOf(PojoTypeInfo.class, typeInfo,
            "ConnEnrichment must be analysed as a POJO/record type; got " + typeInfo.getClass().getSimpleName()
            + ", which serializes with Kryo and forfeits state schema evolution");
    }
}
