package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DnsFeatureProcessFunctionTest {

    // Builds a minimal, valid dns.log event for one (sensor, sourceIp) key at a
    // given time. query/response are fixed because no test in this class cares
    // about their shape -- only sourceIp and eventTime vary, per key and per
    // interval, mirroring the reduction DnsBuildFeaturesUseCaseTest's own
    // dnsEvent() helper makes for the same reason.
    private NetworkEvent dnsEvent(SensorId sensor, String sourceIp, Instant eventTime) {
        DnsQuery query = new DnsQuery("example.com", DnsQType.A, 42);
        DnsResponse response = new DnsResponse(DnsRcode.NOERROR, false, true, false, 1, 300L);
        // LogType.DNS and a non-blank uid are required positional components on
        // the envelope; this harness test only exercises per-key windowing, so a
        // fixed composite string used to derive eventId is enough -- the same
        // approach ConnFeatureProcessFunctionTest's event() helper takes.
        String uid = eventTime.toString() + sourceIp;
        EventEnvelope envelope = new EventEnvelope(EventId.derive(sensor, uid), eventTime, sensor, LogType.DNS, uid);
        return new DnsEvent(envelope, query, response, sourceIp, true, null);
    }

    // A ConnEvent fixture, needed only for aConnEventIsAWiringError below --
    // copied from ConnFeatureProcessFunctionTest's own event() helper so both
    // process-function test classes build the log type they don't own
    // identically, rather than inventing a second way to do it.
    private NetworkEvent connEvent(SensorId sensor, String sourceIp, Instant eventTime) {
        ConnectionTuple tuple = new ConnectionTuple(sourceIp, 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, 100, 0, 1, 1, 0);
        String uid = eventTime.toString() + sourceIp;
        EventEnvelope envelope = new EventEnvelope(EventId.derive(sensor, uid), eventTime, sensor, LogType.CONN, uid);
        return new ConnEvent(envelope, tuple, measurements, new ConnectionLocality(null, null));
    }

    // Mirrors ConnFeatureProcessFunctionTest.stateAccumulatesPerKeyAcrossEvents:
    // two events for one source IP must accumulate record_count_5m (index 0 in
    // dns-feature-v1, unlike conn-feature-v1's 17 -- the common tier sits at the
    // FRONT of dns-feature-v1, since conn-feature-v1 predates the shared tier and
    // is frozen without it), and a different source IP must start its own window
    // at 1, proving the two keys do not share state.
    @Test
    void stateAccumulatesPerKeyAcrossEvents() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new DnsFeatureProcessFunction(), new SourceKeySelector(), TypeInformation.of(SourceKey.class));

        SensorId sensor = new SensorId("sensor-eu-1");
        harness.processElement(new StreamRecord<>(dnsEvent(sensor, "10.0.0.5", Instant.ofEpochSecond(60_000))));
        harness.processElement(new StreamRecord<>(dnsEvent(sensor, "10.0.0.5", Instant.ofEpochSecond(60_010))));
        harness.processElement(new StreamRecord<>(dnsEvent(sensor, "10.0.0.9", Instant.ofEpochSecond(60_010))));

        List<FeatureVector> output = harness.extractOutputValues();
        assertEquals(3, output.size());
        assertEquals(1f, output.get(0).values()[0], "first event for 10.0.0.5");
        assertEquals(2f, output.get(1).values()[0], "second event for 10.0.0.5, same key");
        assertEquals(1f, output.get(2).values()[0], "first event for a different source IP, independent state");

        harness.close();
    }

    // The output must actually be a dns-feature-v1 vector, not merely "a
    // FeatureVector of some kind" -- width, schemaId and logType are the three
    // cheap, distinguishing checks that a wrong schema or a mislabeled logType
    // would fail immediately.
    @Test
    void outputIsATwentyFourValueDnsVectorTaggedWithTheDnsSchemaAndLogType() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new DnsFeatureProcessFunction(), new SourceKeySelector(), TypeInformation.of(SourceKey.class));

        SensorId sensor = new SensorId("sensor-eu-1");
        harness.processElement(new StreamRecord<>(dnsEvent(sensor, "10.0.0.5", Instant.ofEpochSecond(60_000))));

        List<FeatureVector> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        FeatureVector vector = output.get(0);
        assertEquals(24, vector.values().length, "dns-feature-v1 is 24 values wide");
        assertEquals("dns-feature-v1", vector.schemaId());
        assertEquals(LogType.DNS, vector.logType());

        harness.close();
    }

    // A ConnEvent reaching this function is a wiring error (the conn chain and
    // the DNS chain are separate pipelines by design), not a runtime condition,
    // so it must throw rather than silently skip or default the record. Flink's
    // KeyedProcessOperator does not wrap what the user function throws -- verified
    // by this test actually catching IllegalStateException directly, not a
    // wrapper -- so this asserts the concrete type, not just "some exception".
    @Test
    void aConnEventIsAWiringError() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new DnsFeatureProcessFunction(), new SourceKeySelector(), TypeInformation.of(SourceKey.class));

        SensorId sensor = new SensorId("sensor-eu-1");
        NetworkEvent conn = connEvent(sensor, "10.0.0.5", Instant.ofEpochSecond(60_000));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
            () -> harness.processElement(new StreamRecord<>(conn)));
        assertTrue(thrown.getMessage().contains("ConnEvent"),
            "exception message should name the offending type: " + thrown.getMessage());

        harness.close();
    }

    // THE point of this task: prove DnsWindowState -- a record holding two final
    // classes with private constructors, one of which (RecordTimingState) carries
    // a possibly-null Instant -- actually survives Flink's serialize/restore path,
    // not just a Java-level build()-to-build() handoff. Nothing in this repository
    // has snapshotted this state before.
    //
    // harness A is built through ProcessFunctionTestHarnesses, which opens it
    // internally. harness B is built through the direct
    // KeyedOneInputStreamOperatorTestHarness constructor instead, because a
    // restore must happen BEFORE open() -- forKeyedProcessFunction's internal
    // open() call would make that impossible.
    //
    // Both assertions matter: a cold restore -- state silently lost, window
    // starts empty -- would still produce ONE output record for the second event
    // (record_count_5m == 1, inter_arrival_mean_ms == 0, since a first-ever
    // record contributes no interval). Only seeing 2 and 2500 together proves
    // the counters AND the timing state's Instant both came back.
    @Test
    void windowStateSurvivesASnapshotRestoreRoundTrip() throws Exception {
        SensorId sensor = new SensorId("sensor-eu-1");
        String sourceIp = "10.0.0.5";
        Instant t = Instant.ofEpochSecond(60_000);

        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harnessA =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new DnsFeatureProcessFunction(), new SourceKeySelector(), TypeInformation.of(SourceKey.class));

        harnessA.processElement(new StreamRecord<>(dnsEvent(sensor, sourceIp, t)));

        OperatorSubtaskState snapshot = harnessA.snapshot(0L, 0L);
        harnessA.close();

        // Direct constructor, per the Flink 2.2.1 harness facts verified for this
        // task: forKeyedProcessFunction cannot be reused here because it calls
        // open() itself, and state must be restored before open() runs.
        // KeyedProcessOperator here is
        // org.apache.flink.streaming.api.operators.KeyedProcessOperator, from
        // flink-runtime -- NOT the same-named class in
        // org.apache.flink.datastream.impl.operators, from flink-datastream.
        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harnessB =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DnsFeatureProcessFunction()),
                new SourceKeySelector(), TypeInformation.of(SourceKey.class));
        harnessB.initializeState(snapshot);
        harnessB.open();

        harnessB.processElement(new StreamRecord<>(dnsEvent(sensor, sourceIp, t.plusMillis(2500))));

        List<FeatureVector> output = harnessB.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals(2f, output.get(0).values()[0],
            "record_count_5m == 2 proves the restored counters saw both events, not just this one");
        assertEquals(2500f, output.get(0).values()[3], 1e-6f,
            "inter_arrival_mean_ms == 2500 proves RecordTimingState's Instant survived the round trip; "
                + "a cold restore would give 0 here because the second event would be the first one this "
                + "window ever saw");

        harnessB.close();
    }
}
