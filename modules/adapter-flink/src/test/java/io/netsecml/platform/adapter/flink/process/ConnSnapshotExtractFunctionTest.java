package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// ConnSnapshotExtractFunction is the producer half of the conn.log enrichment
// left join (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2): a plain FlatMapFunction with no Flink-managed state, so unlike
// ConnSnapshotJoinFunctionTest these tests call flatMap directly rather than
// going through a Flink test harness -- there is no state or timer behavior a
// harness would exercise that a direct call does not.
class ConnSnapshotExtractFunctionTest {

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final Instant T0 = Instant.parse("2026-09-10T10:00:00Z");

    // A minimal Collector<T> test double that records what it is given, in
    // order. Not a lambda: this project's "no lambdas" rule is about Flink
    // functions/selectors wired into the job graph, but a named class costs
    // nothing here either and keeps this file consistent with the rest of the
    // adapter-flink test suite's style.
    private static final class RecordingCollector<T> implements Collector<T> {
        private final List<T> collected = new ArrayList<>();

        @Override
        public void collect(T record) {
            collected.add(record);
        }

        @Override
        public void close() {
        }
    }

    private ConnEvent connEvent(String uid, Instant eventTime, long durationMillis,
                                long originBytes, long responseBytes, int originPackets, int responsePackets) {
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        ConnectionMeasurements measurements = new ConnectionMeasurements(
            durationMillis, originBytes, responseBytes, originPackets, responsePackets, 0);
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, uid + eventTime), eventTime, SENSOR,
            LogType.CONN, uid);
        return new ConnEvent(envelope, tuple, measurements, new ConnectionLocality(null, null));
    }

    private NetworkEvent dnsEvent(String uid, Instant eventTime) {
        DnsQuery query = new DnsQuery("example.com", DnsQType.A, 42, DnsQType.A.code());
        DnsResponse response = new DnsResponse(DnsRcode.NOERROR, false, true, false, 1, 300L, DnsRcode.NOERROR.code());
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, uid + eventTime), eventTime, SENSOR,
            LogType.DNS, uid);
        return new DnsEvent(envelope, query, response, "10.0.0.5", true, null);
    }

    // A ConnEvent must yield exactly the snapshot ConnSnapshots.fromConnEvent
    // would produce -- asserted field-by-field here (not by re-deriving the
    // expected value through ConnSnapshots.fromConnEvent itself) so this test
    // has independent value rather than merely proving the two call the same
    // code.
    @Test
    void connEventYieldsItsSnapshot() {
        ConnSnapshotExtractFunction function = new ConnSnapshotExtractFunction();
        RecordingCollector<ConnSnapshot> collector = new RecordingCollector<>();
        ConnEvent event = connEvent("Cabc", T0, 45_000L, 1000L, 2000L, 10, 20);

        function.flatMap(event, collector);

        assertEquals(1, collector.collected.size());
        ConnSnapshot snapshot = collector.collected.get(0);
        assertEquals("Cabc", snapshot.connectionUid());
        assertEquals(T0, snapshot.connectionStart());
        assertEquals(T0.plusMillis(45_000L), snapshot.observedAt());
        assertEquals(1000L, snapshot.origBytes());
        assertEquals(2000L, snapshot.respBytes());
        assertEquals(10L, snapshot.origPkts());
        assertEquals(20L, snapshot.respPkts());
    }

    // A DnsEvent reaching this function is a wiring error (this extractor sits
    // only on the conn.log chain), not a runtime condition, so it must throw
    // rather than silently skip the record.
    @Test
    void dnsEventIsAWiringError() {
        ConnSnapshotExtractFunction function = new ConnSnapshotExtractFunction();
        RecordingCollector<ConnSnapshot> collector = new RecordingCollector<>();
        NetworkEvent event = dnsEvent("Cabc", T0);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
            () -> function.flatMap(event, collector));
        assertTrue(thrown.getMessage().contains("DnsEvent"),
            "exception message should name the offending type: " + thrown.getMessage());
        assertTrue(collector.collected.isEmpty(), "nothing should be emitted before the throw");
    }
}
