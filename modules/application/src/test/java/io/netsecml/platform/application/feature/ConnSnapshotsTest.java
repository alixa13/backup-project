package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.ConnectionLocality;
import io.netsecml.platform.domain.event.ConnectionMeasurements;
import io.netsecml.platform.domain.event.ConnectionState;
import io.netsecml.platform.domain.event.ConnectionTuple;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.Protocol;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.event.ServiceCode;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

// ConnSnapshots.fromConnEvent is the producer side of the conn.log enrichment
// join (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2): it turns a parsed conn.log record into the cumulative snapshot
// ConnSnapshotJoinFunction (adapter-flink) keys its state on. These tests pin
// the field mapping and the one guard this pure function owns.
class ConnSnapshotsTest {

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // Builds a minimal, valid conn.log event with a caller-chosen uid, event
    // time and measurements -- everything else (tuple endpoints, locality) is
    // fixed because no test here cares about their shape, mirroring the
    // reduction ConnFeatureProcessFunctionTest's own event() helper makes for
    // the same reason.
    private ConnEvent connEvent(String uid, Instant eventTime, long durationMillis,
                                long originBytes, long responseBytes, int originPackets, int responsePackets) {
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        ConnectionMeasurements measurements = new ConnectionMeasurements(
            durationMillis, originBytes, responseBytes, originPackets, responsePackets, 0);
        // EventId is built directly rather than through EventId.derive, because
        // derive itself refuses a blank upstream id -- and the whole point of
        // the second test below is to construct a ConnEvent whose connectionUid
        // IS blank while its eventId stays valid.
        EventId eventId = EventId.derive(SENSOR, "evt-" + eventTime.toEpochMilli());
        EventEnvelope envelope = new EventEnvelope(eventId, eventTime, SENSOR, LogType.CONN, uid);
        return new ConnEvent(envelope, tuple, measurements, new ConnectionLocality(null, null));
    }

    // Every field on the snapshot must come from the event it was built from,
    // not a default or a coincidentally-matching value -- observedAt in
    // particular is DERIVED (eventTime + durationMillis), not copied from any
    // single field on ConnEvent, so it gets its own explicit assertion.
    @Test
    void mapsEveryConnEventFieldOntoTheSnapshot() {
        Instant eventTime = Instant.parse("2026-09-10T10:00:00Z");
        long durationMillis = 45_000;
        ConnEvent event = connEvent("CHhLku3vLBWyxJgQ7h", eventTime, durationMillis, 1000L, 2000L, 10, 20);

        Optional<ConnSnapshot> result = ConnSnapshots.fromConnEvent(event);

        assertTrue(result.isPresent(), "a well-formed ConnEvent must always produce a snapshot");
        ConnSnapshot snapshot = result.get();
        assertEquals("CHhLku3vLBWyxJgQ7h", snapshot.connectionUid());
        // connectionStart is the connection's FIRST packet -- conn.log's ts is
        // that on every record Zeek emits for a connection, interim and final
        // alike, so it is eventTime unchanged, never eventTime minus duration.
        assertEquals(eventTime, snapshot.connectionStart());
        // observedAt has no field of its own on conn.log: it is how far past
        // connectionStart this particular snapshot's cumulative counters run.
        assertEquals(eventTime.plusMillis(durationMillis), snapshot.observedAt());
        assertEquals(1000L, snapshot.origBytes());
        assertEquals(2000L, snapshot.respBytes());
        assertEquals(10L, snapshot.origPkts());
        assertEquals(20L, snapshot.respPkts());
    }

    // ConnSnapshot's own compact constructor THROWS IllegalArgumentException on
    // a blank connectionUid. fromConnEvent runs inside a Flink FlatMapFunction
    // (ConnSnapshotExtractFunction), where an escaping exception crash-loops the
    // whole job on a single bad record, so this method must turn that throw into
    // Optional.empty() before the constructor ever sees the blank uid.
    // assertDoesNotThrow makes that guarantee explicit rather than incidental --
    // a test that only checked isEmpty() would still pass if the exception were
    // swallowed by a try/catch instead of prevented by an upfront check, which
    // is a strictly weaker claim.
    @Test
    void blankConnectionUidYieldsEmptyOptionalRatherThanThrowing() {
        Instant eventTime = Instant.parse("2026-09-10T10:00:00Z");
        ConnEvent event = connEvent("", eventTime, 1000L, 1L, 1L, 1, 1);

        Optional<ConnSnapshot> result = assertDoesNotThrow(() -> ConnSnapshots.fromConnEvent(event));

        assertTrue(result.isEmpty());
    }
}
