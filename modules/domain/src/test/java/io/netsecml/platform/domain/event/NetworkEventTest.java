package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class NetworkEventTest {
    // Shared conn-only components, built once so every test in this class
    // compares against the same values instead of re-deriving them.
    private static final ConnectionTuple TUPLE = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
        Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
    private static final ConnectionMeasurements MEASUREMENTS = new ConnectionMeasurements(1500, 2048, 4096, 10, 12, 0);
    private static final ConnectionLocality LOCALITY = new ConnectionLocality(null, null);

    @Test
    void buildsValidEvent() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        EventEnvelope envelope = new EventEnvelope(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            LogType.CONN, "Cabc123XYZ");
        ConnEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);
        assertEquals(id, event.eventId());
        assertEquals(sensor, event.sensor());
        assertEquals(443, event.connection().destinationPort());
        assertEquals(2048, event.measurements().originBytes());
    }

    @Test
    void rejectsNullRequiredFields() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");

        // A null eventTime is now rejected by the envelope, not by ConnEvent -- the
        // envelope owns identity and timing, so it is the one that must throw.
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            id, null, sensor, LogType.CONN, "Cabc123XYZ"));

        // A null connection tuple is still rejected by ConnEvent, since only ConnEvent
        // knows a conn record is malformed without one.
        EventEnvelope envelope = new EventEnvelope(id, Instant.now(), sensor, LogType.CONN, "Cabc123XYZ");
        assertThrows(NullPointerException.class, () -> new ConnEvent(
            envelope, null, MEASUREMENTS, LOCALITY));
    }

    // logType and connectionUid are this task's two additions; downstream code
    // (the serializer, later the archive job) reads them straight off the
    // record, so a plain round-trip is what matters here.
    @Test
    void logTypeAndConnectionUidRoundTrip() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        EventEnvelope envelope = new EventEnvelope(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            LogType.CONN, "Cabc123XYZ");
        ConnEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);

        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc123XYZ", event.connectionUid());
    }

    // logType is structural (every event must say which log produced it), so a
    // null is rejected outright. connectionUid is not always available upstream
    // (Modbus/S7comm may carry no uid), so a null is normalized to "" instead of
    // rejected, the same convention RejectedEvent.eventId already uses. Both
    // behaviours now live on EventEnvelope, since it owns both fields.
    @Test
    void nullLogTypeThrowsButNullConnectionUidNormalizesToEmpty() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");

        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            id, Instant.now(), sensor, null, "Cabc123XYZ"));

        EventEnvelope envelope = new EventEnvelope(id, Instant.now(), sensor, LogType.CONN, null);
        ConnEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);
        assertEquals("", event.connectionUid());
    }

    // The default accessors are what let the fourteen files that read only shared
    // fields keep compiling across this refactor. If they stopped delegating, every
    // one of those call sites would have to learn about ConnEvent.
    @Test
    void sharedAccessorsDelegateToTheEnvelope() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(new SensorId("sensor-eu-1"), "Cabc"),
            Instant.parse("2026-09-11T10:00:00Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc");

        NetworkEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);

        assertEquals(envelope.eventId(), event.eventId());
        assertEquals(envelope.eventTime(), event.eventTime());
        assertEquals(envelope.sensor(), event.sensor());
        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc", event.connectionUid());
    }

    // permits lists only implemented log types, so a switch over NetworkEvent is
    // exhaustive with a single case today. When a real second protocol lands, the
    // compiler flags every switch that did not grow with it -- which is the whole
    // reason for sealing rather than leaving the interface open.
    @Test
    void aSwitchOverTheHierarchyIsExhaustiveWithoutADefaultBranch() {
        NetworkEvent event = new ConnEvent(
            new EventEnvelope(EventId.derive(new SensorId("s"), "C"),
                Instant.parse("2026-09-11T10:00:00Z"), new SensorId("s"), LogType.CONN, "C"),
            TUPLE, MEASUREMENTS, LOCALITY);

        String described = switch (event) {
            case ConnEvent conn -> "conn:" + conn.connection().sourceIp();
        };

        assertTrue(described.startsWith("conn:"));
    }
}
