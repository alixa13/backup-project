package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class NetworkEventTest {
    private ConnectionTuple sampleTuple() {
        return new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
    }

    private ConnectionMeasurements sampleMeasurements() {
        return new ConnectionMeasurements(1500, 2048, 4096, 10, 12, 0);
    }

    @Test
    void buildsValidEvent() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        NetworkEvent event = new NetworkEvent(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            LogType.CONN, "Cabc123XYZ", sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null));
        assertEquals(id, event.eventId());
        assertEquals(sensor, event.sensor());
        assertEquals(443, event.connection().destinationPort());
        assertEquals(2048, event.measurements().originBytes());
    }

    @Test
    void rejectsNullRequiredFields() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        assertThrows(NullPointerException.class, () -> new NetworkEvent(
            id, null, sensor, LogType.CONN, "Cabc123XYZ", sampleTuple(), sampleMeasurements(),
            new ConnectionLocality(null, null)));
        assertThrows(NullPointerException.class, () -> new NetworkEvent(
            id, Instant.now(), sensor, LogType.CONN, "Cabc123XYZ", null, sampleMeasurements(),
            new ConnectionLocality(null, null)));
    }

    // logType and connectionUid are this task's two additions; downstream code
    // (the serializer, later the archive job) reads them straight off the
    // record, so a plain round-trip is what matters here.
    @Test
    void logTypeAndConnectionUidRoundTrip() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        NetworkEvent event = new NetworkEvent(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            LogType.CONN, "Cabc123XYZ", sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null));

        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc123XYZ", event.connectionUid());
    }

    // logType is structural (every event must say which log produced it), so a
    // null is rejected outright. connectionUid is not always available upstream
    // (Modbus/S7comm may carry no uid), so a null is normalized to "" instead of
    // rejected, the same convention RejectedEvent.eventId already uses.
    @Test
    void nullLogTypeThrowsButNullConnectionUidNormalizesToEmpty() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");

        assertThrows(NullPointerException.class, () -> new NetworkEvent(
            id, Instant.now(), sensor, null, "Cabc123XYZ", sampleTuple(), sampleMeasurements(),
            new ConnectionLocality(null, null)));

        NetworkEvent event = new NetworkEvent(id, Instant.now(), sensor, LogType.CONN, null,
            sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null));
        assertEquals("", event.connectionUid());
    }
}
