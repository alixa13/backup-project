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
            sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null));
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
            id, null, sensor, sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null)));
        assertThrows(NullPointerException.class, () -> new NetworkEvent(
            id, Instant.now(), sensor, null, sampleMeasurements(), new ConnectionLocality(null, null)));
    }
}
