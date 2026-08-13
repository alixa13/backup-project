package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventIdTest {
    @Test
    void derivesNamespacedIdFromSensorAndUpstreamId() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        assertEquals("sensor-eu-1:Cabc123XYZ", id.value());
    }

    @Test
    void rejectsBlankUpstreamId() {
        SensorId sensor = new SensorId("sensor-eu-1");
        assertThrows(IllegalArgumentException.class, () -> EventId.derive(sensor, ""));
    }

    @Test
    void rejectsBlankDirectConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new EventId(""));
    }
}
