package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SensorIdTest {
    @Test
    void rejectsBlank() {
        assertThrows(IllegalArgumentException.class, () -> new SensorId(""));
        assertThrows(IllegalArgumentException.class, () -> new SensorId(null));
    }

    @Test
    void acceptsNonBlankValue() {
        assertEquals("sensor-eu-1", new SensorId("sensor-eu-1").value());
    }
}
