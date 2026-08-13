package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConnectionMeasurementsTest {
    @Test
    void acceptsNonNegativeValues() {
        ConnectionMeasurements m = new ConnectionMeasurements(1500, 2048, 4096, 10, 12, 0);
        assertEquals(1500, m.durationMillis());
        assertEquals(2048, m.originBytes());
        assertEquals(4096, m.responseBytes());
        assertEquals(10, m.originPackets());
        assertEquals(12, m.responsePackets());
        assertEquals(0, m.missedBytes());
    }

    @Test
    void rejectsNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionMeasurements(-1, 0, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionMeasurements(0, -1, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionMeasurements(0, 0, 0, -1, 0, 0));
    }
}
