package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S7commConnectionKeyTest {

    private static S7commEvent event(String sensor, String uid, boolean request) {
        SensorId sensorId = new SensorId(sensor);
        return new S7commEvent(
            new EventEnvelope(EventId.derive(sensorId, uid + ":1"), Instant.parse("2026-09-24T10:00:00Z"), sensorId,
                LogType.S7COMM, uid),
            1790000000.0, request ? "10.0.0.5" : "10.0.0.9", request ? 50001 : 102,
            request ? "10.0.0.9" : "10.0.0.5", request ? 102 : 50001, 1, 1, 4, null);
    }

    @Test
    void aRequestAndItsResponseShareOneKey() {
        // Keyed by uid, never by the endpoints -- which swap between the two.
        assertEquals(S7commConnectionKey.of(event("sensor-eu-1", "C1", true)),
            S7commConnectionKey.of(event("sensor-eu-1", "C1", false)));
    }

    @Test
    void theSameUidOnTwoSensorsIsTwoKeys() {
        assertNotEquals(S7commConnectionKey.of(event("sensor-eu-1", "C1", true)),
            S7commConnectionKey.of(event("sensor-eu-2", "C1", true)));
    }

    @Test
    void aBlankUidIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new S7commConnectionKey(new SensorId("s"), " "));
    }
}
