package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final Instant EVENT_TIME = Instant.parse("2026-08-13T10:00:00Z");
    private static final Instant PRODUCED_AT = Instant.parse("2026-08-13T10:00:00.402Z");

    // The float[] component must be copied on the way in and on the way out, so
    // no caller can reach into the record's internal state.
    @Test
    void storesValuesAndReturnsDefensiveCopy() {
        float[] values = new float[]{1f, 2f, 3f};
        FeatureVector vector = new FeatureVector("sensor-eu-1:abc", EVENT_TIME, SENSOR,
            "conn-feature-v1", "hash123", values, 0, PRODUCED_AT);

        values[0] = -1f;
        assertEquals(1f, vector.values()[0], "mutating the array passed to the constructor must not affect internal state");

        float[] returned = vector.values();
        returned[0] = 999f;
        assertEquals(1f, vector.values()[0], "mutating the returned array must not affect internal state");
        assertEquals(3, vector.values().length);
    }

    // sensor and producedAt are what make a feature_vectors row self-sufficient
    // for training, so they are structural, not optional.
    @Test
    void exposesSensorAndProducedAt() {
        FeatureVector vector = new FeatureVector("sensor-eu-1:abc", EVENT_TIME, SENSOR,
            "conn-feature-v1", "hash123", new float[]{1f}, 0, PRODUCED_AT);

        assertEquals(SENSOR, vector.sensor());
        assertEquals(PRODUCED_AT, vector.producedAt());
    }

    @Test
    void rejectsNullValues() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", EVENT_TIME, SENSOR, "conn-feature-v1", "hash123", null, 0, PRODUCED_AT));
    }

    // Sensor and producedAt use different null-check idioms upstream
    // (IllegalArgumentException vs NPE); this verifies both independently.
    @Test
    void rejectsNullSensorAndNullProducedAt() {
        assertThrows(NullPointerException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", EVENT_TIME, null, "conn-feature-v1", "hash123", new float[]{1f}, 0, PRODUCED_AT));
        assertThrows(NullPointerException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", EVENT_TIME, SENSOR, "conn-feature-v1", "hash123", new float[]{1f}, 0, null));
    }
}
