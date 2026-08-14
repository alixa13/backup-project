package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorTest {
    @Test
    void storesValuesAndReturnsDefensiveCopy() {
        float[] values = new float[]{1f, 2f, 3f};
        FeatureVector vector = new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-13T10:00:00Z"),
            "conn-feature-v1", "hash123", values, 0);

        values[0] = -1f;
        assertEquals(1f, vector.values()[0], "mutating the array passed to the constructor must not affect internal state");

        float[] returned = vector.values();
        returned[0] = 999f;
        assertEquals(1f, vector.values()[0], "mutating the returned array must not affect internal state");
        assertEquals(3, vector.values().length);
    }

    @Test
    void rejectsNullValues() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", Instant.now(), "conn-feature-v1", "hash123", null, 0));
    }
}
