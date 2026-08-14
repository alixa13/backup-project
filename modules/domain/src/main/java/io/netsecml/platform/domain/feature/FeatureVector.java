package io.netsecml.platform.domain.feature;

import java.time.Instant;
import java.util.Arrays;

public record FeatureVector(String eventId, Instant eventTime, String schemaId, String schemaHash,
                             float[] values, int qualityFlags) {
    public FeatureVector {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        values = Arrays.copyOf(values, values.length);
    }

    @Override
    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }
}
