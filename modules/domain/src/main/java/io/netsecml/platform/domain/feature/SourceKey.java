package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.SensorId;

public record SourceKey(SensorId sensor, String sourceIp) {
    public SourceKey {
        if (sensor == null) {
            throw new IllegalArgumentException("sensor must not be null");
        }
        if (sourceIp == null || sourceIp.isBlank()) {
            throw new IllegalArgumentException("sourceIp must not be blank");
        }
    }
}
