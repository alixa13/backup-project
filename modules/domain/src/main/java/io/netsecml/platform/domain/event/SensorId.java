package io.netsecml.platform.domain.event;

public record SensorId(String value) {
    public SensorId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("SensorId must not be blank");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
