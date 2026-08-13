package io.netsecml.platform.domain.event;

public record EventId(String value) {
    public EventId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("EventId must not be blank");
        }
    }

    public static EventId derive(SensorId sensor, String upstreamId) {
        if (upstreamId == null || upstreamId.isBlank()) {
            throw new IllegalArgumentException("upstreamId must not be blank");
        }
        return new EventId(sensor.value() + ":" + upstreamId);
    }

    @Override
    public String toString() {
        return value;
    }
}
