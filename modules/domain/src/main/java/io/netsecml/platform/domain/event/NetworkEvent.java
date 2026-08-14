package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

public record NetworkEvent(EventId eventId, Instant eventTime, SensorId sensor, ConnectionTuple connection,
                            ConnectionMeasurements measurements, ConnectionLocality locality) {
    public NetworkEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(sensor, "sensor must not be null");
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(measurements, "measurements must not be null");
        Objects.requireNonNull(locality, "locality must not be null");
    }
}
