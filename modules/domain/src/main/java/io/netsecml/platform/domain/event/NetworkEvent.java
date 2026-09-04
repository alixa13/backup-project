package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

public record NetworkEvent(EventId eventId, Instant eventTime, SensorId sensor, LogType logType,
                            String connectionUid, ConnectionTuple connection,
                            ConnectionMeasurements measurements, ConnectionLocality locality) {
    public NetworkEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(sensor, "sensor must not be null");

        // logType is structural: every event must say which Zeek log produced it.
        Objects.requireNonNull(logType, "logType must not be null");

        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(measurements, "measurements must not be null");
        Objects.requireNonNull(locality, "locality must not be null");

        // A log type carrying no correlation uid (Modbus/S7comm may not) yields ""
        // rather than null, matching the convention RejectedEvent.eventId already uses.
        connectionUid = connectionUid == null ? "" : connectionUid;
    }
}
