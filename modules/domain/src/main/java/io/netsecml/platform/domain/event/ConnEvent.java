package io.netsecml.platform.domain.event;

import java.util.Objects;

// A conn.log record: the shared envelope plus the three components only a
// connection has.
//
// These were fields on NetworkEvent itself until the hierarchy was sealed. They
// are required here, exactly as they were before, because a conn record without
// a tuple, measurements or locality is malformed -- but they are now required of
// ConnEvent alone, so a log type that has no such notion is not forced to invent
// them.
public record ConnEvent(EventEnvelope envelope, ConnectionTuple connection,
                        ConnectionMeasurements measurements, ConnectionLocality locality)
        implements NetworkEvent {

    public ConnEvent {
        Objects.requireNonNull(envelope, "envelope must not be null");

        // Unchanged from the pre-refactor NetworkEvent: a conn record is not
        // meaningful without all three.
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(measurements, "measurements must not be null");
        Objects.requireNonNull(locality, "locality must not be null");
    }
}
