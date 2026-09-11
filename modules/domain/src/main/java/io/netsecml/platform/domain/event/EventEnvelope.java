package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

// The identity block every log type shares, whatever the shape of its payload.
//
// Extracted so a protocol record carries only its own fields: a dns.log record
// has a uid and a timestamp like a conn record does, but none of a connection's
// duration or byte counts. Before this existed, NetworkEvent required all three
// conn components non-null, so a non-conn protocol could not be represented at
// all without fabricating them.
public record EventEnvelope(EventId eventId, Instant eventTime, SensorId sensor,
                            LogType logType, String connectionUid) {

    public EventEnvelope {
        // Identity and time are structural: eventId is the ClickHouse ORDER BY key
        // tail and eventTime is the partition key, so a row cannot be archived
        // without either.
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(sensor, "sensor must not be null");

        // Which Zeek log produced this. Without it the archive job cannot route the
        // event and the feature schema cannot be chosen.
        Objects.requireNonNull(logType, "logType must not be null");

        // connectionUid is a CORRELATION key, never an identity -- several dns or
        // http records legitimately share one uid. Some log types have no uid at
        // all, so an absent one normalises to "" rather than being rejected.
        connectionUid = connectionUid == null ? "" : connectionUid;
    }
}
