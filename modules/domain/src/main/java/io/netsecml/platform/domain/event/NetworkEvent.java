package io.netsecml.platform.domain.event;

import java.time.Instant;

// One parsed, validated Zeek record, whatever log produced it.
//
// A sealed interface rather than a record because the log types genuinely differ
// in shape: a conn record carries a connection's duration and byte counts, and a
// dns record carries none of those. Modelling that as one record with nullable
// conn fields is the "generic event" PILOT_ARCHITECTURE rejects -- every consumer
// would have to know which fields are meaningful for which log type, and the
// compiler could not help.
//
// permits lists ONLY implemented log types. A record is added when a protocol has
// a parser, a mapper and a feature schema behind it, never before -- the same
// rule LogType states about its own constants. Sealing then makes the compiler
// flag every non-exhaustive switch the moment a real second protocol arrives.
public sealed interface NetworkEvent permits ConnEvent {

    // Every log type carries the same identity block; only the payload differs.
    EventEnvelope envelope();

    // The shared fields are exposed directly, delegating to the envelope, so the
    // many call sites that read only identity and timing neither know nor care
    // that the hierarchy exists.
    default EventId eventId() {
        return envelope().eventId();
    }

    default Instant eventTime() {
        return envelope().eventTime();
    }

    default SensorId sensor() {
        return envelope().sensor();
    }

    default LogType logType() {
        return envelope().logType();
    }

    default String connectionUid() {
        return envelope().connectionUid();
    }
}
