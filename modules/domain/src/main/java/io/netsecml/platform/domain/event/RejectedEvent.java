package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

// The consumer-side view of a rejected record: what the archive job reads off
// the DLQ topic and writes into invalid_events.
//
// It deliberately carries the payload HASH and never the payload bytes — the
// raw bytes exist only on the producer side, where RejectedRecord holds them
// long enough for the serializer to hash them.
//
// This type exists so adapter-kafka (which deserializes DLQ JSON) and
// adapter-clickhouse (which maps to a row) have somewhere neutral to meet
// without importing each other.
public record RejectedEvent(String eventId, String rawPayloadHash, ReasonCode reason,
                             String detail, Instant receivedAt) {

    // Length of a hex-encoded SHA-256, matching the FixedString(64) column.
    private static final int SHA256_HEX_LENGTH = 64;

    public RejectedEvent {
        // reason and receivedAt are structural — a row cannot be written without them.
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");

        // The hash lands in a FixedString(64); a wrong length is a silent
        // server-side error, so it is refused here instead.
        Objects.requireNonNull(rawPayloadHash, "rawPayloadHash must not be null");
        if (rawPayloadHash.length() != SHA256_HEX_LENGTH) {
            throw new IllegalArgumentException(
                "rawPayloadHash must be " + SHA256_HEX_LENGTH + " hex characters, was " + rawPayloadHash.length());
        }

        // Parse-stage rejections have no recoverable event identity, and detail
        // may be absent. Both columns are non-nullable String in ClickHouse, so
        // normalize to "" once here rather than in every mapper.
        eventId = eventId == null ? "" : eventId;
        detail = detail == null ? "" : detail;
    }
}
