package io.netsecml.platform.adapter.kafka.sink;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

// The producer-side shape of a DLQ message. It still carries the raw bytes
// because RejectedRecordSerializer hashes them on the way out; the bytes are
// never published. This is deliberately NOT the same type as domain
// RejectedEvent, which is the consumer-side view and carries only the hash.
public record RejectedRecordPayload(byte[] rawPayload, String eventId, String stage,
                                     String reasonCode, String detail, Instant receivedAt) {
    public RejectedRecordPayload {
        // Unlike eventId and detail below, receivedAt has no sensible default —
        // it is required rather than normalized.
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");

        // Normalize the optional text fields once, so the serializer never emits null.
        eventId = eventId == null ? "" : eventId;
        detail = detail == null ? "" : detail;

        // Defensive copy in.
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    // Defensive copy out.
    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
