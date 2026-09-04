package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ReasonCode;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

// A record the pipeline refused, travelling on the DLQ side output.
//
// It holds the raw bytes because the Kafka serializer downstream hashes them;
// the bytes themselves are never published. receivedAt is stamped where the
// rejection happens, not where it is serialized. eventId is present only for
// map-stage rejections, where a DTO parsed successfully before domain
// validation refused it.
public record RejectedRecord(byte[] rawPayload, ReasonCode reason, String detail,
                              Instant receivedAt, String eventId) {
    public RejectedRecord {
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");

        // Parse-stage rejections have no identity; normalize once so nothing
        // downstream has to null-check.
        eventId = eventId == null ? "" : eventId;

        // Defensive copy in.
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    // Defensive copy out.
    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
