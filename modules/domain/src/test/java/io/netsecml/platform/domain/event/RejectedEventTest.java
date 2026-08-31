package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class RejectedEventTest {
    private static final String HASH = "a".repeat(64);

    // The happy path: every component survives construction unchanged.
    @Test
    void storesAllComponents() {
        Instant receivedAt = Instant.parse("2026-08-27T10:03:11.250Z");
        RejectedEvent event = new RejectedEvent("sensor-eu-1:Cabc", HASH,
            ReasonCode.INVALID_PORT, "port 70000 out of range", receivedAt);

        assertEquals("sensor-eu-1:Cabc", event.eventId());
        assertEquals(HASH, event.rawPayloadHash());
        assertEquals(ReasonCode.INVALID_PORT, event.reason());
        assertEquals("port 70000 out of range", event.detail());
        assertEquals(receivedAt, event.receivedAt());
    }

    // Parse-stage rejections have no recoverable identity. ClickHouse's event_id
    // column is non-nullable String, so null normalizes to "" here rather than
    // becoming an adapter's problem later.
    @Test
    void normalizesNullEventIdAndDetailToEmptyString() {
        RejectedEvent event = new RejectedEvent(null, HASH,
            ReasonCode.MALFORMED_JSON, null, Instant.parse("2026-08-27T10:03:11.250Z"));

        assertEquals("", event.eventId());
        assertEquals("", event.detail());
    }

    // The hash is written into a FixedString(64) column; a wrong length would be
    // a silent server-side truncation or error, so reject it at construction.
    @Test
    void rejectsHashThatIsNotSixtyFourCharacters() {
        assertThrows(IllegalArgumentException.class, () -> new RejectedEvent(
            "", "tooshort", ReasonCode.MALFORMED_JSON, "", Instant.now()));
    }

    // reason and receivedAt are structural (see RejectedEvent's compact
    // constructor); this verifies both are independently null-checked.
    @Test
    void rejectsNullReasonAndNullReceivedAt() {
        assertThrows(NullPointerException.class, () -> new RejectedEvent(
            "", HASH, null, "", Instant.now()));
        assertThrows(NullPointerException.class, () -> new RejectedEvent(
            "", HASH, ReasonCode.MALFORMED_JSON, "", null));
    }
}
