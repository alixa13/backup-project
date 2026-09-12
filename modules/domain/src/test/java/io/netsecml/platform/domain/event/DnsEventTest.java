package io.netsecml.platform.domain.event;

import java.time.Instant;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DnsEventTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final EventEnvelope ENVELOPE = new EventEnvelope(
        EventId.derive(SENSOR, "Dabc123XYZ"),
        Instant.parse("2026-09-12T10:00:00Z"),
        SENSOR,
        LogType.DNS,
        "Dabc123XYZ");
    private static final DnsQuery QUERY = new DnsQuery("example.com", DnsQType.A, 1);
    private static final DnsResponse RESPONSE = new DnsResponse(DnsRcode.NOERROR, true, true, false, 1, 300L);

    @Test
    void buildsValidEventWithAResponse() {
        DnsEvent event = new DnsEvent(ENVELOPE, QUERY, RESPONSE, "10.0.0.5", true, null);

        assertEquals(ENVELOPE, event.envelope());
        assertEquals(QUERY, event.query());
        assertEquals(RESPONSE, event.response());
        assertEquals("10.0.0.5", event.sourceIp());
        assertTrue(event.isOrig());
        assertNull(event.enrichment(), "enrichment must be null by default -- no left join has run yet");
    }

    // response is deliberately nullable: a query with no answer is a real
    // observation dns.log records, not a parse failure. This is the case the
    // extractor's DEFAULT_ZERO policy for indices 12, 14-18 exists to handle.
    @Test
    void acceptsANullResponseForAQueryThatReceivedNoAnswer() {
        DnsEvent event = new DnsEvent(ENVELOPE, QUERY, null, "10.0.0.5", true, null);
        assertNull(event.response());
    }

    @Test
    void rejectsNullEnvelopeOrQuery() {
        assertThrows(NullPointerException.class,
            () -> new DnsEvent(null, QUERY, RESPONSE, "10.0.0.5", true, null));
        assertThrows(NullPointerException.class,
            () -> new DnsEvent(ENVELOPE, null, RESPONSE, "10.0.0.5", true, null));
    }

    @Test
    void rejectsBlankSourceIp() {
        assertThrows(IllegalArgumentException.class,
            () -> new DnsEvent(ENVELOPE, QUERY, RESPONSE, null, true, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DnsEvent(ENVELOPE, QUERY, RESPONSE, "  ", true, null));
    }

    // The DNS query record itself rejects a blank name -- it feeds five of the
    // twelve protocol features, so a blank name is a malformed record, not a
    // missing-value case.
    @Test
    void queryRejectsBlankName() {
        assertThrows(IllegalArgumentException.class, () -> new DnsQuery("  ", DnsQType.A, 1));
        assertThrows(NullPointerException.class, () -> new DnsQuery("example.com", null, 1));
    }

    @Test
    void responseRequiresRcode() {
        assertThrows(NullPointerException.class,
            () -> new DnsResponse(null, false, false, false, 0, 0L));
    }

    // withEnrichment is how the (not-yet-built) join operator hands back a
    // populated event without mutating the one it received -- records have no
    // setters, so a new instance is the only way.
    @Test
    void withEnrichmentReturnsANewInstanceCarryingTheDelta() {
        DnsEvent original = new DnsEvent(ENVELOPE, QUERY, RESPONSE, "10.0.0.5", true, null);
        ConnSnapshotDelta delta = new ConnSnapshotDelta(100, 200, 3, 4, 30);

        DnsEvent enriched = original.withEnrichment(delta);

        assertNull(original.enrichment(), "the original instance must be unchanged");
        assertEquals(delta, enriched.enrichment());
        assertEquals(original.envelope(), enriched.envelope());
        assertEquals(original.query(), enriched.query());
        assertEquals(original.response(), enriched.response());
        assertEquals(original.sourceIp(), enriched.sourceIp());
        assertEquals(original.isOrig(), enriched.isOrig());
    }

    // Shared accessors delegate to the envelope, same as ConnEvent -- this is
    // what lets the many call sites that read only identity/timing stay ignorant
    // of which record type they were handed.
    @Test
    void sharedAccessorsDelegateToTheEnvelope() {
        NetworkEvent event = new DnsEvent(ENVELOPE, QUERY, RESPONSE, "10.0.0.5", true, null);

        assertEquals(ENVELOPE.eventId(), event.eventId());
        assertEquals(ENVELOPE.eventTime(), event.eventTime());
        assertEquals(ENVELOPE.sensor(), event.sensor());
        assertEquals(LogType.DNS, event.logType());
        assertEquals("Dabc123XYZ", event.connectionUid());
    }

    @Test
    void rcodeAndQTypeMapToIanaRegistryNumbers() {
        assertEquals(0, DnsRcode.NOERROR.code());
        assertEquals(3, DnsRcode.NXDOMAIN.code());
        assertEquals(-1, DnsRcode.OTHER.code());
        assertEquals(DnsRcode.NXDOMAIN, DnsRcode.fromCode(3));
        assertEquals(DnsRcode.OTHER, DnsRcode.fromCode(999));
        assertFalse(DnsRcode.NOERROR.isFailure());
        assertTrue(DnsRcode.NXDOMAIN.isFailure());

        assertEquals(1, DnsQType.A.code());
        assertEquals(28, DnsQType.AAAA.code());
        assertEquals(-1, DnsQType.OTHER.code());
        assertEquals(DnsQType.AAAA, DnsQType.fromCode(28));
        assertEquals(DnsQType.OTHER, DnsQType.fromCode(999));
    }
}
