package io.netsecml.platform.adapter.clickhouse.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class InvalidEventRowMapperTest {
    private static final String HASH = "a".repeat(64);
    // Conn is the only log type that exists today, so it is the fixed binding
    // for every test here that isn't specifically exercising construction.
    private final InvalidEventRowMapper mapper = new InvalidEventRowMapper(LogType.CONN);

    // A map-stage rejection parsed cleanly before domain validation refused it,
    // so it knows which event failed.
    @Test
    void mapsAMapStageRejection() {
        RejectedEvent event = new RejectedEvent("sensor-eu-1:Cabc", HASH, ReasonCode.INVALID_PORT,
            "port 70000 out of range", Instant.parse("2026-08-27T10:03:11.250Z"));

        InvalidEventRow row = mapper.toRow(event);

        assertEquals("sensor-eu-1:Cabc", row.eventId());
        assertEquals("MAP", row.stage());
        assertEquals("INVALID_PORT", row.reasonCode());
        assertEquals("port 70000 out of range", row.detail());
        assertEquals(HASH, row.rawPayloadHash());
        assertEquals("2026-08-27 10:03:11.250", row.receivedAt());
        assertEquals(mapper.sourceVersion(), row.sourceVersion());
    }

    // A parse-stage rejection never produced a DTO. event_id is the empty string
    // and event_time stays null — the column is Nullable for exactly this case.
    @Test
    void mapsAParseStageRejectionWithNoIdentity() {
        RejectedEvent event = new RejectedEvent(null, HASH, ReasonCode.MALFORMED_JSON,
            "unexpected end of input", Instant.parse("2026-08-27T10:03:11.250Z"));

        InvalidEventRow row = mapper.toRow(event);

        assertEquals("", row.eventId());
        assertNull(row.eventTime());
        assertEquals("PARSE", row.stage());
    }

    // The stage is taken from the domain's ReasonCode, never re-derived here.
    @Test
    void takesStageFromTheDomainReasonCode() {
        // Every reason code, not just one, so a future MAP/PARSE reassignment in
        // ReasonCode is caught here instead of silently drifting.
        for (ReasonCode code : ReasonCode.values()) {
            RejectedEvent event = new RejectedEvent("", HASH, code, "", Instant.parse("2026-08-27T10:03:11.250Z"));
            assertEquals(code.stage().name(), mapper.toRow(event).stage());
        }
    }

    // The @JsonProperty names ARE the ClickHouse column names — the sink writes
    // this record straight out as a JSONEachRow line. created_at is absent on
    // purpose: the column carries DEFAULT now64(3) and is set server-side.
    @Test
    void serializesToExactlyTheClickHouseColumnNames() throws Exception {
        RejectedEvent event = new RejectedEvent("id", HASH, ReasonCode.INVALID_PORT, "d",
            Instant.parse("2026-08-27T10:03:11.250Z"));

        String json = new ObjectMapper().writeValueAsString(mapper.toRow(event));
        // Read the JSON back and collect only the field names, so this test
        // fails on any renamed/added/dropped column regardless of value.
        Set<String> emitted = new LinkedHashSet<>();
        new ObjectMapper().readTree(json).fieldNames().forEachRemaining(emitted::add);

        assertEquals(new LinkedHashSet<>(List.of(
            "event_id", "event_time", "received_at", "stage", "reason_code",
            "detail", "source_version", "raw_payload_hash", "log_type")), emitted);
    }

    // The log type is supplied when the mapper is constructed, not read from the
    // event. dlq-v1 is frozen and carries no protocol field: the archive job knows
    // the log type from the topic it is reading, bound at wiring time.
    @Test
    void carriesTheConstructedLogTypeOntoTheRow() {
        RejectedEvent event = new RejectedEvent("sensor-eu-1:abc", "f".repeat(64),
            ReasonCode.MALFORMED_JSON, "unparseable", Instant.parse("2026-09-10T10:00:00Z"));

        InvalidEventRow row = new InvalidEventRowMapper(LogType.CONN).toRow(event);

        assertEquals("conn", row.logType(),
            "the row must carry the wire form, matching feature_vectors.log_type");
    }

    // The wire form is lowercase, the same value the success path writes. A row
    // reading "CONN" would be silently accepted by LowCardinality(String) and
    // would not join against feature_vectors.
    @Test
    void usesTheWireFormNotTheEnumName() {
        RejectedEvent event = new RejectedEvent("sensor-eu-1:abc", "f".repeat(64),
            ReasonCode.MALFORMED_JSON, "unparseable", Instant.parse("2026-09-10T10:00:00Z"));

        InvalidEventRow row = new InvalidEventRowMapper(LogType.CONN).toRow(event);

        assertNotEquals(LogType.CONN.name(), row.logType());
        assertEquals(LogType.CONN.wireName(), row.logType());
    }

    // A mapper without a log type cannot produce a diagnosable row, which is this
    // unit's whole purpose, so it is a construction error rather than a default.
    @Test
    void rejectsANullLogType() {
        assertThrows(IllegalArgumentException.class, () -> new InvalidEventRowMapper(null));
    }

    // SOURCE_VERSION is derived from the constructed log type rather than
    // hardcoded, so it stays correct as more Zeek log types are added. This pins
    // today's only value so existing behaviour does not silently drift.
    @Test
    void derivesSourceVersionFromTheLogType() {
        assertEquals("zeek-conn-source-v1", new InvalidEventRowMapper(LogType.CONN).sourceVersion());
    }
}
