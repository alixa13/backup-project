package io.netsecml.platform.adapter.kafka.parser;

import io.netsecml.platform.adapter.kafka.dto.ZeekDnsEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JsonZeekDnsParserTest {
    private final JsonZeekDnsParser parser = new JsonZeekDnsParser();

    private byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_dns", name));
    }

    @Test
    void parsesValidARecordFixture() throws IOException {
        MappingResult<ZeekDnsEvent> result = parser.parse(fixture("valid-a-record.json"));
        assertTrue(result.isValid());
        ZeekDnsEvent dto = result.value();
        assertEquals("Cdns001ABC", dto.id());
        assertEquals(53, dto.idRespP());
        assertEquals(4242, dto.transId());
        assertEquals("example.com", dto.query());
        assertEquals(1, dto.qtype());
        assertEquals(0, dto.rcode());
        assertEquals(false, dto.aa());
        assertEquals(true, dto.ra());
        assertEquals(false, dto.tc());
        assertEquals(List.of("93.184.216.34"), dto.answers());
        assertEquals(List.of(299.0), dto.ttls());
    }

    // answers is optional and nullable: dns.log records a query that received no
    // answer (e.g. NXDOMAIN, rcode 3 in this fixture) as a real observation, not
    // a parse failure -- DnsEvent.response and DnsFeatureExtractor both rely on
    // being able to tell "no answer" apart from "answered with zero records", so
    // the DTO must preserve null here rather than defaulting to an empty list.
    @Test
    void answersAbsentStillParsesWithNullAnswers() throws IOException {
        MappingResult<ZeekDnsEvent> result = parser.parse(fixture("answers-absent.json"));
        assertTrue(result.isValid());
        ZeekDnsEvent dto = result.value();
        assertEquals(3, dto.rcode());
        assertNull(dto.answers());
    }

    // TTLs present-but-empty ([]) is a different wire state from TTLs absent
    // (null): DnsEventMapper (Task 8) reads TTLs.get(0), so this parser must
    // preserve the empty list rather than collapsing it to null. Collapsing the
    // two would make an empty list indistinguishable from absent, and a naive
    // get(0) on a genuinely empty (not null) list throws where get(0)-on-absent
    // (guarded by a null check) would not. The fixture gives answers the same
    // empty-array treatment (a realistic NOERROR/NODATA response), so this is
    // also checked here rather than left to look untested: both list fields go
    // through the identical Jackson mapping, and TTLs is merely the one Task 8
    // depends on.
    @Test
    void emptyListFieldsParseAsEmptyNotNull() throws IOException {
        MappingResult<ZeekDnsEvent> result = parser.parse(fixture("ttls-empty.json"));
        assertTrue(result.isValid());
        ZeekDnsEvent dto = result.value();
        assertNotNull(dto.ttls());
        assertTrue(dto.ttls().isEmpty());
        assertNotNull(dto.answers());
        assertTrue(dto.answers().isEmpty());
    }

    // trans_id is half of this log type's event identity (sensor:uid:trans_id):
    // a resolver reuses one connection for many queries, so several dns.log
    // records legitimately share a uid, and only trans_id disambiguates them.
    // Its absence is therefore a rejection, not a defaulted field -- structurally
    // the same situation as id_resp_p for conn, so it fails the same way: Jackson
    // cannot build the record and the parser reports MALFORMED_JSON at PARSE.
    @Test
    void missingTransIdFailsToParse() throws IOException {
        MappingResult<ZeekDnsEvent> result = parser.parse(fixture("missing-trans-id.json"));
        assertFalse(result.isValid());
        // Both the reason AND the stage matter: a malformed-JSON rejection and a
        // missing-field rejection must land in different DLQ columns, and
        // asserting isValid() alone cannot tell them apart.
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
        assertEquals(ReasonCode.Stage.PARSE, result.reason().stage());
    }

    @Test
    void rejectsMalformedJson() throws IOException {
        MappingResult<ZeekDnsEvent> result = parser.parse(fixture("malformed.json"));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }
}
