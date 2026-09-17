package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekDnsEvent;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekDnsParser;
import io.netsecml.platform.domain.event.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DnsEventMapperTest {
    private final JsonZeekDnsParser parser = new JsonZeekDnsParser();
    private final DnsEventMapper mapper = new DnsEventMapper();
    private final SensorId sensor = new SensorId("sensor-eu-1");

    private ZeekDnsEvent fixture(String name) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_dns", name));
        return parser.parse(bytes).value();
    }

    // The plan's fixture pseudocode (task-7-brief.md, BINDING CORRECTIONS #9)
    // calls this with exactly (uid, transId, query), and no such helper exists
    // anywhere in the codebase yet -- defined here to match that call shape.
    // Every other field is fixed at the same values as
    // tests/fixtures/zeek_dns/valid-a-record.json (a realistic, fully valid
    // dns.log record) because tests using this 3-arg form never need to vary
    // anything else; tests that do vary another field use the fuller overload
    // below instead of growing this one's parameter list.
    private ZeekDnsEvent dto(String uid, int transId, String query) {
        return dto(uid, "10.0.0.5", transId, query, 1, 0, false, true, false,
            List.of("93.184.216.34"), List.of(299.0));
    }

    // Fuller overload for tests that vary a field the 3-arg dto() above cannot
    // reach: id_orig_h, qtype, rcode, the AA/RA/TC flags, answers or TTLs.
    // id_orig_p, id_resp_h and id_resp_p are deliberately not parameters at all
    // -- DnsEventMapper reads none of them (BINDING CORRECTIONS #7), so no test
    // needs to vary what the mapper never looks at; they stay fixed at the same
    // valid-a-record.json values used everywhere else in this file.
    private ZeekDnsEvent dto(String uid, String idOrigH, int transId, String query, Integer qtype,
                              Integer rcode, Boolean aa, Boolean ra, Boolean tc,
                              List<String> answers, List<Double> ttls) {
        return new ZeekDnsEvent(uid, 1786608100.5, idOrigH, 53421, "8.8.8.8", 53,
            transId, query, qtype, rcode, aa, ra, tc, answers, ttls);
    }

    // Spec section 10 ("Fixture obligations") of
    // docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
    // -- named just "section 10" in task-7-brief.md and the unit plan, which is
    // unambiguous there but not in this file with three numbered spec docs in
    // play; the filename is added here for that reason, not because the
    // citation itself was wrong. That section 10 in turn restates section 5.4
    // of docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md,
    // the original per-log-type-identity rule.
    //
    // This is also the reason DNS's identity is sensor:uid:trans_id rather than
    // sensor:uid. A resolver reuses one connection for many queries, so several
    // dns.log records legitimately share a uid. If eventId were derived from
    // uid alone they would collapse into ONE ReplacingMergeTree row and every
    // query but the last would vanish from the archive -- silently, with no
    // error anywhere.
    @Test
    void twoQueriesSharingAUidGetDistinctEventIds() {
        SensorId sensor = new SensorId("sensor-eu-1");
        ZeekDnsEvent first = dto("CXWv6p3arKYeMETxOg", 4242, "example.com");
        ZeekDnsEvent second = dto("CXWv6p3arKYeMETxOg", 4243, "example.org");

        NetworkEvent a = new DnsEventMapper().map(first, sensor).value();
        NetworkEvent b = new DnsEventMapper().map(second, sensor).value();

        assertNotEquals(a.eventId().value(), b.eventId().value());
        assertEquals("sensor-eu-1:CXWv6p3arKYeMETxOg:4242", a.eventId().value());

        // connectionUid is the CORRELATION key and is SUPPOSED to be shared -- it
        // is what joins these two queries back to their connection. Asserting it
        // stays equal is asserting the two concepts did not get conflated.
        assertEquals(a.connectionUid(), b.connectionUid());
    }

    // Happy path, narrowed to DnsEvent so the query- and response-specific
    // fields can be asserted. eventId and eventTime are hand-derived from the
    // fixture's raw id/trans_id/ts, not by re-invoking the mapper's own
    // formula, so this test would actually catch a wrong derivation rather
    // than mirror it back at itself.
    @Test
    void mapsValidFixtureToADnsEventWithQueryAndResponse() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-a-record.json"), sensor);
        assertTrue(result.isValid());
        NetworkEvent event = result.value();
        assertEquals("sensor-eu-1:Cdns001ABC:4242", event.eventId().value());
        assertEquals(Instant.ofEpochMilli(1786608100500L), event.eventTime());

        // DnsEventMapper handles dns.log exclusively, so its output is always a
        // DnsEvent -- this switch reaches the dns-specific fields to assert on,
        // with an explicit arm rather than a default, mirroring EventMapperTest
        // on the other side of this same sealed hierarchy: a ConnEvent here
        // would be a wiring bug this test should catch, not paper over.
        DnsEvent dns = switch (event) {
            case DnsEvent d -> d;
            case ConnEvent c -> throw new AssertionError("DnsEventMapper maps dns.log exclusively; got a ConnEvent");
        };
        assertEquals("example.com", dns.query().name());
        assertEquals(DnsQType.A, dns.query().qtype());
        assertEquals(4242, dns.query().transId());

        assertNotNull(dns.response(), "rcode was present in this fixture");
        assertEquals(DnsRcode.NOERROR, dns.response().rcode());
        assertFalse(dns.response().authoritative());
        assertTrue(dns.response().recursionAvailable());
        assertFalse(dns.response().truncated());
        assertEquals(1, dns.response().answerCount());
        assertEquals(299L, dns.response().firstTtlSeconds());

        // BINDING CORRECTIONS #6: dns.log has no is_orig column, so every
        // record is the query the originator sent, and sourceIp is id_orig_h.
        assertEquals("10.0.0.5", dns.sourceIp());
        assertTrue(dns.isOrig());
        assertNull(dns.enrichment(), "enrichment is populated by a later join operator, not this mapper");
    }

    // Mirrors EventMapperTest.mapsLogTypeAndConnectionUidFromTheFixture exactly:
    // both fields are exposed by NetworkEvent's default methods, so no
    // narrowing switch is needed here at all.
    @Test
    void mapsLogTypeAndConnectionUidFromTheFixture() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-a-record.json"), sensor);
        assertTrue(result.isValid());
        NetworkEvent event = result.value();
        assertEquals(LogType.DNS, event.logType());
        assertEquals("Cdns001ABC", event.connectionUid());
    }

    // answers and TTLs are both ABSENT (not merely empty) in this fixture --
    // the sibling case to emptyListFieldsParseAsEmptyNotNull in
    // JsonZeekDnsParserTest. rcode IS present (3, NXDOMAIN), so a response must
    // still be built; answerCount and firstTtlSeconds must default to zero
    // rather than the mapper throwing on a null list.
    @Test
    void answerCountAndFirstTtlSecondsDefaultToZeroWhenAnswersAndTtlsAreAbsent() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("answers-absent.json"), sensor);
        assertTrue(result.isValid());
        DnsEvent dns = switch (result.value()) {
            case DnsEvent d -> d;
            case ConnEvent c -> throw new AssertionError("DnsEventMapper maps dns.log exclusively; got a ConnEvent");
        };
        assertNotNull(dns.response(), "rcode was present, so a response must be built even with no answers");
        assertEquals(DnsRcode.NXDOMAIN, dns.response().rcode());
        assertEquals(0, dns.response().answerCount());
        assertEquals(0L, dns.response().firstTtlSeconds());
    }

    // Required test: TTLs is present but EMPTY ([]) in this fixture, a
    // different wire state from absent (null) -- a naive ttls.get(0) throws on
    // this case where a null-check alone would not catch it (BINDING
    // CORRECTIONS #5).
    @Test
    void firstTtlSecondsIsZeroWhenTtlsIsPresentButEmpty() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("ttls-empty.json"), sensor);
        assertTrue(result.isValid());
        DnsEvent dns = switch (result.value()) {
            case DnsEvent d -> d;
            case ConnEvent c -> throw new AssertionError("DnsEventMapper maps dns.log exclusively; got a ConnEvent");
        };
        assertNotNull(dns.response());
        assertEquals(0, dns.response().answerCount(), "answers was also present-but-empty in this fixture");
        assertEquals(0L, dns.response().firstTtlSeconds());
    }

    // Required test: a blank query must be rejected before it ever reaches
    // DnsQuery's compact constructor, which throws IllegalArgumentException on
    // a blank name. assertDoesNotThrow names the property this protects --
    // that map() itself never lets that throw escape -- rather than merely the
    // rejection outcome.
    @Test
    void blankQueryIsRejectedWithoutThrowing() {
        MappingResult<NetworkEvent> result = assertDoesNotThrow(
            () -> mapper.map(dto("Cdns005", 4242, ""), sensor));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // Required test, BINDING CORRECTIONS #1: id + ":" + transId turns a blank
    // id into ":4242" -- NOT blank -- so EventId.derive's own guard would never
    // fire on it. This must be caught before that concatenation happens.
    @Test
    void blankIdIsRejectedRatherThanDerivingAGarbageIdentity() {
        MappingResult<NetworkEvent> result = mapper.map(dto("", 4242, "example.com"), sensor);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // BINDING CORRECTIONS #3: id_orig_h becomes DnsEvent.sourceIp, which throws
    // IllegalArgumentException on blank -- validated ahead of that constructor
    // for the same reason as every other check in this mapper. Not on the
    // brief's required-test bullet list, but its behavior is (correction #3),
    // so it is covered here rather than left as an untested assumption.
    @Test
    void blankIdOrigHIsRejected() {
        MappingResult<NetworkEvent> result = mapper.map(
            dto("Cdns006", "  ", 4242, "example.com", 1, 0, false, true, false, List.of(), List.of()),
            sensor);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // Required test, BINDING CORRECTIONS #4: qtype ABSENT means "not observed"
    // and must be rejected -- mapping it to DnsQType.OTHER would write -1 into
    // schema index 13, which the frozen dns-feature-v1 contract marks REQUIRED,
    // corrupting a required feature rather than merely defaulting one. A
    // PRESENT-but-unrecognised qtype is a different case that DOES map to
    // OTHER correctly; that path belongs to DnsQType.fromCode and is exercised
    // by DnsEventTest, not re-tested here.
    @Test
    void absentQtypeIsRejectedNotMappedToOther() {
        MappingResult<NetworkEvent> result = mapper.map(
            dto("Cdns007", "10.0.0.5", 4242, "example.com", null, 0, false, true, false, List.of(), List.of()),
            sensor);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // Required test, BINDING CORRECTIONS #5: Zeek writes AA/RA/TC as false on
    // EVERY record, reply or not, so their presence proves nothing -- only
    // rcode indicates a response was actually seen. AA=true here is
    // deliberately misleading input: if the mapper checked AA instead of rcode
    // it would fabricate a response for a query that, per rcode's absence,
    // never received one.
    @Test
    void rcodeAbsentWithAaTrueStillYieldsNullResponse() {
        MappingResult<NetworkEvent> result = mapper.map(
            dto("Cdns008", "10.0.0.5", 4242, "example.com", 1, null, true, false, false, null, null),
            sensor);
        assertTrue(result.isValid());
        DnsEvent dns = switch (result.value()) {
            case DnsEvent d -> d;
            case ConnEvent c -> throw new AssertionError("DnsEventMapper maps dns.log exclusively; got a ConnEvent");
        };
        assertNull(dns.response());
    }
}
