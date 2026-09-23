package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ModbusEventMapperTest {
    private final ModbusEventMapper mapper = new ModbusEventMapper();
    private final SensorId sensor = new SensorId("sensor-eu-1");

    // Takes exactly (ts, uid, tid, isOrig) -- the four fields most tests using
    // this helper actually vary -- direction is resolved via is_orig alone
    // (requestResponse left null) so callers can flip REQUEST/RESPONSE with a
    // boolean. Every other field is fixed at a realistic, fully valid value;
    // tests that need to vary something else build a ZeekModbusRecord
    // directly instead of growing this helper's parameter list, mirroring
    // DnsEventMapperTest's dto()/fuller-overload convention.
    private ZeekModbusRecord record(double ts, String uid, int tid, boolean isOrig) {
        return new ZeekModbusRecord(ts, uid, "10.0.0.5", "10.0.0.9", null, null, isOrig, null,
            tid, "1", "READ_HOLDING_REGISTERS", 40001.0, 2.0, true,
            List.of(7.0, 9.0), List.of());
    }

    // Neither is_orig nor request_response set -- the one input direction
    // resolution must reject rather than guess on.
    private ZeekModbusRecord recordWithNoDirectionFields() {
        return new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null, null, null,
            17, "1", "READ_HOLDING_REGISTERS", 40001.0, 2.0, true,
            List.of(), List.of());
    }

    private MappingResult<NetworkEvent> map(ZeekModbusRecord dto) {
        return mapper.map(dto, sensor);
    }

    // Narrows a NetworkEvent to ModbusEvent with an explicit arm per sealed
    // permits member, never a default -- mirrors DnsEventMapperTest's own
    // narrowing switches on this same sealed hierarchy.
    private static ModbusEvent asModbusEvent(NetworkEvent event) {
        return switch (event) {
            case ModbusEvent m -> m;
            case ConnEvent c -> throw new AssertionError("ModbusEventMapper maps modbus_detailed.log exclusively; got a ConnEvent");
            case DnsEvent d -> throw new AssertionError("ModbusEventMapper maps modbus_detailed.log exclusively; got a DnsEvent");
        };
    }

    // -- Required by the task brief --

    // Both carry the same uid and tid; only direction differs. Event identity
    // is sensor:uid:tid:direction:ts_millis, so folding in direction is what
    // keeps a request and its own response from colliding on one identity.
    @Test
    void aRequestAndItsResponseGetDistinctEventIds() {
        MappingResult<NetworkEvent> request = map(record(1758000000.5, "CXY1", 17, true));
        MappingResult<NetworkEvent> response = map(record(1758000000.6, "CXY1", 17, false));
        assertTrue(request.isValid());
        assertTrue(response.isValid());
        assertNotEquals(request.value().eventId(), response.value().eventId());
    }

    // The counter-wrap case: tid is 16 bits and repeats inside one
    // connection, so the millisecond timestamp component is what keeps two
    // same-direction records sharing a tid from colliding.
    @Test
    void twoSameDirectionRecordsSharingATidDifferOnlyByTimestamp() {
        MappingResult<NetworkEvent> first = map(record(1758000000.5, "CXY1", 17, true));
        MappingResult<NetworkEvent> second = map(record(1758000900.5, "CXY1", 17, true));
        assertTrue(first.isValid());
        assertTrue(second.isValid());
        assertNotEquals(first.value().eventId(), second.value().eventId());
    }

    // is_response is a required frozen feature and both entity-key
    // components depend on orientation, so an unresolvable direction is
    // rejected, never guessed (ModbusEvent's own javadoc).
    @Test
    void anUnresolvableDirectionIsRejectedRatherThanGuessed() {
        ZeekModbusRecord noDirection = recordWithNoDirectionFields();
        MappingResult<NetworkEvent> result = map(noDirection);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // Pins the ceiling itself (10_413_792_000, the epoch-second of
    // 2300-01-01T00:00:00Z -- ClickHouse's event_time column is
    // DateTime64(3, 'UTC')) and both sides of the ">=" comparison: at the
    // ceiling is rejected, one second before it is accepted.
    @Test
    void aTimestampAtOrBeyondTheClickHouseCeilingIsRejected() {
        assertEquals(10_413_792_000L, (long) ModbusEventMapper.MAX_VALID_TS_SECONDS);
        MappingResult<NetworkEvent> atCeiling = map(record(10_413_792_000.0, "CXY1", 17, true));
        MappingResult<NetworkEvent> beforeCeiling = map(record(10_413_791_999.0, "CXY1", 17, true));
        assertFalse(atCeiling.isValid());
        assertEquals(ReasonCode.INVALID_TIMESTAMP, atCeiling.reason());
        assertTrue(beforeCeiling.isValid());
    }

    // Pins F1's fix: tsSeconds is the wire ts EXACTLY (same double, no
    // rounding), while envelope().eventTime() is that same ts rounded to the
    // nearest millisecond -- two different values, deliberately, not one
    // value read two ways. A microsecond-resolution ts (as Zeek's JSON writer
    // emits) is what would have exposed the pre-fix bug, where the causal
    // engine read a millisecond-rounded value derived from eventTime instead
    // of this unrounded one.
    @Test
    void tsSecondsIsTheUnroundedWireValueWhileEventTimeIsMillisecondRounded() {
        double microsecondTs = 1758000000.123456;
        MappingResult<NetworkEvent> result = map(record(microsecondTs, "CXY1", 17, true));
        assertTrue(result.isValid());

        ModbusEvent modbus = asModbusEvent(result.value());
        assertEquals(microsecondTs, modbus.tsSeconds(), "tsSeconds must be the exact wire double, not rounded");
        assertEquals(Instant.ofEpochMilli(Math.round(microsecondTs * 1000.0)), modbus.eventTime(),
            "eventTime stays millisecond-rounded for the event id and the ClickHouse DateTime64(3) column");
    }

    // -- Endpoint orientation: connection-level wire input vs. per-packet ModbusEvent --

    // THE regression test for the endpoint-orientation defect, built from the
    // wire shape this platform actually receives and driven through the real
    // parser, the real mapper and the real ModbusEntityKey -- nothing
    // hand-built in between.
    //
    // Zeek's id_orig_h/id_resp_h are CONNECTION-level: they name the
    // connection's originator and responder, so a request and its own
    // response carry the SAME pair. The earlier tests all built a response
    // whose endpoints were already swapped (per-packet source/destination),
    // which is the one shape this platform's wire never sends, so they could
    // not see that a mapper passing id_orig_h/id_resp_h straight through --
    // followed by ModbusEntityKey's per-packet swap for a response -- lands
    // a response in a different key from its request. Two records of one
    // transaction in two different keys never share causal state, so every
    // stateful feature would be computed on split half-streams.
    @Test
    void aRequestAndItsResponseFromTheRealWireShareOneEntityKey() {
        // One transaction as the sensor emits it: identical uid, tid and
        // connection-level id_orig_h/id_resp_h on both records; only the
        // direction fields (and the timestamp) differ.
        String requestJson = "{\"ts\":1758000000.5,\"uid\":\"CXY1\","
            + "\"id_orig_h\":\"10.0.0.5\",\"id_orig_p\":50001,\"id_resp_h\":\"10.0.0.9\",\"id_resp_p\":502,"
            + "\"is_orig\":true,\"request_response\":\"REQUEST\",\"tid\":17,\"unit\":\"1\","
            + "\"func\":\"READ_HOLDING_REGISTERS\",\"address\":40001,\"quantity\":2,"
            + "\"request_values\":[],\"response_values\":[]}";
        String responseJson = "{\"ts\":1758000000.6,\"uid\":\"CXY1\","
            + "\"id_orig_h\":\"10.0.0.5\",\"id_orig_p\":50001,\"id_resp_h\":\"10.0.0.9\",\"id_resp_p\":502,"
            + "\"is_orig\":false,\"request_response\":\"RESPONSE\",\"tid\":17,\"unit\":\"1\","
            + "\"func\":\"READ_HOLDING_REGISTERS\",\"address\":40001,\"quantity\":2,\"matched\":true,"
            + "\"request_values\":[],\"response_values\":[7,9]}";

        // Real parser, then real mapper, for both records.
        ModbusEvent request = parseAndMap(requestJson);
        ModbusEvent response = parseAndMap(responseJson);

        // Real key. The headline property: one transaction, one key.
        ModbusEntityKey requestKey = ModbusEntityKey.of(request);
        ModbusEntityKey responseKey = ModbusEntityKey.of(response);
        assertEquals(requestKey, responseKey,
            "a request and its response carrying identical connection-level id_orig_h/id_resp_h "
                + "must land in the same ModbusEntityKey");

        // And it is the upstream's own key for connection-level input: the
        // connection's originator as client and responder as server, in both
        // directions -- never the reverse.
        assertEquals("10.0.0.5", responseKey.clientIp());
        assertEquals("10.0.0.9", responseKey.serverIp());
    }

    // Connection-level input, both directions: a REQUEST travels from the
    // connection's originator to its responder, a RESPONSE travels back, so
    // the mapper orients the SAME (orig, resp) pair into two different
    // per-packet pairs.
    @Test
    void aConnectionLevelPairIsOrientedIntoPerPacketFormByDirection() {
        // orig 10.0.0.5, resp 10.0.0.9 on both records; no per-packet pair.
        ZeekModbusRecord requestDto = new ZeekModbusRecord(1758000000.5, "CXY1",
            "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        ZeekModbusRecord responseDto = new ZeekModbusRecord(1758000000.6, "CXY1",
            "10.0.0.5", "10.0.0.9", null, null,
            false, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());

        ModbusEvent request = mapToModbusEvent(requestDto);
        ModbusEvent response = mapToModbusEvent(responseDto);

        // REQUEST: source = orig, destination = resp.
        assertEquals("10.0.0.5", request.sourceIp());
        assertEquals("10.0.0.9", request.destinationIp());
        // RESPONSE: source = resp, destination = orig.
        assertEquals("10.0.0.9", response.sourceIp());
        assertEquals("10.0.0.5", response.destinationIp());
    }

    // Per-packet input (source_h/destination_h) is already in the form
    // ModbusEvent carries, so it passes through unchanged in BOTH directions
    // -- the response's pair arrives already reversed and must stay so.
    // Driven through the real parser so the per-packet JSON spellings are
    // proven to reach the per-packet fields.
    @Test
    void aPerPacketPairPassesThroughUnchangedInBothDirections() {
        // The same transaction as per-packet fields: the request is sent
        // 10.0.0.5 -> 10.0.0.9, the response 10.0.0.9 -> 10.0.0.5.
        ModbusEvent request = parseAndMap("{\"ts\":1758000000.5,\"uid\":\"CXY1\","
            + "\"source_h\":\"10.0.0.5\",\"destination_h\":\"10.0.0.9\","
            + "\"is_orig\":true,\"tid\":17,\"unit\":\"1\",\"func\":3}");
        ModbusEvent response = parseAndMap("{\"ts\":1758000000.6,\"uid\":\"CXY1\","
            + "\"source_h\":\"10.0.0.9\",\"destination_h\":\"10.0.0.5\","
            + "\"is_orig\":false,\"tid\":17,\"unit\":\"1\",\"func\":3}");

        // Unchanged on both records.
        assertEquals("10.0.0.5", request.sourceIp());
        assertEquals("10.0.0.9", request.destinationIp());
        assertEquals("10.0.0.9", response.sourceIp());
        assertEquals("10.0.0.5", response.destinationIp());

        // And the per-packet route keys the pair together too.
        assertEquals(ModbusEntityKey.of(request), ModbusEntityKey.of(response));
    }

    // When a record carries BOTH complete pairs, the connection-level one
    // wins, as the upstream prefers it. The per-packet pair here is
    // deliberately inconsistent with the connection-level one (different
    // hosts entirely) so the assertion can only pass if the connection-level
    // pair was the one used.
    @Test
    void theConnectionLevelPairWinsWhenBothPairsArePresent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.6, "CXY1",
            "10.0.0.5", "10.0.0.9", "192.168.1.1", "192.168.1.2",
            false, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());

        ModbusEvent response = mapToModbusEvent(dto);

        // Oriented from (orig, resp) for a RESPONSE; source_h/destination_h ignored.
        assertEquals("10.0.0.9", response.sourceIp());
        assertEquals("10.0.0.5", response.destinationIp());
    }

    // An incomplete connection-level pair (orig only) does not win: the
    // record falls through to its complete per-packet pair rather than
    // mixing one half of each kind.
    @Test
    void anIncompleteConnectionLevelPairFallsThroughToThePerPacketPair() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.6, "CXY1",
            "10.0.0.5", null, "10.0.0.9", "10.0.0.5",
            false, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());

        ModbusEvent response = mapToModbusEvent(dto);

        // The per-packet pair, unchanged.
        assertEquals("10.0.0.9", response.sourceIp());
        assertEquals("10.0.0.5", response.destinationIp());
    }

    // Neither pair present: rejected, never defaulted -- a record with no
    // endpoints cannot be keyed, so it cannot be scored.
    @Test
    void aRecordWithNeitherEndpointPairIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1",
            null, null, null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());

        MappingResult<NetworkEvent> result = map(dto);

        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // -- Additional coverage, mirroring DnsEventMapperTest's thoroughness --

    // uid must be checked blank BEFORE it is folded into the identity string
    // below -- a blank uid concatenated with ":17:REQUEST:..." is still
    // non-blank, so EventId.derive's own guard would never fire.
    @Test
    void blankUidIsRejectedRatherThanDerivingAGarbageIdentity() {
        MappingResult<NetworkEvent> result = map(record(1758000000.5, "", 17, true));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    @Test
    void aNegativeTimestampIsRejectedAsInvalidTimestamp() {
        MappingResult<NetworkEvent> result = map(record(-1.0, "CXY1", 17, true));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_TIMESTAMP, result.reason());
    }

    @Test
    void aNonFiniteTimestampIsRejectedAsInvalidTimestamp() {
        MappingResult<NetworkEvent> nan = map(record(Double.NaN, "CXY1", 17, true));
        MappingResult<NetworkEvent> infinite = map(record(Double.POSITIVE_INFINITY, "CXY1", 17, true));
        assertFalse(nan.isValid());
        assertEquals(ReasonCode.INVALID_TIMESTAMP, nan.reason());
        assertFalse(infinite.isValid());
        assertEquals(ReasonCode.INVALID_TIMESTAMP, infinite.reason());
    }

    // ModbusEvent's compact constructor throws IllegalArgumentException on a
    // blank sourceIp/destinationIp -- validated ahead of that call, same
    // crash-loop reasoning as every other check in this mapper. A blank half
    // makes the connection-level pair incomplete, and with no per-packet pair
    // to fall back to the record has no endpoints at all.
    @Test
    void aBlankOrigHostWithNoPerPacketPairIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "  ", "10.0.0.9", null, null, true, null,
            17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    @Test
    void anAbsentRespHostWithNoPerPacketPairIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", null, null, null, true, null,
            17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // func is required; the upstream engine hard-fails on a missing or
    // unparseable one too.
    @Test
    void anUnresolvableFunctionCodeIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null, true, null,
            17, "1", "NOT_A_REAL_FUNCTION", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // request_response is the PRIMARY source and must win over is_orig when
    // both are present and would otherwise disagree.
    @Test
    void requestResponseWinsOverIsOrigWhenBothArePresent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            false, "REQUEST", 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals(ModbusEvent.ModbusDirection.REQUEST, asModbusEvent(result.value()).direction());
    }

    // is_orig is read only when request_response is ABSENT.
    @Test
    void isOrigIsUsedOnlyWhenRequestResponseIsAbsent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            false, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals(ModbusEvent.ModbusDirection.RESPONSE, asModbusEvent(result.value()).direction());
    }

    // Design doc section 5: "the engine lowercases and trims, then
    // hard-fails on anything that is not request/response" -- a PRESENT but
    // unparseable request_response is a rejection, never a silent
    // fall-through to is_orig, even when is_orig would otherwise resolve.
    @Test
    void aGarbageRequestResponseIsRejectedRatherThanFallingBackToIsOrig() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, "SIDEWAYS", 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // request_response's comparison is case-insensitive after trimming, per
    // the design doc's "lowercases and trims" rule.
    @Test
    void requestResponseIsCaseInsensitiveAndTrimmed() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            null, "  Response  ", 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals(ModbusEvent.ModbusDirection.RESPONSE, asModbusEvent(result.value()).direction());
    }

    // The upstream engine's own absent-unit sentinel, not null -- see
    // ModbusEventMapper.ABSENT_UNIT_ID's comment.
    @Test
    void anAbsentUnitIdDefaultsToTheNaSentinel() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, null, "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals("NA", asModbusEvent(result.value()).unitId());
    }

    // matched is meaningful only on a response record; a null wire value
    // must default to false rather than being rejected.
    @Test
    void aNullMatchedDefaultsToFalse() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertFalse(asModbusEvent(result.value()).matched());
    }

    // A null element inside request_values/response_values (a valid JSON
    // array shape: "[7, null, 9]") must reject the record, not default to
    // 0.0 and not throw a NullPointerException out of map() when unboxed.
    // The authoritative engine's parse_numeric_vector raises TypeError from
    // float(None) and process_capture lets that abort the whole record --
    // 0.0 is a plausible real register value, so defaulting would have
    // silently corrupted request_value_min/max/mean instead of failing (see
    // ModbusEventMapper.toValidatedArray's own comment).
    @Test
    void aNullElementInRequestValuesIsRejectedRatherThanDefaultedOrThrown() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null,
            Arrays.asList(7.0, null, 9.0), List.of());
        MappingResult<NetworkEvent> result = assertDoesNotThrow(() -> map(dto));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_COUNTER, result.reason());
        assertTrue(result.detail().contains("request_values[1]"),
            () -> "detail should name the field and offending index, was: " + result.detail());
    }

    // A non-finite element must also reject, built the way production
    // actually would receive it: a JSON literal (1e400) overflowing double
    // to Double.POSITIVE_INFINITY through the REAL parser, not a DTO
    // hand-built with Double.POSITIVE_INFINITY -- proving this path is
    // reachable from an actual Kafka record, mirroring
    // DnsEventMapperTest.aPositiveInfiniteTimestampFromTheRealParserIsRejectedAsInvalidTimestamp.
    @Test
    void aNonFiniteRequestValueFromTheRealParserIsRejected() {
        String json = "{\"ts\":1758000000.5,\"uid\":\"CXY1\",\"id_orig_h\":\"10.0.0.5\","
            + "\"id_resp_h\":\"10.0.0.9\",\"is_orig\":true,\"tid\":17,\"unit\":\"1\","
            + "\"func\":\"READ_HOLDING_REGISTERS\",\"request_values\":[1e400]}";
        MappingResult<ZeekModbusRecord> parsed =
            new JsonZeekModbusParser().parse(json.getBytes(StandardCharsets.UTF_8));
        assertTrue(parsed.isValid(), () -> "parser rejected the fixture: " + parsed);
        assertTrue(Double.isInfinite(parsed.value().requestValues().get(0)),
            "1e400 must overflow to POSITIVE_INFINITY, proving this element is reachable from real JSON");

        MappingResult<NetworkEvent> result = map(parsed.value());

        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_COUNTER, result.reason());
        assertTrue(result.detail().contains("request_values[0]"),
            () -> "detail should name the field and offending index, was: " + result.detail());
    }

    // An ordinary array of finite values must still map successfully --
    // this fix must not reject valid data, only null/non-finite elements.
    @Test
    void anOrdinaryFiniteValueArrayStillMapsSuccessfully() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null,
            List.of(7.0, 9.0, -3.5), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid(), () -> "unexpected rejection: " + describe(result));
        assertArrayEquals(new double[] {7.0, 9.0, -3.5}, asModbusEvent(result.value()).requestValues());
    }

    // An empty array and an absent field both still map to an empty
    // double[] -- unchanged by this fix, and correct: the authoritative
    // engine's parse_numeric_vector also returns [] for both None and [].
    @Test
    void anEmptyArrayAndAnAbsentFieldBothMapToAnEmptyDoubleArray() {
        ZeekModbusRecord emptyArray = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        ZeekModbusRecord absentField = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, null, null);

        MappingResult<NetworkEvent> emptyResult = map(emptyArray);
        MappingResult<NetworkEvent> absentResult = map(absentField);

        assertTrue(emptyResult.isValid());
        assertTrue(absentResult.isValid());
        assertArrayEquals(new double[0], asModbusEvent(emptyResult.value()).requestValues());
        assertArrayEquals(new double[0], asModbusEvent(absentResult.value()).requestValues());
    }

    // Happy path, narrowed to ModbusEvent so every field can be checked at
    // once: identity, timing, sourceIp/destinationIp, transactionId as tid's
    // String form, and address/quantity/matched/the value arrays passed
    // through as-is.
    //
    // The endpoints here are the CONNECTION-level pair (orig 10.0.0.5, resp
    // 10.0.0.9 -- the id_orig_h/id_resp_h fields) on a RESPONSE, so the
    // mapper orients them into per-packet form: the response travels from
    // the responder back to the originator, making sourceIp 10.0.0.9 and
    // destinationIp 10.0.0.5. This test used to expect the pair passed
    // through unchanged (sourceIp 10.0.0.5), which is exactly the defect:
    // ModbusEntityKey then swapped it again and keyed the response apart from
    // its request.
    @Test
    void mapsAFullyPopulatedResponseRecordToAModbusEvent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            false, null, 17, "3", "READ_HOLDING_REGISTERS", 40001.0, 2.0, true,
            List.of(), List.of(11.0, 22.0));
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid(), () -> "unexpected rejection: " + describe(result));

        NetworkEvent event = result.value();
        assertEquals(LogType.MODBUS, event.logType());
        assertEquals("CXY1", event.connectionUid());
        assertEquals(Instant.ofEpochMilli(1758000000500L), event.eventTime());
        assertEquals("sensor-eu-1:CXY1:17:RESPONSE:1758000000500", event.eventId().value());

        ModbusEvent modbus = asModbusEvent(event);
        assertEquals(ModbusEvent.ModbusDirection.RESPONSE, modbus.direction());
        assertEquals("10.0.0.9", modbus.sourceIp());
        assertEquals("10.0.0.5", modbus.destinationIp());
        assertEquals(3, modbus.functionCode());
        assertEquals("17", modbus.transactionId());
        assertEquals("3", modbus.unitId());
        assertEquals(40001.0, modbus.address());
        assertEquals(2.0, modbus.quantity());
        assertTrue(modbus.matched());
        assertArrayEquals(new double[0], modbus.requestValues());
        assertArrayEquals(new double[] {11.0, 22.0}, modbus.responseValues());
    }

    // address/quantity are boxed Double specifically so their absence stays
    // representable -- a genuinely absent field must pass through as null,
    // not default to 0.0 (that would collide with a real address/quantity of
    // zero).
    @Test
    void absentAddressAndQuantityStayNullRatherThanDefaultingToZero() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        ModbusEvent modbus = asModbusEvent(result.value());
        assertNull(modbus.address());
        assertNull(modbus.quantity());
    }

    // Pins the ceiling constant's exact value, mirroring
    // DnsEventMapperTest.maxValidTsSecondsIsTheEpochSecondOfTheFirstInstantOutsideClickHousesDateTime64Range.
    @Test
    void maxValidTsSecondsIsTheEpochSecondOfTheFirstInstantOutsideClickHousesDateTime64Range() {
        assertEquals(10_413_792_000.0, ModbusEventMapper.MAX_VALID_TS_SECONDS,
            "must equal Instant.parse(\"2300-01-01T00:00:00Z\").getEpochSecond()");
    }

    // Maps a DTO and narrows the result, failing the test (rather than
    // returning a rejection) if the mapper refuses it -- the orientation
    // tests need a mapped ModbusEvent to inspect, so a rejection there is
    // itself the failure.
    private ModbusEvent mapToModbusEvent(ZeekModbusRecord dto) {
        MappingResult<NetworkEvent> mapped = map(dto);
        assertTrue(mapped.isValid(), () -> "mapper rejected the fixture: " + describe(mapped));
        return asModbusEvent(mapped.value());
    }

    // Same as mapToModbusEvent, but starting from a raw JSON line run through
    // the real JsonZeekModbusParser first, so the wire spellings themselves
    // are exercised, not just the DTO's positional constructor.
    private ModbusEvent parseAndMap(String json) {
        MappingResult<ZeekModbusRecord> parsed =
            new JsonZeekModbusParser().parse(json.getBytes(StandardCharsets.UTF_8));
        assertTrue(parsed.isValid(), () -> "parser rejected the fixture: " + parsed.reason() + ": " + parsed.detail());
        return mapToModbusEvent(parsed.value());
    }

    private static String describe(MappingResult<NetworkEvent> result) {
        return result.isValid() ? "valid" : result.reason() + ": " + result.detail();
    }
}
