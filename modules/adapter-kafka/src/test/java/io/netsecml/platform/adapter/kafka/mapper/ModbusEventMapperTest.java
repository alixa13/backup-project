package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.domain.event.*;
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
        return new ZeekModbusRecord(ts, uid, "10.0.0.5", "10.0.0.9", isOrig, null,
            tid, "1", "READ_HOLDING_REGISTERS", 40001.0, 2.0, true,
            List.of(7.0, 9.0), List.of());
    }

    // Neither is_orig nor request_response set -- the one input direction
    // resolution must reject rather than guess on.
    private ZeekModbusRecord recordWithNoDirectionFields() {
        return new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
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
    // crash-loop reasoning as every other check in this mapper.
    @Test
    void blankSourceHostIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "  ", "10.0.0.9", true, null,
            17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    @Test
    void blankDestinationHostIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", null, true, null,
            17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // func is required; the upstream engine hard-fails on a missing or
    // unparseable one too.
    @Test
    void anUnresolvableFunctionCodeIsRejected() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", true, null,
            17, "1", "NOT_A_REAL_FUNCTION", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // request_response is the PRIMARY source and must win over is_orig when
    // both are present and would otherwise disagree.
    @Test
    void requestResponseWinsOverIsOrigWhenBothArePresent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
            false, "REQUEST", 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals(ModbusEvent.ModbusDirection.REQUEST, asModbusEvent(result.value()).direction());
    }

    // is_orig is read only when request_response is ABSENT.
    @Test
    void isOrigIsUsedOnlyWhenRequestResponseIsAbsent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
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
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
            true, "SIDEWAYS", 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    // request_response's comparison is case-insensitive after trimming, per
    // the design doc's "lowercases and trims" rule.
    @Test
    void requestResponseIsCaseInsensitiveAndTrimmed() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
            null, "  Response  ", 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals(ModbusEvent.ModbusDirection.RESPONSE, asModbusEvent(result.value()).direction());
    }

    // The upstream engine's own absent-unit sentinel, not null -- see
    // ModbusEventMapper.ABSENT_UNIT_ID's comment.
    @Test
    void anAbsentUnitIdDefaultsToTheNaSentinel() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
            true, null, 17, null, "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        MappingResult<NetworkEvent> result = map(dto);
        assertTrue(result.isValid());
        assertEquals("NA", asModbusEvent(result.value()).unitId());
    }

    // matched is meaningful only on a response record; a null wire value
    // must default to false rather than being rejected.
    @Test
    void aNullMatchedDefaultsToFalse() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
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
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
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
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
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
        ZeekModbusRecord emptyArray = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, List.of(), List.of());
        ZeekModbusRecord absentField = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
            true, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, null, null);

        MappingResult<NetworkEvent> emptyResult = map(emptyArray);
        MappingResult<NetworkEvent> absentResult = map(absentField);

        assertTrue(emptyResult.isValid());
        assertTrue(absentResult.isValid());
        assertArrayEquals(new double[0], asModbusEvent(emptyResult.value()).requestValues());
        assertArrayEquals(new double[0], asModbusEvent(absentResult.value()).requestValues());
    }

    // Happy path, narrowed to ModbusEvent so every field can be checked at
    // once: identity, timing, sourceIp/destinationIp (NOT swapped by
    // direction -- that normalization is ModbusEntityKey's job, a later
    // task, per ModbusEvent's own javadoc), transactionId as tid's String
    // form, and address/quantity/matched/the value arrays passed through
    // as-is.
    @Test
    void mapsAFullyPopulatedResponseRecordToAModbusEvent() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
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
        assertEquals("10.0.0.5", modbus.sourceIp());
        assertEquals("10.0.0.9", modbus.destinationIp());
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
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9",
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

    private static String describe(MappingResult<NetworkEvent> result) {
        return result.isValid() ? "valid" : result.reason() + ": " + result.detail();
    }
}
