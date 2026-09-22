package io.netsecml.platform.adapter.kafka.parser;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class JsonZeekModbusParserTest {
    private final JsonZeekModbusParser parser = new JsonZeekModbusParser();

    @Test
    void aWellFormedModbusDetailedLineParses() {
        String json = """
            {"ts":1758000000.5,"uid":"CXY1","id.orig_h":"10.0.0.5","id.orig_p":50001,
             "id.resp_h":"10.0.0.9","id.resp_p":502,"is_orig":true,"tid":17,"unit":1,
             "func":"READ_HOLDING_REGISTERS","address":40001,"quantity":2,
             "matched":true,"request_values":[7,9],"response_values":[]}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertTrue(result.isValid(), () -> "unexpected rejection: " + describe(result));
        assertEquals(17, result.value().tid());
        assertEquals(2, result.value().requestValues().size());
    }

    // required=true only enforces that the KEY is present. func binds as a
    // String (ZeekModbusRecord's own comment explains why), so Jackson's
    // FAIL_ON_NULL_FOR_PRIMITIVES -- which covers only primitive fields --
    // does not by itself stop an explicit "func": null from binding as a
    // null String. JsonZeekModbusParser rejects that null by hand for
    // exactly the reason FAIL_ON_NULL_FOR_PRIMITIVES exists for ts/tid
    // below: a defaulted/absent function code must be a parse failure, not
    // a record silently forwarded for scoring with func == null.
    @Test
    void anExplicitNullFunctionCodeIsRejectedRatherThanBecomingZero() {
        String json = """
            {"ts":1758000000.5,"uid":"CXY1","tid":17,"func":null}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    // The offline engine falls back to ast.literal_eval for research files
    // shaped like this. On the wire that tolerance would accept a malformed
    // payload instead of rejecting it, so this parser requires strict JSON
    // arrays and must not reproduce that fallback.
    @Test
    void aPythonReprValueVectorIsRejectedNotSalvaged() {
        String json = """
            {"ts":1758000000.5,"uid":"CXY1","tid":17,"func":3,"request_values":"[7, 9]"}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    @Test
    void bothEndpointSpellingsBindToTheSameFields() {
        String alternate = """
            {"ts":1758000000.5,"uid":"CXY1","source_h":"10.0.0.5","destination_h":"10.0.0.9",
             "is_orig":true,"tid":17,"uint":3,"func":3}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(alternate.getBytes(StandardCharsets.UTF_8));
        assertTrue(result.isValid(), () -> "unexpected rejection: " + describe(result));
        assertEquals("10.0.0.5", result.value().sourceHost());
        assertEquals("3", result.value().unitId());
    }

    // -- Extra coverage beyond the brief's floor of 4 --

    // ts and tid are the DTO's other two required=true fields, and both are
    // primitives, so this is the test that actually exercises
    // FAIL_ON_NULL_FOR_PRIMITIVES itself: func's null-rejection above is a
    // by-hand check (func is a String), not the flag. Without the flag, an
    // explicit "tid": null would silently bind to 0 -- a plausible-looking
    // transaction id -- exactly as ts:null would silently become epoch 0.
    @Test
    void anExplicitNullTidIsRejectedRatherThanBecomingZero() {
        String json = """
            {"ts":1758000000.5,"uid":"CXY1","tid":null,"func":3}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    // required=true's PRESENCE check, not its nullness check: tid is absent
    // entirely here (not null), so this pins the same PARSE-stage rejection
    // path JsonZeekDnsParserTest.missingTransIdFailsToParse pins for dns --
    // both the reason and the stage matter, since isValid() alone cannot
    // tell a structurally-broken payload apart from one missing a required
    // field, and the archive job's DLQ column depends on which it was.
    @Test
    void missingTidFailsToParseAtParseStage() {
        String json = """
            {"ts":1758000000.5,"uid":"CXY1","func":3}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
        assertEquals(ReasonCode.Stage.PARSE, result.reason().stage());
    }

    // modbus_detailed.log carries several columns this DTO never reads
    // (exception_code, request_data, response_data, request_subfunction_code,
    // response_subfunction_code, mei_type, modbus_detailed_link_id -- see
    // contracts/source/zeek-modbus-source-v1.json's excludedRawFields).
    // Without @JsonIgnoreProperties(ignoreUnknown = true) every real
    // modbus_detailed.log record would throw UnrecognizedPropertyException
    // and route to the DLQ as MALFORMED_JSON -- the same production-shaped
    // gap JsonZeekDnsParserTest.toleratesRejectedAndTheOtherColumnsZeekAlwaysEmits
    // pins for dns's "rejected"/"rtt"/"qclass"/"Z" columns.
    @Test
    void toleratesRawColumnsThisDtoDoesNotModel() {
        String json = """
            {"ts":1758000000.5,"uid":"CXY1","tid":17,"func":3,
             "exception_code":0,"request_data":"AB12","response_data":"",
             "request_subfunction_code":0,"response_subfunction_code":0,
             "mei_type":0,"modbus_detailed_link_id":"L1"}
            """;
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertTrue(result.isValid(), () -> "rejected a real modbus_detailed.log record: " + describe(result));
        assertEquals(17, result.value().tid());
    }

    // General structural-garbage case, mirroring
    // JsonZeekDnsParserTest.rejectsMalformedJson: catch(Exception) inside
    // JsonZeekModbusParser.parse must turn ANY Jackson failure into
    // MALFORMED_JSON, not just the required-field and null-primitive shapes
    // the other tests target individually.
    @Test
    void rejectsStructurallyMalformedJson() {
        String json = "{not json at all";
        MappingResult<ZeekModbusRecord> result = parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    private static String describe(MappingResult<ZeekModbusRecord> result) {
        return result.isValid() ? "valid" : result.reason() + ": " + result.detail();
    }
}
