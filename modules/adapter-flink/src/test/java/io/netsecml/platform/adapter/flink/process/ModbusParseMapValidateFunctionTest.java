package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

// modbus_detailed.log's half of the shared parse-map-validate body, mirroring
// DnsParseMapValidateFunctionTest's structure (happy path, a parse-stage DLQ
// rejection, a map-stage DLQ rejection, and the deriveEventId blank-guard) so
// the abstract ParseMapValidateFunction body is proven to behave the same
// regardless of which subclass drives it. Records are inline JSON, mirroring
// JsonZeekModbusParserTest/ModbusEventMapperTest's own convention -- unlike
// dns and conn, modbus has no committed fixture file directory (tests/fixtures
// has zeek_conn/ and zeek_dns/ but no zeek_modbus/) to read from.
class ModbusParseMapValidateFunctionTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // A well-formed modbus_detailed.log request record: every required field
    // present and valid, so it must parse, map, and reach main output.
    @Test
    void validModbusRequestReachesMainOutputWithDerivedEventId() throws Exception {
        String json = "{\"ts\":1758000000.5,\"uid\":\"CXY1\",\"id_orig_h\":\"10.0.0.5\","
            + "\"id_resp_h\":\"10.0.0.9\",\"is_orig\":true,\"tid\":17,\"unit\":\"1\","
            + "\"func\":\"READ_HOLDING_REGISTERS\",\"address\":40001,\"quantity\":2,"
            + "\"matched\":true,\"request_values\":[7,9],\"response_values\":[]}";

        ModbusParseMapValidateFunction function = new ModbusParseMapValidateFunction(SENSOR);
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(json.getBytes(StandardCharsets.UTF_8)));

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        // sensor:uid:tid:direction:ts_millis, per ModbusEventMapper -- proves the
        // full composite identity is what actually reaches main output, not just
        // "some non-blank id".
        assertEquals("sensor-eu-1:CXY1:17:REQUEST:1758000000500", output.get(0).eventId().value());

        harness.close();
    }

    // A parse-stage failure never produced a ZeekModbusRecord, so there is
    // nothing to derive an identity from -- same reasoning as conn's
    // parseStageRejectionCarriesNoEventId and dns's own copy, just against
    // JsonZeekModbusParser.
    @Test
    void malformedModbusPayloadGoesToRejectedSideOutputWithReasonCodeAndNoEventId() throws Exception {
        byte[] malformed = "{not json at all".getBytes(StandardCharsets.UTF_8);

        ModbusParseMapValidateFunction function = new ModbusParseMapValidateFunction(SENSOR);
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(malformed));

        assertEquals(0, harness.extractOutputValues().size(), "malformed record must not reach main output");
        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.MALFORMED_JSON, record.reason());
        assertEquals(ReasonCode.Stage.PARSE, record.reason().stage());
        assertEquals("", record.eventId());

        harness.close();
    }

    // The poison record: every Jackson-required field is present (ts, uid,
    // tid, func), so this parses cleanly and reaches ModbusEventMapper, where
    // an unresolvable direction (neither request_response nor is_orig present)
    // fails the map-stage check. The property under test is that the subtask
    // survives -- the valid record right behind it still reaches main output --
    // mirroring dns's blankQueryMapStageRejectionDoesNotKillTheSubtask and
    // conn's mapStageRejectionWithBlankIdCarriesNoEventIdAndDoesNotKillTheSubtask.
    @Test
    void unresolvableDirectionMapStageRejectionDoesNotKillTheSubtask() throws Exception {
        byte[] noDirection = "{\"ts\":1758000000.5,\"uid\":\"CXY1\",\"tid\":17,\"func\":3}"
            .getBytes(StandardCharsets.UTF_8);
        byte[] valid = ("{\"ts\":1758000001.0,\"uid\":\"CXY2\",\"id_orig_h\":\"10.0.0.5\","
            + "\"id_resp_h\":\"10.0.0.9\",\"is_orig\":true,\"tid\":18,\"unit\":\"1\","
            + "\"func\":3}").getBytes(StandardCharsets.UTF_8);

        ModbusParseMapValidateFunction function = new ModbusParseMapValidateFunction(SENSOR);
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(noDirection));
        harness.processElement(new StreamRecord<>(valid));

        assertEquals(1, harness.extractOutputValues().size(),
            "the poison record must not kill the subtask; the following valid record still reaches main output");
        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, record.reason());
        assertEquals(ReasonCode.Stage.MAP, record.reason().stage(),
            "MISSING_REQUIRED_FIELD is ModbusEventMapper's code, never the parser's -- it must never report PARSE");
        // uid and tid are both present, so deriveEventId still derives a
        // correlation id here -- this rejection is not the blank-uid case.
        assertEquals("sensor-eu-1:CXY1:17", record.eventId());

        harness.close();
    }

    // ModbusParseMapValidateFunction.deriveEventId must check uid blank
    // BEFORE combining it with tid: "" + ":" + 17 is ":17", which is NOT
    // blank, so EventId.derive's own guard would never fire and
    // "sensor-eu-1::17" would be written to the DLQ as if it were a real
    // identity. tid is deliberately non-zero here so that bug, if
    // reintroduced, would produce a visibly non-empty eventId instead of
    // coincidentally matching "".
    @Test
    void mapStageRejectionWithBlankUidCarriesNoEventId() throws Exception {
        byte[] blankUid = "{\"ts\":1758000000.5,\"uid\":\"\",\"tid\":17,\"func\":3}"
            .getBytes(StandardCharsets.UTF_8);

        ModbusParseMapValidateFunction function = new ModbusParseMapValidateFunction(SENSOR);
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(blankUid));

        assertEquals(0, harness.extractOutputValues().size());
        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, record.reason());
        assertEquals(ReasonCode.Stage.MAP, record.reason().stage());
        assertEquals("", record.eventId(), "blank uid must never combine with tid into \"sensor-eu-1::17\"");

        harness.close();
    }
}
