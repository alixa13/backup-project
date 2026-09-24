package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

// The s7comm parse stage in Flink: valid records reach the main output as
// S7commEvent; every rejection reaches the side output with its reason and
// a correlation id, and never stops the next record.
class S7commParseMapValidateFunctionTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    private static final String VALID = "{\"ts\":1790000000.5,\"uid\":\"CS7P\",\"source_h\":\"10.0.0.5\","
        + "\"source_p\":50001,\"destination_h\":\"10.0.0.9\",\"destination_p\":102,\"rosctr_code\":1,"
        + "\"pdu_reference\":7,\"function_code\":4}";

    private static OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness() throws Exception {
        return ProcessFunctionTestHarnesses.forProcessFunction(new S7commParseMapValidateFunction(SENSOR));
    }

    private static StreamRecord<byte[]> record(String json) {
        return new StreamRecord<>(json.getBytes(StandardCharsets.UTF_8));
    }

    private static RejectedRecord onlyRejection(OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness) {
        List<StreamRecord<RejectedRecord>> rejected =
            List.copyOf(harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG));
        assertEquals(1, rejected.size());
        return rejected.get(0).getValue();
    }

    @Test
    void aValidRecordReachesTheMainOutputAsAnS7commEvent() throws Exception {
        var harness = harness();
        harness.processElement(record(VALID));
        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        S7commEvent event = assertInstanceOf(S7commEvent.class, output.get(0));
        assertEquals("sensor-eu-1:CS7P:7:REQUEST:1790000000500", event.eventId().value());
        harness.close();
    }

    @Test
    void aMalformedPayloadIsAParseRejectionWithNoEventId() throws Exception {
        var harness = harness();
        harness.processElement(record("{not json"));
        RejectedRecord rejection = onlyRejection(harness);
        assertEquals(ReasonCode.MALFORMED_JSON, rejection.reason());
        assertEquals(ReasonCode.Stage.PARSE, rejection.reason().stage());
        assertEquals("", rejection.eventId());
        harness.close();
    }

    @Test
    void aMapRejectionCarriesTheUidAndPduReferenceAsItsCorrelationId() throws Exception {
        var harness = harness();
        harness.processElement(record(VALID.replace("\"destination_p\":102,", "")));
        RejectedRecord rejection = onlyRejection(harness);
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, rejection.reason());
        assertEquals("sensor-eu-1:CS7P:7", rejection.eventId());
        harness.close();
    }

    @Test
    void aMapRejectionWithoutAPduReferenceCarriesTheUidAlone() throws Exception {
        var harness = harness();
        harness.processElement(record(VALID.replace("\"pdu_reference\":7,", "")));
        assertEquals("sensor-eu-1:CS7P", onlyRejection(harness).eventId());
        harness.close();
    }

    @Test
    void aRejectionDoesNotStopTheNextRecord() throws Exception {
        var harness = harness();
        harness.processElement(record(VALID.replace("\"uid\":\"CS7P\",", "")));
        harness.processElement(record(VALID));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals("", onlyRejection(harness).eventId(), "no uid, so no correlation id");
        harness.close();
    }
}
