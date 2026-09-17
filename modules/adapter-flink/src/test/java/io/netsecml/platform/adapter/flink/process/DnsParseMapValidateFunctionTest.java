package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// dns.log's half of the shared parse-map-validate body. Every case here mirrors
// a ParseMapValidateFunctionTest (now ConnParseMapValidateFunctionTest) case,
// proving the abstract body behaves the same regardless of which subclass is
// driving it -- with one addition: the happy path uses a Zeek-SHAPED fixture
// (valid-zeek-shaped.json), not a DTO-shaped one, because that is precisely the
// distinction that let commit 41ef087's bug hide until now.
class DnsParseMapValidateFunctionTest {
    private byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_dns", name));
    }

    // valid-zeek-shaped.json carries rejected/rtt/qclass/Z alongside the fields
    // the DTO declares -- the four columns every real dns.log record carries
    // that no other committed fixture includes. Reaching main output at all
    // proves @JsonIgnoreProperties survives the Flink function, not just the
    // parser's own unit test; the event id proves the uid+trans_id composite
    // (spec section 5) is what actually gets derived on the success path.
    @Test
    void validZeekShapedRecordReachesMainOutputWithDerivedEventId() throws Exception {
        DnsParseMapValidateFunction function = new DnsParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("valid-zeek-shaped.json")));

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals("sensor-eu-1:Cdns005ZEK:4242", output.get(0).eventId().value());

        harness.close();
    }

    // A parse-stage failure never produced a ZeekDnsEvent, so there is nothing
    // to derive an identity from -- same reasoning as conn's
    // parseStageRejectionCarriesNoEventId, just against JsonZeekDnsParser.
    @Test
    void malformedDnsPayloadGoesToRejectedSideOutputWithReasonCodeAndNoEventId() throws Exception {
        DnsParseMapValidateFunction function = new DnsParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));

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

    // The poison record: every Jackson-required field is present, including a
    // non-blank "id", so this parses cleanly and reaches DnsEventMapper, where
    // an empty "query" fails the map-stage required-field check. The property
    // under test is that the subtask survives -- the valid record right behind
    // it still reaches main output -- mirroring conn's
    // mapStageRejectionWithBlankIdCarriesNoEventIdAndDoesNotKillTheSubtask.
    @Test
    void blankQueryMapStageRejectionDoesNotKillTheSubtask() throws Exception {
        byte[] blankQuery = ("{ \"id\": \"Cdns777POI\", \"ts\": 1786608150.0, \"id_orig_h\": \"10.0.0.5\", "
            + "\"id_orig_p\": 53425, \"id_resp_h\": \"8.8.8.8\", \"id_resp_p\": 53, "
            + "\"trans_id\": 4243, \"query\": \"\", \"qtype\": 1 }").getBytes(StandardCharsets.UTF_8);

        DnsParseMapValidateFunction function = new DnsParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(blankQuery));
        harness.processElement(new StreamRecord<>(fixture("valid-zeek-shaped.json")));

        assertEquals(1, harness.extractOutputValues().size(),
            "the poison record must not kill the subtask; the following valid record still reaches main output");
        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, record.reason());
        assertEquals(ReasonCode.Stage.MAP, record.reason().stage(),
            "MISSING_REQUIRED_FIELD is DnsEventMapper's code, never the parser's -- it must never report PARSE");

        harness.close();
    }

    // DnsParseMapValidateFunction.deriveEventId must check id blank BEFORE
    // combining it with trans_id: "" + ":" + 4242 is ":4242", which is NOT
    // blank, so EventId.derive's own guard would never fire and
    // "sensor-eu-1::4242" would be written to the DLQ as if it were a real
    // identity. trans_id is deliberately non-zero here so that bug, if
    // reintroduced, would produce a visibly non-empty eventId instead of
    // coincidentally matching "".
    @Test
    void mapStageRejectionWithBlankIdCarriesNoEventId() throws Exception {
        byte[] blankId = ("{ \"id\": \"\", \"ts\": 1786608030.0, \"id_orig_h\": \"10.0.0.5\", "
            + "\"id_orig_p\": 53421, \"id_resp_h\": \"8.8.8.8\", \"id_resp_p\": 53, "
            + "\"trans_id\": 4242, \"query\": \"example.com\", \"qtype\": 1 }").getBytes(StandardCharsets.UTF_8);

        DnsParseMapValidateFunction function = new DnsParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(blankId));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, record.reason());
        assertEquals(ReasonCode.Stage.MAP, record.reason().stage());
        assertEquals("", record.eventId(), "blank id must never combine with trans_id into \"sensor-eu-1::4242\"");

        harness.close();
    }
}
