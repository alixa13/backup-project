package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ParseMapValidateFunctionTest {
    private byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", name));
    }

    @Test
    void validRecordReachesMainOutput() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("valid-tcp-ssl.json")));

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals("sensor-eu-1:Cabc123XYZ", output.get(0).eventId().value());

        harness.close();
    }

    @Test
    void malformedRecordGoesToRejectedSideOutputAndJobKeepsRunning() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));
        harness.processElement(new StreamRecord<>(fixture("valid-tcp-ssl.json")));

        assertEquals(1, harness.extractOutputValues().size(), "malformed record must not reach main output");
        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        assertEquals(ReasonCode.MALFORMED_JSON, rejected.iterator().next().getValue().reason());

        harness.close();
    }

    @Test
    void invalidPortGoesToRejectedSideOutputWithReasonCode() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("invalid-port.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        assertEquals(ReasonCode.INVALID_PORT, rejected.iterator().next().getValue().reason());

        harness.close();
    }

    // A fixed Clock is what makes receivedAt assertable. Stamping it at the point
    // of rejection — rather than inside the serializer, where it used to live —
    // means it records when the record was rejected, not when the sink ran.
    @Test
    void stampsReceivedAtAtTheMomentOfRejection() throws Exception {
        Instant fixed = Instant.parse("2026-08-27T10:03:11.250Z");
        ParseMapValidateFunction function =
            new ParseMapValidateFunction(new SensorId("sensor-eu-1"), Clock.fixed(fixed, ZoneOffset.UTC));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(fixed, rejected.iterator().next().getValue().receivedAt());

        harness.close();
    }

    // A parse-stage failure never produced a DTO, so there is no identity to carry.
    @Test
    void parseStageRejectionCarriesNoEventId() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals("", rejected.iterator().next().getValue().eventId());

        harness.close();
    }

    // A map-stage failure parsed cleanly, so it CAN be named. The ID is derived
    // exactly as the feature path derives it, which is what makes an invalid_events
    // row joinable to feature_vectors.event_id.
    @Test
    void mapStageRejectionCarriesTheDerivedEventId() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("invalid-port.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.INVALID_PORT, record.reason());
        assertFalse(record.eventId().isBlank(), "a map-stage rejection knows which event failed");
        assertTrue(record.eventId().startsWith("sensor-eu-1:"), "event id is the same composite the feature path derives");

        harness.close();
    }
}
