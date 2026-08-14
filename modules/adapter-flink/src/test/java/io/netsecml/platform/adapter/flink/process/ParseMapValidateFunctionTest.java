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
}
