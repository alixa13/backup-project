package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ConnFeatureProcessFunctionTest {
    private NetworkEvent event(SensorId sensor, String sourceIp, Instant eventTime, long originBytes) {
        ConnectionTuple tuple = new ConnectionTuple(sourceIp, 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, originBytes, 0, 1, 1, 0);
        // LogType.CONN and a non-blank uid are required positional components now;
        // this harness test only exercises per-key windowing, so a fixed constant
        // and the same composite string used to derive eventId are enough.
        String uid = eventTime.toString() + sourceIp;
        EventEnvelope envelope = new EventEnvelope(EventId.derive(sensor, uid), eventTime, sensor, LogType.CONN, uid);
        return new ConnEvent(envelope, tuple, measurements, new ConnectionLocality(null, null));
    }

    @Test
    void stateAccumulatesPerKeyAcrossEvents() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new ConnFeatureProcessFunction(), new SourceKeySelector(), TypeInformation.of(SourceKey.class));

        SensorId sensor = new SensorId("sensor-eu-1");
        harness.processElement(new StreamRecord<>(event(sensor, "10.0.0.5", Instant.ofEpochSecond(60_000), 100)));
        harness.processElement(new StreamRecord<>(event(sensor, "10.0.0.5", Instant.ofEpochSecond(60_010), 200)));
        harness.processElement(new StreamRecord<>(event(sensor, "10.0.0.9", Instant.ofEpochSecond(60_010), 999)));

        List<FeatureVector> output = harness.extractOutputValues();
        assertEquals(3, output.size());
        assertEquals(1f, output.get(0).values()[17], "first event for 10.0.0.5");
        assertEquals(2f, output.get(1).values()[17], "second event for 10.0.0.5, same key");
        assertEquals(1f, output.get(2).values()[17], "first event for a different source IP, independent state");

        harness.close();
    }
}
