package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.SourceWindowState;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class BuildFeaturesUseCaseImplTest {
    private final BuildFeaturesUseCaseImpl useCase = new BuildFeaturesUseCaseImpl();

    private NetworkEvent event(Instant eventTime, long originBytes, long responseBytes, boolean failed) {
        SensorId sensor = new SensorId("sensor-eu-1");
        ConnectionState state = failed ? ConnectionState.S0 : ConnectionState.SF;
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, state);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, originBytes, responseBytes, 5, 5, 0);
        return new NetworkEvent(EventId.derive(sensor, eventTime.toString()), eventTime, sensor, tuple, measurements,
            new ConnectionLocality(null, null));
    }

    @Test
    void producesTwentyValueVectorWithFrozenSchemaIdentity() {
        NetworkEvent e = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult result = useCase.build(e, SourceWindowState.empty());
        assertEquals(20, result.vector().values().length);
        assertEquals("conn-feature-v1", result.vector().schemaId());
        assertEquals("f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b", result.vector().schemaHash());
        assertEquals(e.eventId().value(), result.vector().eventId());
    }

    @Test
    void windowedFeaturesAccumulateAcrossCallsForSameKey() {
        SourceWindowState state = SourceWindowState.empty();

        NetworkEvent first = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult r1 = useCase.build(first, state);
        assertEquals(1f, r1.vector().values()[17], "source_connections_5m after first event");
        assertEquals(300f, r1.vector().values()[18], "source_bytes_5m after first event (total_bytes=100+200)");
        assertEquals(0f, r1.vector().values()[19], "source_failed_connections_5m, first event was not failed");

        NetworkEvent second = event(Instant.ofEpochSecond(60_030), 50, 50, true);
        FeatureBuildResult r2 = useCase.build(second, r1.newState());
        assertEquals(2f, r2.vector().values()[17], "source_connections_5m after second event, same minute bucket");
        assertEquals(400f, r2.vector().values()[18], "300 + total_bytes(50+50)=100 = 400");
        assertEquals(1f, r2.vector().values()[19], "second event was failed (S0)");
    }
}
