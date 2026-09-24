package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

// A flood on one S7 connection costs constant work per event: every history
// is a fixed ring and the outstanding set is bounded by the 16-bit reference
// space. One million events, half answered, must finish well inside the bound
// (a few seconds on this machine); per-event work that grew with the flood --
// an unbounded history, a rescan of every reference ever seen -- would not.
class S7commFloodTest {

    @Test
    void aMillionEventsOnOneConnectionRunInConstantWorkPerEvent() {
        SensorId sensor = new SensorId("sensor-eu-1");
        S7commBuildFeaturesUseCase useCase = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            S7commConnectionState state = S7commConnectionState.empty();
            for (int i = 0; i < 1_000_000; i++) {
                // Requests to port 102 and, every other event, the response to
                // the previous request; PDU references wrap at 65,536.
                boolean request = i % 2 == 0;
                int pdu = (i / 2) % 65_536;
                double ts = 1_790_000_000.0 + i * 0.0001;
                S7commEvent event = new S7commEvent(
                    new EventEnvelope(EventId.derive(sensor, "CFLOOD:" + i), Instant.ofEpochMilli((long) (ts * 1000)),
                        sensor, LogType.S7COMM, "CFLOOD"),
                    ts, request ? "10.0.0.5" : "10.0.0.9", request ? 50001 : 102,
                    request ? "10.0.0.9" : "10.0.0.5", request ? 102 : 50001, pdu, request ? 1 : 3, 4 + (i % 3),
                    null);
                state = useCase.build(event, state).newState();
            }
        });
    }
}
