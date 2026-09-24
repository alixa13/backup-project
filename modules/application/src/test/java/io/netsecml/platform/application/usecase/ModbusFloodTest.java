package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.feature.ModbusEntityState;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

// The modbus engine's cost under a flood on one entity key. The trailing
// windows must be bounded by time -- exact counts over (t-w, t] are the
// frozen contract -- so a flood of r events/s holds about 60r entries in the
// 60 s window, and the per-event work must not grow with that. An engine that
// copied or rescanned its windows on every event did ~192r element operations
// per event and could not keep pace with ~1000 events/s on one key; this test
// holds the engine to amortized constant work per event instead.
class ModbusFloodTest {

    @Test
    void sixtySecondsOfAFiveThousandEventPerSecondFloodIsProcessedInUnderThirtySeconds() {
        // 300,000 events is 60 s of event time at 5,000 events/s, half of them
        // answered request/response pairs and half unanswered requests. The
        // 30 s bound is generous on purpose: amortized constant work finishes
        // in a second or two, while per-event window copies take hours.
        int events = 300_000;
        double ratePerSecond = 5_000.0;
        ModbusBuildFeaturesUseCase useCase = new ModbusBuildFeaturesUseCase(Clock.systemUTC());
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            ModbusEntityState state = ModbusEntityState.empty();
            for (int i = 0; i < events; i++) {
                state = useCase.build(ModbusEventStreams.steadyFloodEvent(i, ratePerSecond), state).newState();
            }
        });
    }
}
