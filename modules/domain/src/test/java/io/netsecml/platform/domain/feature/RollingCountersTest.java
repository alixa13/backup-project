package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RollingCountersTest {
    @Test
    void emptyStateHasZeroSums() {
        RollingCounters state = RollingCounters.empty();
        assertEquals(0, state.recordCount5m());
        assertEquals(0, state.byteSum5m());
        assertEquals(0, state.failedCount5m());
    }

    @Test
    void recordAccumulatesWithinFiveMinuteWindow() {
        RollingCounters state = RollingCounters.empty();
        long minute = 1000L;
        state = state.record(minute, 500, false);
        state = state.record(minute, 300, true);
        state = state.record(minute + 1, 200, false);
        assertEquals(3, state.recordCount5m());
        assertEquals(1000, state.byteSum5m());
        assertEquals(1, state.failedCount5m());
    }

    @Test
    void recordIsImmutable() {
        RollingCounters original = RollingCounters.empty();
        RollingCounters updated = original.record(1000L, 500, false);
        assertEquals(0, original.recordCount5m(), "original state must not be mutated");
        assertEquals(1, updated.recordCount5m());
    }

    @Test
    void bucketsOlderThanFiveMinutesRollOff() {
        RollingCounters state = RollingCounters.empty();
        state = state.record(1000L, 999, false);
        state = state.record(1006L, 1, false);
        assertEquals(1, state.recordCount5m(), "bucket from minute 1000 is 6 minutes behind minute 1006 and must roll off");
        assertEquals(1, state.byteSum5m());
    }
}
