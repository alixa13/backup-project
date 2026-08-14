package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SourceWindowStateTest {
    @Test
    void emptyStateHasZeroSums() {
        SourceWindowState state = SourceWindowState.empty();
        assertEquals(0, state.connectionCount5m());
        assertEquals(0, state.byteSum5m());
        assertEquals(0, state.failedCount5m());
    }

    @Test
    void recordAccumulatesWithinFiveMinuteWindow() {
        SourceWindowState state = SourceWindowState.empty();
        long minute = 1000L;
        state = state.record(minute, 500, false);
        state = state.record(minute, 300, true);
        state = state.record(minute + 1, 200, false);
        assertEquals(3, state.connectionCount5m());
        assertEquals(1000, state.byteSum5m());
        assertEquals(1, state.failedCount5m());
    }

    @Test
    void recordIsImmutable() {
        SourceWindowState original = SourceWindowState.empty();
        SourceWindowState updated = original.record(1000L, 500, false);
        assertEquals(0, original.connectionCount5m(), "original state must not be mutated");
        assertEquals(1, updated.connectionCount5m());
    }

    @Test
    void bucketsOlderThanFiveMinutesRollOff() {
        SourceWindowState state = SourceWindowState.empty();
        state = state.record(1000L, 999, false);
        state = state.record(1006L, 1, false);
        assertEquals(1, state.connectionCount5m(), "bucket from minute 1000 is 6 minutes behind minute 1006 and must roll off");
        assertEquals(1, state.byteSum5m());
    }
}
