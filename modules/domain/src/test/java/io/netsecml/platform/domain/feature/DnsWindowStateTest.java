package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// DnsWindowState is a thin pairing of the two independent pieces of state DNS's
// use case folds an event into. The behavior worth pinning here is narrow: both
// components are required (this record is what ends up in Flink's ValueState,
// so a silently-null half would surface as an NPE deep inside build(), not
// here), and empty() is the zero state a fresh key starts from.
class DnsWindowStateTest {

    @Test
    void emptyPairsBothComponentsAtTheirOwnEmptyState() {
        DnsWindowState state = DnsWindowState.empty();

        assertEquals(0L, state.counters().recordCount5m());
        assertEquals(0L, state.counters().byteSum5m());
        assertEquals(0L, state.counters().failedCount5m());
        assertEquals(0L, state.timing().observationCount());
    }

    @Test
    void rejectsNullCountersOrTiming() {
        assertThrows(NullPointerException.class,
            () -> new DnsWindowState(null, RecordTimingState.empty()));
        assertThrows(NullPointerException.class,
            () -> new DnsWindowState(RollingCounters.empty(), null));
    }

}
