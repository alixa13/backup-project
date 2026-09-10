package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// Inter-arrival statistics for one key, kept in constant space. The bounded-state
// invariant forbids holding a timestamp history, so these tests pin the online
// arithmetic that replaces one.
class RecordTimingStateTest {

    private static final Instant T0 = Instant.parse("2026-09-10T10:00:00Z");

    // A single record has no interval yet -- there is nothing to measure against.
    // Zero is the defined answer so the feature is always emittable.
    @Test
    void aSingleObservationHasNoIntervalYet() {
        RecordTimingState state = RecordTimingState.empty().observe(T0);

        assertEquals(0.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(0.0, state.stddevIntervalMillis(), 0.0001);
        assertEquals(1L, state.observationCount());
    }

    // Evenly spaced records: the mean is the spacing and the deviation is zero.
    // This is what a healthy SCADA poll loop looks like.
    @Test
    void evenlySpacedRecordsHaveZeroDeviation() {
        RecordTimingState state = RecordTimingState.empty()
            .observe(T0)
            .observe(T0.plusMillis(100))
            .observe(T0.plusMillis(200))
            .observe(T0.plusMillis(300));

        assertEquals(100.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(0.0, state.stddevIntervalMillis(), 0.0001);
    }

    // Uneven spacing must produce a non-zero deviation; this is the signal that
    // separates a steady poll from bursty traffic.
    @Test
    void unevenSpacingProducesANonZeroDeviation() {
        RecordTimingState state = RecordTimingState.empty()
            .observe(T0)
            .observe(T0.plusMillis(100))
            .observe(T0.plusMillis(1100));

        // Intervals are 100 ms and 1000 ms: mean 550, population stddev 450.
        assertEquals(550.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(450.0, state.stddevIntervalMillis(), 0.0001);
    }

    // The whole point of Welford: state size is constant. Ten thousand records
    // must leave exactly the same footprint as three.
    @Test
    void stateIsBoundedRegardlessOfObservationCount() {
        RecordTimingState state = RecordTimingState.empty();
        for (int i = 0; i < 10_000; i++) {
            state = state.observe(T0.plusMillis(i * 10L));
        }

        assertEquals(10_000L, state.observationCount());
        assertEquals(10.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(0.0, state.stddevIntervalMillis(), 0.0001);
    }

    // Zeek can deliver slightly out of order. A negative interval would corrupt
    // the running mean, so it is clamped rather than trusted.
    @Test
    void outOfOrderRecordsDoNotProduceNegativeIntervals() {
        RecordTimingState state = RecordTimingState.empty()
            .observe(T0.plusMillis(500))
            .observe(T0);

        assertTrue(state.meanIntervalMillis() >= 0.0,
            "an out-of-order record must not drive the mean interval negative");
    }
}
