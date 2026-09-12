package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
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

    // Welford is numerically stable at scale: ten thousand records must still
    // produce the exact arithmetic answer, not a drifted one. Intervals alternate
    // between 10 ms and 30 ms rather than staying identical -- identical intervals
    // leave sumSquaredDifferences at exactly 0 no matter what, so that shape could
    // never have caught instability in the running deviation. Alternating gives a
    // known non-zero mean and deviation to check the accumulated arithmetic
    // against after ten thousand folds.
    @Test
    void arithmeticStaysExactAcrossManyObservations() {
        RecordTimingState state = RecordTimingState.empty();
        Instant time = T0;
        state = state.observe(time);
        for (int i = 0; i < 10_000; i++) {
            long step = (i % 2 == 0) ? 10L : 30L;
            time = time.plusMillis(step);
            state = state.observe(time);
        }

        // 5,000 intervals of 10 ms and 5,000 of 30 ms: mean 20, population
        // variance 0.5*(10-20)^2 + 0.5*(30-20)^2 = 100, stddev 10.
        assertEquals(10_001L, state.observationCount());
        assertEquals(20.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(10.0, state.stddevIntervalMillis(), 0.0001);
    }

    // The bounded-state invariant, asserted structurally rather than implied.
    // The previous version of this test ran a large loop and checked the answer,
    // which would have passed unchanged if someone added a List<Instant> field.
    // This one fails the moment any collection or array is introduced.
    @Test
    void holdsNoStructureThatGrowsWithObservationCount() {
        for (Field field : RecordTimingState.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            Class<?> type = field.getType();
            assertFalse(type.isArray(),
                "RecordTimingState must hold no array field, found: " + field.getName());
            assertFalse(Collection.class.isAssignableFrom(type),
                "RecordTimingState must hold no collection field, found: " + field.getName());
            assertFalse(Map.class.isAssignableFrom(type),
                "RecordTimingState must hold no map field, found: " + field.getName());
        }
    }

    // Zeek can deliver slightly out of order. Such a record yields no valid
    // interval, so it must be ignored entirely -- clamping it to zero would fold
    // a fabricated data point into the mean and deviation the model consumes.
    @Test
    void outOfOrderRecordsAreIgnoredRatherThanClampedToZero() {
        RecordTimingState inOrder = RecordTimingState.empty()
            .observe(T0)
            .observe(T0.plusMillis(100));

        RecordTimingState withLateArrival = inOrder.observe(T0.plusMillis(50));

        assertEquals(inOrder.observationCount(), withLateArrival.observationCount(),
            "an out-of-order record must not bump the observation count either -- "
            + "an implementation could bump the count without folding an interval and still pass the checks below");
        assertEquals(inOrder.meanIntervalMillis(), withLateArrival.meanIntervalMillis(), 0.0001,
            "an out-of-order record must not move the mean");
        assertEquals(inOrder.stddevIntervalMillis(), withLateArrival.stddevIntervalMillis(), 0.0001,
            "an out-of-order record must not move the deviation");
    }
}
