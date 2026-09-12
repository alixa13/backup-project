package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// conn.log snapshots are cumulative, so the only useful rate signal is the
// difference between two of them. These tests pin that arithmetic and the
// guards around it.
class ConnSnapshotTest {

    private static final Instant START = Instant.parse("2026-09-10T10:00:00Z");

    // A snapshot five minutes in, then another five minutes later. The delta must
    // be the difference, never the raw cumulative totals.
    @Test
    void deltaFromSubtractsCumulativeCounters() {
        ConnSnapshot first = new ConnSnapshot("Cabc", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        ConnSnapshot second = new ConnSnapshot("Cabc", START, START.plusSeconds(600), 1500L, 2600L, 14L, 27L);

        ConnSnapshotDelta delta = second.deltaFrom(first);

        assertEquals(500L, delta.origBytes());
        assertEquals(600L, delta.respBytes());
        assertEquals(4L, delta.origPkts());
        assertEquals(7L, delta.respPkts());
    }

    // Zeek can restart or a counter can be reset between snapshots. A negative
    // delta is meaningless as a rate, so it clamps to zero rather than emitting
    // a negative feature the model would have to learn around.
    @Test
    void deltaFromClampsNegativeCountersToZero() {
        ConnSnapshot first = new ConnSnapshot("Cabc", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        ConnSnapshot reset = new ConnSnapshot("Cabc", START, START.plusSeconds(600), 5L, 5L, 1L, 1L);

        ConnSnapshotDelta delta = reset.deltaFrom(first);

        assertEquals(0L, delta.origBytes());
        assertEquals(0L, delta.respBytes());
        assertEquals(0L, delta.origPkts());
        assertEquals(0L, delta.respPkts());
    }

    // Two snapshots from different connections must never be subtracted; that
    // would silently blend unrelated traffic into one rate.
    @Test
    void deltaFromRejectsAMismatchedConnection() {
        ConnSnapshot mine = new ConnSnapshot("Cabc", START, START.plusSeconds(600), 1500L, 2600L, 14L, 27L);
        ConnSnapshot theirs = new ConnSnapshot("Cxyz", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        assertThrows(IllegalArgumentException.class, () -> mine.deltaFrom(theirs));
    }

    // Age is what distinguishes a long-lived SCADA poll loop from a short burst,
    // and it is the reason conn.log is worth joining at all.
    @Test
    void ageSecondsMeasuresFromConnectionStartToObservation() {
        ConnSnapshot snapshot = new ConnSnapshot("Cabc", START, START.plusSeconds(3600), 1L, 1L, 1L, 1L);

        assertEquals(3600L, snapshot.ageSeconds());
    }

    // A connection's first snapshot has no predecessor, but its own cumulative
    // counters already ARE the delta -- conn.log counts from connection start.
    @Test
    void asInitialDeltaUsesTheSnapshotsOwnCountersAsTheDelta() {
        ConnSnapshot first = new ConnSnapshot("Cabc", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        ConnSnapshotDelta delta = first.asInitialDelta();

        assertEquals(1000L, delta.origBytes());
        assertEquals(2000L, delta.respBytes());
        assertEquals(10L, delta.origPkts());
        assertEquals(20L, delta.respPkts());
        assertEquals(300L, delta.ageSeconds());
    }

    // Identity is structural: without a uid the snapshot cannot be joined to
    // anything, so an absent one is a construction error rather than a default.
    @Test
    void rejectsABlankConnectionUid() {
        assertThrows(IllegalArgumentException.class,
            () -> new ConnSnapshot("  ", START, START, 1L, 1L, 1L, 1L));
    }

    // A sensor with clock skew, or an out-of-order record, can put observedAt
    // before connectionStart. A negative age is meaningless as a feature, so it
    // clamps to zero rather than propagating into the vector.
    @Test
    void ageSecondsClampsToZeroWhenObservationPrecedesTheConnectionStart() {
        ConnSnapshot skewed = new ConnSnapshot("Cabc", START, START.minusSeconds(30), 1L, 1L, 1L, 1L);

        assertEquals(0L, skewed.ageSeconds());
    }
}
