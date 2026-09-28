package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// A connection's last rows, oldest first, their flags, and its event count
// since the last reset.
class S7commScoreWindowTest {

    @Test
    void itKeepsTheLastCapacityRowsOldestFirst() {
        S7commScoreWindow w = S7commScoreWindow.empty("b/v1", 3, 2);
        for (int i = 0; i < 5; i++) {
            w.append(new float[]{i, -i}, 0);
        }
        assertTrue(w.isFull());
        assertEquals(3, w.size());
        float[][] rows = w.sequence();
        assertEquals(2f, rows[0][0]);
        assertEquals(4f, rows[2][0]);
    }

    @Test
    void itCountsEventsSinceItsLastReset() {
        S7commScoreWindow w = S7commScoreWindow.empty("b/v1", 3, 2);
        assertEquals(1, w.countEvent());
        assertEquals(2, w.countEvent());
        w.append(new float[2], 8);
        w.reset();
        assertEquals(0, w.size());
        assertEquals(0, w.eventsSinceReset());
        assertEquals(0, w.flagsOr());
        assertEquals(1, w.countEvent());
    }

    @Test
    void itOrsTheFlagsOfTheRowsItHolds() {
        S7commScoreWindow w = S7commScoreWindow.empty("b/v1", 2, 1);
        w.append(new float[1], 16);
        w.append(new float[1], 0);
        assertEquals(16, w.flagsOr());
        w.append(new float[1], 0);
        assertEquals(0, w.flagsOr(), "the flagged row left");
    }

    @Test
    void aRowOfTheWrongWidthIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> S7commScoreWindow.empty("b/v1", 2, 3).append(new float[2], 0));
    }
}
