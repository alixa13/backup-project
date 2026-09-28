package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The per-stream ring of the last `capacity` preprocessed vectors.
class ModbusScoreWindowTest {

    private static float[] row(float v) {
        return new float[]{v, v + 0.5f};
    }

    @Test
    void itFillsThenSlidesKeepingTheNewestOldestFirst() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 3, 2);
        for (int i = 0; i < 5; i++) {
            window.append(row(i), 0);
        }
        assertEquals(3, window.size());
        assertTrue(window.isFull());
        assertArrayEquals(new float[][]{row(2), row(3), row(4)}, window.sequence());
    }

    @Test
    void resetEmptiesIt() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 3, 2);
        window.append(row(1), 8);
        window.reset();
        assertEquals(0, window.size());
        assertFalse(window.isFull());
        assertEquals(0, window.flagsOr());
    }

    // qualityFlags OR over the rows still in the window: a flag leaves with its row.
    @Test
    void theFlagsAreTheOrOverTheRowsStillInTheWindow() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 2, 2);
        window.append(row(1), 8);
        window.append(row(2), 4);
        assertEquals(12, window.flagsOr());
        window.append(row(3), 0);
        assertEquals(4, window.flagsOr());
    }

    // sequence() hands out a copy the caller cannot use to change the window.
    @Test
    void theSequenceIsACopy() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 2, 2);
        window.append(row(1), 0);
        window.sequence()[0][0] = 99f;
        assertEquals(1f, window.sequence()[0][0]);
    }

    @Test
    void aRowOfTheWrongWidthIsRejected() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 2, 2);
        assertThrows(IllegalArgumentException.class, () -> window.append(new float[3], 0));
    }
}
