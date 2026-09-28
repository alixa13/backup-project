package io.netsecml.platform.domain.inference;

import java.util.Arrays;

// One stream's last `capacity` preprocessed vectors, a ring, plus each row's
// quality flags and the bundle that filled it. Mutable and updated in place,
// like ModbusEntityState: keyed Flink state, read through value() on every
// call, never cached.
public final class ModbusScoreWindow {

    private final String bundleId;
    private final int capacity;
    private final int width;
    private final float[][] rows;
    private final int[] flags;
    // Index of the oldest row, and how many rows are held.
    private int start;
    private int size;

    private ModbusScoreWindow(String bundleId, int capacity, int width) {
        this.bundleId = bundleId;
        this.capacity = capacity;
        this.width = width;
        this.rows = new float[capacity][width];
        this.flags = new int[capacity];
    }

    public static ModbusScoreWindow empty(String bundleId, int capacity, int width) {
        if (bundleId == null || capacity < 1 || width < 1) {
            throw new IllegalArgumentException("a window needs a bundle id, a capacity and a width");
        }
        return new ModbusScoreWindow(bundleId, capacity, width);
    }

    public String bundleId() {
        return bundleId;
    }

    public int size() {
        return size;
    }

    public boolean isFull() {
        return size == capacity;
    }

    // A segment start: the next sequence begins at the next row.
    public void reset() {
        start = 0;
        size = 0;
        Arrays.fill(flags, 0);
    }

    // Appends a row; once full, the oldest row leaves.
    public void append(float[] row, int qualityFlags) {
        if (row.length != width) {
            throw new IllegalArgumentException("expected a row of " + width + " values, got " + row.length);
        }
        int slot;
        if (size < capacity) {
            slot = (start + size) % capacity;
            size++;
        } else {
            slot = start;
            start = (start + 1) % capacity;
        }
        System.arraycopy(row, 0, rows[slot], 0, width);
        flags[slot] = qualityFlags;
    }

    // The held rows, oldest first, as a fresh array.
    public float[][] sequence() {
        float[][] out = new float[size][];
        for (int i = 0; i < size; i++) {
            out[i] = Arrays.copyOf(rows[(start + i) % capacity], width);
        }
        return out;
    }

    // The OR of the held rows' quality flags.
    public int flagsOr() {
        int or = 0;
        for (int i = 0; i < size; i++) {
            or |= flags[(start + i) % capacity];
        }
        return or;
    }
}
