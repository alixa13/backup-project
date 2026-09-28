package io.netsecml.platform.domain.inference;

import java.util.Arrays;

// One S7 connection's last `capacity` preprocessed vectors, a ring, with each
// row's quality flags, the bundle that filled it, and the connection's event
// count since the window last reset (scoring design section 5). Mutable and
// updated in place, like ModbusScoreWindow: keyed Flink state, read through
// value() on every call, never cached. Its own class, not ModbusScoreWindow's,
// because that one is live Flink state whose layout must not change.
public final class S7commScoreWindow {

    private final String bundleId;
    private final int capacity;
    private final int width;
    private final float[][] rows;
    private final int[] flags;
    // Index of the oldest row, how many rows are held, and events since reset.
    private int start;
    private int size;
    private long eventsSinceReset;

    private S7commScoreWindow(String bundleId, int capacity, int width) {
        this.bundleId = bundleId;
        this.capacity = capacity;
        this.width = width;
        this.rows = new float[capacity][width];
        this.flags = new int[capacity];
    }

    public static S7commScoreWindow empty(String bundleId, int capacity, int width) {
        if (bundleId == null || capacity < 1 || width < 1) {
            throw new IllegalArgumentException("a window needs a bundle id, a capacity and a width");
        }
        return new S7commScoreWindow(bundleId, capacity, width);
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

    // The connection's history starts again: no row, no flag, no event.
    public void reset() {
        start = 0;
        size = 0;
        Arrays.fill(flags, 0);
        eventsSinceReset = 0;
    }

    // One more event since the last reset; returns the count, 1-based.
    public long countEvent() {
        return ++eventsSinceReset;
    }

    public long eventsSinceReset() {
        return eventsSinceReset;
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
