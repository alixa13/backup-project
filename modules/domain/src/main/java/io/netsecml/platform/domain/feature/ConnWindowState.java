package io.netsecml.platform.domain.feature;

import java.util.Arrays;

/**
 * Five fixed one-minute buckets holding rolling connection-count/byte-sum/
 * failed-count totals for a single (sensor, sourceIp) key.
 *
 * Named for conn deliberately. These three counters are connection concepts: a
 * DNS window would want NXDOMAIN counts and an SSH window would want auth
 * failures. The ring mechanics are worth extracting when a second log type
 * actually needs a rolling window -- not before, because each of the three
 * possible generalizations trades away something real, and there is no second
 * consumer yet to say which trade is right.
 *
 * That second consumer already exists in the code, even without a second log
 * type: {@code CommonFeatureExtractor.extract} takes this class as its window
 * parameter to populate the protocol-agnostic tier's indices 0-2, and DNS is
 * that tier's first consumer. The extraction trigger this javadoc describes
 * fires exactly there.
 *
 * Bounded by construction: exactly 5 longs per array, never a per-IP set or
 * list. See PILOT_ARCHITECTURE.md section 6 for the design rationale.
 */
public final class ConnWindowState {
    private static final int BUCKET_COUNT = 5;

    // Ring-buffer style parallel arrays: each index is a one-minute bucket slot,
    // reused (and reset) once the epoch minute wraps back onto that slot.
    private final long[] bucketMinutes;
    private final long[] connectionCounts;
    private final long[] byteSums;
    private final long[] failedCounts;

    private ConnWindowState(long[] bucketMinutes, long[] connectionCounts, long[] byteSums, long[] failedCounts) {
        this.bucketMinutes = bucketMinutes;
        this.connectionCounts = connectionCounts;
        this.byteSums = byteSums;
        this.failedCounts = failedCounts;
    }

    // Fresh window: every bucket slot is unset (Long.MIN_VALUE sentinel) so the
    // first record into any slot is always treated as a new bucket.
    public static ConnWindowState empty() {
        long[] minutes = new long[BUCKET_COUNT];
        Arrays.fill(minutes, Long.MIN_VALUE);
        return new ConnWindowState(minutes, new long[BUCKET_COUNT], new long[BUCKET_COUNT], new long[BUCKET_COUNT]);
    }

    // Folds one record into its bucket, returning a new immutable state. If the
    // target slot belongs to a different (older) minute, it is reset to zero
    // before accumulating -- this is how stale buckets roll off the window.
    public ConnWindowState record(long bucketEpochMinute, long bytes, boolean failed) {
        long[] minutes = Arrays.copyOf(bucketMinutes, BUCKET_COUNT);
        long[] counts = Arrays.copyOf(connectionCounts, BUCKET_COUNT);
        long[] sums = Arrays.copyOf(byteSums, BUCKET_COUNT);
        long[] fails = Arrays.copyOf(failedCounts, BUCKET_COUNT);

        int slot = (int) Math.floorMod(bucketEpochMinute, (long) BUCKET_COUNT);
        if (minutes[slot] != bucketEpochMinute) {
            minutes[slot] = bucketEpochMinute;
            counts[slot] = 0;
            sums[slot] = 0;
            fails[slot] = 0;
        }
        counts[slot] += 1;
        sums[slot] += bytes;
        if (failed) {
            fails[slot] += 1;
        }
        return new ConnWindowState(minutes, counts, sums, fails);
    }

    // Sums only the buckets still within the 5-minute window relative to the
    // most recent bucket seen; anything older is treated as rolled off.
    private long sumWithinWindow(long[] values, long currentMinuteHint) {
        long total = 0;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            if (bucketMinutes[i] != Long.MIN_VALUE && currentMinuteHint - bucketMinutes[i] < BUCKET_COUNT) {
                total += values[i];
            }
        }
        return total;
    }

    // The most recent bucket minute recorded, used as the reference point for
    // deciding which other buckets are still within the window.
    private long latestBucketMinute() {
        long latest = Long.MIN_VALUE;
        for (long m : bucketMinutes) {
            if (m > latest) {
                latest = m;
            }
        }
        return latest;
    }

    public long connectionCount5m() {
        return sumWithinWindow(connectionCounts, latestBucketMinute());
    }

    public long byteSum5m() {
        return sumWithinWindow(byteSums, latestBucketMinute());
    }

    public long failedCount5m() {
        return sumWithinWindow(failedCounts, latestBucketMinute());
    }
}
