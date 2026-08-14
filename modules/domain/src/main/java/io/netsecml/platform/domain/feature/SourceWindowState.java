package io.netsecml.platform.domain.feature;

import java.util.Arrays;

/**
 * Five fixed one-minute buckets holding rolling connection-count/byte-sum/failed-count totals
 * for a single (sensor, sourceIp) key. Bounded by construction: exactly 5 longs per array,
 * never a per-IP set or list. See PILOT_ARCHITECTURE.md section 6 for the design rationale.
 */
public final class SourceWindowState {
    private static final int BUCKET_COUNT = 5;

    private final long[] bucketMinutes;
    private final long[] connectionCounts;
    private final long[] byteSums;
    private final long[] failedCounts;

    private SourceWindowState(long[] bucketMinutes, long[] connectionCounts, long[] byteSums, long[] failedCounts) {
        this.bucketMinutes = bucketMinutes;
        this.connectionCounts = connectionCounts;
        this.byteSums = byteSums;
        this.failedCounts = failedCounts;
    }

    public static SourceWindowState empty() {
        long[] minutes = new long[BUCKET_COUNT];
        Arrays.fill(minutes, Long.MIN_VALUE);
        return new SourceWindowState(minutes, new long[BUCKET_COUNT], new long[BUCKET_COUNT], new long[BUCKET_COUNT]);
    }

    public SourceWindowState record(long bucketEpochMinute, long bytes, boolean failed) {
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
        return new SourceWindowState(minutes, counts, sums, fails);
    }

    private long sumWithinWindow(long[] values, long currentMinuteHint) {
        long total = 0;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            if (bucketMinutes[i] != Long.MIN_VALUE && currentMinuteHint - bucketMinutes[i] < BUCKET_COUNT) {
                total += values[i];
            }
        }
        return total;
    }

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
