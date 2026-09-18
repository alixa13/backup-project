package io.netsecml.platform.domain.feature;

import java.util.Arrays;

// A bounded five-bucket, one-minute rolling window of three counters for a
// single key: how many records, how many bytes, how many failures.
//
// Extracted from ConnWindowState, which named conn in a type the protocol-
// agnostic common tier depends on. What "bytes" and "failed" MEAN is the
// caller's decision, and that is the whole point of the extraction: conn passes
// total_bytes and a failed conn_state; DNS passes 0 bytes (dns.log carries no
// byte counts) and rcode != NOERROR. The three frozen common-tier names --
// record_count_5m, byte_sum_5m, failed_count_5m -- are already generic, so this
// type now matches the contract it feeds.
//
// Deliberately holds no timestamps: RecordTimingState owns inter-arrival shape,
// and the bounded-state invariant forbids keeping a record history here.
public final class RollingCounters {
    private static final int BUCKET_COUNT = 5;

    // Ring-buffer style parallel arrays: each index is a one-minute bucket slot,
    // reused (and reset) once the epoch minute wraps back onto that slot.
    private final long[] bucketMinutes;
    private final long[] recordCounts;
    private final long[] byteSums;
    private final long[] failedCounts;

    private RollingCounters(long[] bucketMinutes, long[] recordCounts, long[] byteSums, long[] failedCounts) {
        this.bucketMinutes = bucketMinutes;
        this.recordCounts = recordCounts;
        this.byteSums = byteSums;
        this.failedCounts = failedCounts;
    }

    // Fresh window: every bucket slot is unset (Long.MIN_VALUE sentinel) so the
    // first record into any slot is always treated as a new bucket.
    public static RollingCounters empty() {
        long[] minutes = new long[BUCKET_COUNT];
        Arrays.fill(minutes, Long.MIN_VALUE);
        return new RollingCounters(minutes, new long[BUCKET_COUNT], new long[BUCKET_COUNT], new long[BUCKET_COUNT]);
    }

    // Folds one record into its bucket, returning a new immutable state. If the
    // target slot belongs to a different (older) minute, it is reset to zero
    // before accumulating -- this is how stale buckets roll off the window.
    //
    // KNOWN LIMIT: "different" is a bare != against the slot's stored minute,
    // not a comparison of "older" vs "newer" -- a record arriving for a minute
    // OLDER than what the slot already holds resets that slot too, erasing the
    // newer minute's counts for this key. Nothing requires the external
    // conn/dns topics to be partitioned by id_orig_h, so out-of-order arrival
    // per key is ordinary here, not exceptional. DEPLOYMENT REQUIREMENT: the
    // sensor should partition its Kafka producer by source IP so records for
    // one key arrive in order. Deliberately not changed to compare minutes
    // instead of merely differing from them -- that would alter conn's already
    // emitted values and is its own unit's decision, not this fix wave's.
    public RollingCounters record(long bucketEpochMinute, long bytes, boolean failed) {
        long[] minutes = Arrays.copyOf(bucketMinutes, BUCKET_COUNT);
        long[] counts = Arrays.copyOf(recordCounts, BUCKET_COUNT);
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
        return new RollingCounters(minutes, counts, sums, fails);
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

    public long recordCount5m() {
        return sumWithinWindow(recordCounts, latestBucketMinute());
    }

    public long byteSum5m() {
        return sumWithinWindow(byteSums, latestBucketMinute());
    }

    public long failedCount5m() {
        return sumWithinWindow(failedCounts, latestBucketMinute());
    }
}
