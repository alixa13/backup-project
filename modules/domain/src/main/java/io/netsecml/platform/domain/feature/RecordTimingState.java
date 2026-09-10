package io.netsecml.platform.domain.feature;

import java.time.Duration;
import java.time.Instant;

// Running inter-arrival statistics for a single (sensor, sourceIp) key.
//
// Deliberately NOT part of SourceWindowState: that type's serialized shape is
// shared with the frozen conn path, and widening it would change checkpoint
// state for a job that is already running.
//
// Uses Welford's online algorithm, so the footprint is a count, a mean and a
// sum-of-squared-differences no matter how many records pass through. The
// bounded-state invariant forbids keeping a timestamp history.
public final class RecordTimingState {

    private final Instant lastRecordTime;
    private final long observationCount;
    private final double meanMillis;
    private final double sumSquaredDifferences;

    private RecordTimingState(Instant lastRecordTime, long observationCount,
                              double meanMillis, double sumSquaredDifferences) {
        this.lastRecordTime = lastRecordTime;
        this.observationCount = observationCount;
        this.meanMillis = meanMillis;
        this.sumSquaredDifferences = sumSquaredDifferences;
    }

    public static RecordTimingState empty() {
        return new RecordTimingState(null, 0L, 0.0, 0.0);
    }

    // Folds one record's timestamp into the running statistics, returning a new
    // state. The first record establishes a baseline but contributes no interval,
    // because there is nothing preceding it to measure against.
    public RecordTimingState observe(Instant recordTime) {
        if (recordTime == null) {
            throw new IllegalArgumentException("recordTime must not be null");
        }
        if (lastRecordTime == null) {
            return new RecordTimingState(recordTime, 1L, 0.0, 0.0);
        }

        // Zeek can deliver slightly out of order. A record that arrives out of order
        // yields no valid interval, so it must be ignored entirely. Clamping it to
        // zero would fold a fabricated data point into the mean and deviation the
        // model consumes, biasing the feature for every downstream inference.
        //
        // Ignoring it is not bias-free either: lastRecordTime stays at the running
        // maximum, so the next IN-ORDER record after this gap measures one long
        // interval spanning both the skipped record and itself, instead of the two
        // shorter intervals a fully-ordered stream would have produced. That is a
        // directional bias toward a longer mean and a higher deviation, on top of
        // the smaller sample size from dropping the out-of-order record outright.
        // Accepted because the alternative (clamping to a fabricated near-zero
        // interval) is worse, not because this path is neutral.
        if (recordTime.isBefore(lastRecordTime)) {
            return this;
        }

        long intervalMillis = Duration.between(lastRecordTime, recordTime).toMillis();

        // Welford: update the mean, then accumulate the squared difference using
        // both the old and new means. This is numerically stable where a naive
        // sum-of-squares is not.
        long nextCount = observationCount + 1L;
        long intervalCount = nextCount - 1L;
        double delta = intervalMillis - meanMillis;
        double nextMean = meanMillis + delta / intervalCount;
        double nextSumSquares = sumSquaredDifferences + delta * (intervalMillis - nextMean);

        return new RecordTimingState(recordTime, nextCount, nextMean, nextSumSquares);
    }

    public long observationCount() {
        return observationCount;
    }

    public double meanIntervalMillis() {
        return meanMillis;
    }

    // Population standard deviation over the intervals observed so far. Fewer
    // than two records means no interval exists, so the answer is zero rather
    // than undefined -- the feature must always be emittable.
    public double stddevIntervalMillis() {
        long intervalCount = observationCount - 1L;
        if (intervalCount < 1L) {
            return 0.0;
        }
        return Math.sqrt(sumSquaredDifferences / intervalCount);
    }
}
