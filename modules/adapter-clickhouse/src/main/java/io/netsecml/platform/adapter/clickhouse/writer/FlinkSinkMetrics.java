package io.netsecml.platform.adapter.clickhouse.writer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

// Publishes the archive sink's four numbers through Flink's metric system.
//
// Gauges of the most recent batch rather than histograms: Flink's Histogram needs
// either the dropwizard bridge or an internal implementation, and neither is
// worth a dependency for four values. Day 12 owns real observability.
public final class FlinkSinkMetrics implements SinkMetrics {
    // Counts every failed insert attempt across the sink's lifetime.
    private final Counter insertFailures;

    // Read by the gauges from the metric reporter's thread.
    private volatile int lastBatchRows;
    private volatile long lastBatchBytes;
    private volatile long lastFlushLatencyMillis;

    public FlinkSinkMetrics(MetricGroup group, String table) {
        // Scoped per table, so feature_vectors and invalid_events report separately.
        MetricGroup scoped = group.addGroup("archive").addGroup("table", table);

        // Register the failure counter plus one gauge per last-batch field. The
        // gauges read the volatile fields above whenever the reporter polls them.
        this.insertFailures = scoped.counter("insert.failures");
        scoped.gauge("batch.rows", () -> lastBatchRows);
        scoped.gauge("batch.bytes", () -> lastBatchBytes);
        scoped.gauge("flush.latency.ms", () -> lastFlushLatencyMillis);
    }

    @Override
    public void recordBatch(int rows, long bytes, long latencyMillis) {
        // Overwrite the last-batch snapshot; the gauges above read these lazily.
        lastBatchRows = rows;
        lastBatchBytes = bytes;
        lastFlushLatencyMillis = latencyMillis;
    }

    @Override
    public void recordFailure() {
        // One insert attempt failed; bump the counter so it accumulates across
        // the subtask's lifetime rather than resetting per batch.
        insertFailures.inc();
    }
}
