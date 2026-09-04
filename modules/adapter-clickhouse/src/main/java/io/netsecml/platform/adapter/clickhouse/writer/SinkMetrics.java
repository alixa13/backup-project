package io.netsecml.platform.adapter.clickhouse.writer;

// A narrow seam over Flink's metric group.
//
// It exists so the writer's retry and flush logic can be unit-tested without a
// Flink runtime: constructing a real SinkWriterMetricGroup outside a running task
// means reaching into Flink internals, which breaks on every minor upgrade.
public interface SinkMetrics {

    // One batch landed successfully.
    void recordBatch(int rows, long bytes, long latencyMillis);

    // One insert attempt failed. Counted per attempt, not per batch, so the
    // counter distinguishes a flaky server from a dead one.
    void recordFailure();
}
