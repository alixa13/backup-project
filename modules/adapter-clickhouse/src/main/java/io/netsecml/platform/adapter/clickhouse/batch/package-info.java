/**
 * Batch-size/time-window batching policy (5,000 rows / 4 MiB / 1 second).
 *
 * <p>Implemented: {@code BatchBuffer} accumulates JSONEachRow lines and reports
 * when a row-count or byte-size trigger fires; the 1-second timer that drives
 * the third trigger lives in the sink writer, which owns Flink's
 * {@code ProcessingTimeService}.
 */
package io.netsecml.platform.adapter.clickhouse.batch;
