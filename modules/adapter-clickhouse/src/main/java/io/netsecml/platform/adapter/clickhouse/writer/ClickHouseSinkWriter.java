package io.netsecml.platform.adapter.clickhouse.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.batch.BatchBuffer;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

// Buffers rows and flushes them to ClickHouse on four triggers: row count, byte
// size, a processing-time timer, and the checkpoint barrier.
//
// The writer holds no state across flushes — everything buffered is drained by
// flush() — so this sink needs no committer and no snapshotState.
final class ClickHouseSinkWriter<T> implements SinkWriter<T> {
    private final String table;
    private final ClickHouseInserter inserter;
    private final BatchBuffer buffer;
    private final ObjectMapper objectMapper;
    private final int maxRetries;
    private final long initialBackoffMillis;
    private final long flushIntervalMillis;
    private final ProcessingTimeService timeService;
    private final SinkMetrics metrics;

    ClickHouseSinkWriter(String table, ClickHouseInserter inserter, BatchBuffer buffer,
                         ObjectMapper objectMapper, int maxRetries, long initialBackoffMillis,
                         long flushIntervalMillis, ProcessingTimeService timeService, SinkMetrics metrics) {
        this.table = table;
        this.inserter = inserter;
        this.buffer = buffer;
        this.objectMapper = objectMapper;
        this.maxRetries = maxRetries;
        this.initialBackoffMillis = initialBackoffMillis;
        this.flushIntervalMillis = flushIntervalMillis;
        this.timeService = timeService;
        this.metrics = metrics;

        // Arm the latency trigger immediately, so a trickle of rows still reaches
        // ClickHouse within a second instead of waiting for a full batch.
        registerFlushTimer();
    }

    @Override
    public void write(T element, Context context) throws IOException {
        // Rows are buffered as their final JSONEachRow text, so the retry loop
        // re-sends bytes rather than re-serializing objects.
        if (buffer.add(objectMapper.writeValueAsString(element))) {
            flushBuffer();
        }
    }

    // Called by Flink when the checkpoint barrier reaches this writer, and again
    // at end of input.
    //
    // This is the whole delivery contract: if the insert fails, this throws, the
    // checkpoint fails, and the Kafka offsets in that checkpoint never advance.
    // The job restarts and replays, and ReplacingMergeTree absorbs the duplicates.
    @Override
    public void flush(boolean endOfInput) throws IOException {
        flushBuffer();
    }

    @Override
    public void close() {
        // Releases the live HTTP client. Nothing buffered survives a close — the
        // caller is expected to have flushed (via the checkpoint or end-of-input
        // path) before the writer is torn down.
        inserter.close();
    }

    // Re-arms itself after every firing, so the latency trigger keeps running for
    // the life of the subtask.
    private void registerFlushTimer() {
        timeService.registerTimer(
            timeService.getCurrentProcessingTime() + flushIntervalMillis,
            timestamp -> {
                flushBuffer();
                registerFlushTimer();
            });
    }

    private void flushBuffer() throws IOException {
        // Nothing buffered — an idle timer firing, or a checkpoint on a quiet
        // subtask, has nothing to send. Skip straight past the retry loop below.
        if (buffer.isEmpty()) {
            return;
        }

        // Drain first: the batch is now owned by this call, and new rows arriving
        // during the retry loop accumulate separately.
        List<String> batch = buffer.drain();

        // Recompute the batch's byte size for the metrics gauge: drain() already
        // reset the buffer's own running total back to zero.
        long batchBytes = 0;
        for (String line : batch) {
            batchBytes += line.getBytes(StandardCharsets.UTF_8).length + 1;
        }

        // Tracks elapsed time for the latency gauge, and the most recent failure
        // so it can be attached as the cause if every attempt is exhausted.
        long startNanos = System.nanoTime();
        Exception lastFailure = null;

        // One initial attempt plus maxRetries retries, backing off exponentially.
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                inserter.insert(table, batch);
                metrics.recordBatch(batch.size(), batchBytes, (System.nanoTime() - startNanos) / 1_000_000L);
                return;
            } catch (Exception e) {
                // Record the failed attempt, then back off before retrying — unless
                // this was the last allowed attempt, in which case sleeping would
                // only delay the exception thrown below for no benefit.
                lastFailure = e;
                metrics.recordFailure();

                if (attempt < maxRetries) {
                    sleepBackoff(initialBackoffMillis << attempt);
                }
            }
        }

        // Retries are bounded on purpose. Retrying forever here would turn a
        // ClickHouse outage into silent backpressure on the archive job; throwing
        // turns it into a job restart, the restart strategy backs off, and the
        // failure becomes visible Kafka lag instead of growing heap.
        throw new IOException("ClickHouse insert into " + table + " failed after "
            + (maxRetries + 1) + " attempts", lastFailure);
    }

    private void sleepBackoff(long millis) {
        // A blocking sleep is acceptable here: it runs on the sink writer's own
        // call stack between bounded retries, and every path out of it ends in
        // either a successful insert or an exception that restarts the job.
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // A cancelling task must not be delayed by our backoff.
            Thread.currentThread().interrupt();
        }
    }
}
