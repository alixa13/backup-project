package io.netsecml.platform.adapter.clickhouse.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.batch.BatchBuffer;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import static org.junit.jupiter.api.Assertions.*;

// The retry and flush logic, exercised with no server and no Docker. That is the
// entire point of the ClickHouseInserter seam.
class ClickHouseSinkWriterTest {

    // Records every batch it is handed, and fails on demand.
    private static final class FakeInserter implements ClickHouseInserter {
        final List<List<String>> attempts = new ArrayList<>();
        int failuresRemaining;
        boolean closed;

        @Override
        public void insert(String table, List<String> jsonLines) throws Exception {
            attempts.add(List.copyOf(jsonLines));
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new IOException("simulated ClickHouse failure");
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    // Captures registered timers so a test can fire them deliberately rather than
    // sleeping on wall-clock time.
    private static final class FakeProcessingTimeService implements ProcessingTimeService {
        final List<ProcessingTimeCallback> pending = new ArrayList<>();
        long now;

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public ScheduledFuture<?> registerTimer(long time, ProcessingTimeCallback target) {
            pending.add(target);
            return null;
        }

        void fireOldestTimer() throws Exception {
            pending.remove(0).onProcessingTime(now);
        }
    }

    private static final class RecordingMetrics implements SinkMetrics {
        int batches;
        int failures;
        int lastRows;

        @Override
        public void recordBatch(int rows, long bytes, long latencyMillis) {
            batches++;
            lastRows = rows;
        }

        @Override
        public void recordFailure() {
            failures++;
        }
    }

    private final FakeInserter inserter = new FakeInserter();
    private final FakeProcessingTimeService timeService = new FakeProcessingTimeService();
    private final RecordingMetrics metrics = new RecordingMetrics();

    // Backoff is 1 ms in tests: the production 200/400/800 ms would add 1.4 s to
    // every retry assertion for no extra coverage.
    private ClickHouseSinkWriter<Map<String, Object>> writer(int maxRows) {
        return new ClickHouseSinkWriter<>("feature_vectors", inserter, new BatchBuffer(maxRows, 1_000_000),
            new ObjectMapper(), 3, 1L, 1_000L, timeService, metrics);
    }

    private Map<String, Object> row(String id) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("event_id", id);
        return row;
    }

    // The checkpoint-barrier path: flush() drains whatever is buffered.
    @Test
    void flushSendsBufferedRows() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);
        writer.write(row("b"), null);

        assertEquals(0, inserter.attempts.size(), "nothing is sent before a trigger fires");

        writer.flush(false);

        assertEquals(1, inserter.attempts.size());
        assertEquals(2, inserter.attempts.get(0).size());
        assertTrue(inserter.attempts.get(0).get(0).contains("\"event_id\":\"a\""));
        assertEquals(2, metrics.lastRows);
    }

    // The row-count trigger fires inside write(), without waiting for a checkpoint.
    @Test
    void rowLimitTriggersAnImmediateFlush() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(2);
        writer.write(row("a"), null);
        assertEquals(0, inserter.attempts.size());

        writer.write(row("b"), null);

        assertEquals(1, inserter.attempts.size(), "the second row reaches the 2-row limit");
        assertEquals(2, inserter.attempts.get(0).size());
    }

    // The 1-second trigger, driven deterministically.
    @Test
    void processingTimeTimerFlushesAndReschedules() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        assertEquals(1, timeService.pending.size(), "a flush timer is registered at construction");
        timeService.fireOldestTimer();

        assertEquals(1, inserter.attempts.size());
        assertEquals(1, timeService.pending.size(), "the timer re-arms itself after firing");
    }

    // Transient failures must not lose the batch: the same content is retried.
    @Test
    void retriesTheSameBatchThenSucceeds() throws Exception {
        inserter.failuresRemaining = 2;
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        writer.flush(false);

        assertEquals(3, inserter.attempts.size(), "two failures then one success");
        assertEquals(inserter.attempts.get(0), inserter.attempts.get(2), "the retried batch is unchanged");
        assertEquals(2, metrics.failures);
        assertEquals(1, metrics.batches, "only the successful attempt counts as a batch");
    }

    // The load-bearing behaviour. Exhausted retries throw, which fails the
    // checkpoint, which keeps the Kafka offsets where they are.
    @Test
    void throwsAfterExhaustingRetries() throws Exception {
        inserter.failuresRemaining = Integer.MAX_VALUE;
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        IOException thrown = assertThrows(IOException.class, () -> writer.flush(false));

        assertTrue(thrown.getMessage().contains("feature_vectors"), "the error names the table");
        assertEquals(4, inserter.attempts.size(), "one initial attempt plus three retries");
        assertEquals(4, metrics.failures);
        assertEquals(0, metrics.batches, "a batch that never landed is not counted as written");
    }

    // A batch is never dropped silently — the failure has to reach the caller.
    @Test
    void neverSwallowsAFailedBatch() throws Exception {
        inserter.failuresRemaining = Integer.MAX_VALUE;
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        assertThrows(IOException.class, () -> writer.flush(false));
        for (List<String> attempt : inserter.attempts) {
            assertEquals(1, attempt.size(), "every attempt carried the full batch");
        }
    }

    @Test
    void flushOnAnEmptyBufferDoesNothing() throws Exception {
        writer(1000).flush(false);

        assertEquals(0, inserter.attempts.size());
    }

    @Test
    void closeReleasesTheInserter() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);

        writer.close();

        assertTrue(inserter.closed);
    }
}
