package io.netsecml.platform.adapter.clickhouse.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.batch.BatchBuffer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

// Flink Sink V2 that batches rows of type T into one ClickHouse table.
//
// Transport-agnostic on purpose: this class knows nothing about Kafka. T is any
// Jackson-serializable row whose property names are the table's column names.
public final class ClickHouseBatchSink<T> implements Sink<T> {

    // Initial values from FINAL_ARCHITECTURE.md Step 8, to be benchmarked rather
    // than treated as settled. They are constructor parameters for that reason.
    public static final int DEFAULT_MAX_ROWS = 5_000;
    public static final long DEFAULT_MAX_BYTES = 4L * 1024 * 1024;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 1_000L;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 200L;

    private final String table;
    private final ClickHouseConfig config;
    private final ClickHouseInserterFactory inserterFactory;
    private final int maxRows;
    private final long maxBytes;
    private final long flushIntervalMillis;
    private final int maxRetries;
    private final long initialBackoffMillis;

    // Production entry point: the real ClickHouse client plus the tuned defaults.
    public ClickHouseBatchSink(String table, ClickHouseConfig config) {
        this(table, config, ClientV2Inserter::new, DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES,
            DEFAULT_FLUSH_INTERVAL_MILLIS, DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_BACKOFF_MILLIS);
    }

    // Full constructor: lets tests substitute a fake inserter factory and tighter
    // triggers/backoff. Every parameter here must stay Serializable (or, for the
    // factory, produce its live object lazily) because this constructor runs
    // while the job graph is being built, not on the subtask that executes it.
    public ClickHouseBatchSink(String table, ClickHouseConfig config, ClickHouseInserterFactory inserterFactory,
                               int maxRows, long maxBytes, long flushIntervalMillis,
                               int maxRetries, long initialBackoffMillis) {
        this.table = table;
        this.config = config;
        this.inserterFactory = inserterFactory;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.flushIntervalMillis = flushIntervalMillis;
        this.maxRetries = maxRetries;
        this.initialBackoffMillis = initialBackoffMillis;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        // The live HTTP client is built here, on the subtask that will use it —
        // only the Serializable config and factory crossed the job graph.
        return new ClickHouseSinkWriter<>(
            table,
            inserterFactory.create(config),
            new BatchBuffer(maxRows, maxBytes),
            new ObjectMapper(),
            maxRetries,
            initialBackoffMillis,
            flushIntervalMillis,
            context.getProcessingTimeService(),
            new FlinkSinkMetrics(context.metricGroup(), table));
    }
}
