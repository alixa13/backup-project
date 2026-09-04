package io.netsecml.platform.adapter.clickhouse.batch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

// Accumulates encoded JSONEachRow lines until a bounded flush trigger fires.
//
// Deliberately knows nothing about Flink, ClickHouse or time: the row and byte
// limits are the only triggers it owns, and the 1-second timer lives in the sink
// writer, which has Flink's ProcessingTimeService. That split is what makes every
// threshold here testable without a runtime.
//
// The bounds are also the memory bound. A ClickHouse outage must produce Kafka
// lag, never unbounded heap growth, and the buffer can never exceed one batch
// because the writer flushes or throws the moment a trigger fires.
public final class BatchBuffer {
    private final int maxRows;
    private final long maxBytes;
    private final List<String> lines = new ArrayList<>();

    // Running UTF-8 size of the serialized batch, including the newline that
    // separates JSONEachRow records.
    private long bytes;

    public BatchBuffer(int maxRows, long maxBytes) {
        // Validate that both bounds are positive; a zero or negative limit would
        // allow unlimited buffering or cause immediate flush loops.
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive, was " + maxRows);
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, was " + maxBytes);
        }
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
    }

    // Buffers one line. Returns true when a flush trigger has been reached, so
    // the caller can act without a second call.
    public boolean add(String jsonLine) {
        // Append the JSON-encoded line to the buffer.
        lines.add(jsonLine);

        // UTF-8 length, not character count: a batch of non-ASCII detail strings
        // would otherwise slip past the configured payload bound. The +1 accounts
        // for the newline that separates JSONEachRow records.
        bytes += jsonLine.getBytes(StandardCharsets.UTF_8).length + 1;

        // Immediately report whether a flush trigger (row count or byte size) has
        // been reached, allowing the caller to respond without a second poll.
        return isFull();
    }

    // Checks whether either flush trigger (row count or total byte size) has been
    // exceeded. Used by add() to signal flush, and by the sink writer to decide
    // whether to force a flush after the 1-second timer fires.
    public boolean isFull() {
        return lines.size() >= maxRows || bytes >= maxBytes;
    }

    // Reports whether the buffer contains no lines.
    public boolean isEmpty() {
        return lines.isEmpty();
    }

    // Returns the current number of buffered lines.
    public int size() {
        return lines.size();
    }

    // Returns the current total byte size of the batch (UTF-8 line lengths plus
    // newlines), which serves as a proxy for the uncompressed ClickHouse payload.
    public long byteSize() {
        return bytes;
    }

    // Removes and returns everything buffered. The returned list is a snapshot:
    // the sink holds it across a retry loop while new rows may already be arriving.
    // Using List.copyOf() ensures that the returned list is immutable and does not
    // alias the internal list, protecting against concurrent mutation during retries.
    public List<String> drain() {
        // Create an immutable snapshot of all buffered lines.
        List<String> drained = List.copyOf(lines);
        // Clear the internal buffer and reset byte accounting.
        lines.clear();
        bytes = 0;
        // Return the snapshot, which is safe to hold across retries.
        return drained;
    }
}
