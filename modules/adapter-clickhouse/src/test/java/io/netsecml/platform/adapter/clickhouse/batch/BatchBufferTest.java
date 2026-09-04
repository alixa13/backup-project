package io.netsecml.platform.adapter.clickhouse.batch;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BatchBufferTest {
    // A line whose UTF-8 length is exactly 10 bytes, so byte thresholds are easy
    // to reason about below.
    private static final String TEN_BYTE_LINE = "0123456789";

    // Locks down that a fresh buffer has no rows, is not full, and reports zero bytes.
    @Test
    void startsEmpty() {
        BatchBuffer buffer = new BatchBuffer(5, 1000);

        assertTrue(buffer.isEmpty());
        assertFalse(buffer.isFull());
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.byteSize());
    }

    // The row-count trigger. add() reports the trigger so the caller flushes
    // without having to ask twice.
    @Test
    void signalsFullAtTheRowLimit() {
        BatchBuffer buffer = new BatchBuffer(3, 1_000_000);

        assertFalse(buffer.add("a"));
        assertFalse(buffer.add("b"));
        assertTrue(buffer.add("c"), "third row reaches the 3-row limit");
        assertTrue(buffer.isFull());
    }

    // The byte trigger. Each line counts its UTF-8 length plus the newline that
    // separates JSONEachRow records.
    @Test
    void signalsFullAtTheByteLimit() {
        BatchBuffer buffer = new BatchBuffer(1_000_000, 22);

        assertFalse(buffer.add(TEN_BYTE_LINE), "11 bytes buffered, under the limit");
        assertTrue(buffer.add(TEN_BYTE_LINE), "22 bytes buffered, reaches the limit");
        assertEquals(22, buffer.byteSize());
    }

    // Byte accounting must be UTF-8, not character count, or a batch of non-ASCII
    // detail strings would silently exceed the configured payload bound.
    @Test
    void countsBytesNotCharacters() {
        BatchBuffer buffer = new BatchBuffer(1_000_000, 1_000_000);

        // Two 3-byte UTF-8 characters plus the newline.
        buffer.add("中文");

        assertEquals(7, buffer.byteSize());
    }

    // Locks down that drain returns all buffered rows and resets both line and byte counts.
    @Test
    void drainReturnsEverythingAndEmptiesTheBuffer() {
        BatchBuffer buffer = new BatchBuffer(10, 1_000_000);
        buffer.add("a");
        buffer.add("b");

        assertEquals(List.of("a", "b"), buffer.drain());
        assertTrue(buffer.isEmpty());
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.byteSize(), "byte accounting resets with the rows");
        assertFalse(buffer.isFull());
    }

    // Locks down that drain on a never-filled buffer returns an empty list, not null.
    @Test
    void drainOnAnEmptyBufferReturnsAnEmptyList() {
        assertEquals(List.of(), new BatchBuffer(10, 1000).drain());
    }

    // The drained list must not alias internal state — the sink holds it across a
    // retry loop while new rows may already be arriving.
    @Test
    void drainedListIsIndependentOfLaterAdds() {
        BatchBuffer buffer = new BatchBuffer(10, 1_000_000);
        buffer.add("a");

        List<String> drained = buffer.drain();
        buffer.add("b");

        assertEquals(List.of("a"), drained);
    }

    // Locks down that a maxRows of 0 would make isFull() always true, causing every
    // row to flush immediately and defeat the purpose of batching; likewise for maxBytes.
    @Test
    void rejectsNonPositiveLimits() {
        assertThrows(IllegalArgumentException.class, () -> new BatchBuffer(0, 1000));
        assertThrows(IllegalArgumentException.class, () -> new BatchBuffer(10, 0));
    }
}
