package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Pins LongRing against Python's deque(maxlen=n) and the three statistics
// upstream's builders take over one (_ratio_true, _unique_ratio,
// _normalized_entropy in two-models-info/S7___/customer_icsnpp_enriched_builder.py).
class LongRingTest {

    private static LongRing of(int capacity, long... values) {
        LongRing ring = new LongRing(capacity);
        for (long value : values) {
            ring.add(value);
        }
        return ring;
    }

    @Test
    void aFullRingDropsItsOldestEntryLikeADequeWithMaxlen() {
        LongRing ring = of(3, 1, 2, 3, 4);
        assertEquals(3, ring.size());
        assertEquals(2, ring.get(0), "1 was dropped");
        assertEquals(4, ring.get(2));
        assertThrows(IndexOutOfBoundsException.class, () -> ring.get(3));
    }

    @Test
    void meanIsSumOverSizeAndZeroWhenEmpty() {
        assertEquals(0.0, of(16).meanOrZero());
        assertEquals(2.0 / 3.0, of(16, 1, 1, 0).meanOrZero());
    }

    @Test
    void distinctRatioIsDistinctOverSizeAndZeroWhenEmpty() {
        assertEquals(0.0, of(32).distinctRatioOrZero());
        assertEquals(0.5, of(32, 7, 7, 8, 8).distinctRatioOrZero());
    }

    @Test
    void entropyFollowsNormalizedEntropy() {
        // <= 1 value: 0. Two equally likely values: 1 bit / log2(2) = 1.
        assertEquals(0.0, of(16, 4).normalizedEntropy(16));
        // One distinct value in n >= 2: upstream computes -sum(...) over a single
        // 0.0 term, which is -0.0 -- and the float32 vector carries that sign, so
        // the exact value is pinned, sign included (JUnit compares doubles by bits).
        assertEquals(-0.0, of(16, 4, 4, 4).normalizedEntropy(16), "one distinct value: -0.0, as upstream");
        assertEquals(1.0, of(16, 4, 5).normalizedEntropy(16));
        assertEquals(0.5, of(16, 4, 5, 4, 5).normalizedEntropy(16),
            "1 bit of entropy over log2(min(16, 4)) = 2");
        assertEquals(1.0, of(16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15).normalizedEntropy(16),
            "16 distinct values: log2(16) / log2(16)");
    }
}
