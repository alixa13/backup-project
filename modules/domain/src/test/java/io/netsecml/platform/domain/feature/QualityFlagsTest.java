package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// QualityFlags has no branching logic to exercise -- it is a bag of int
// constants -- but the constants themselves are load-bearing: build() OR's them
// together into one int, so if two bits ever collided the two conditions they
// record would become inseparable on every archived row. That is the one way
// this class can be wrong without a compiler error, so it is the one thing
// pinned here.
class QualityFlagsTest {

    @Test
    void noneIsZero() {
        assertEquals(0, QualityFlags.NONE);
    }

    // Each condition must own a distinct bit, or OR-ing them together would
    // silently merge two different provenance facts into one flag value. This
    // is the exact invariant an earlier draft of this task's own instructions
    // violated -- MODBUS_OUT_OF_ORDER was first specified as 2, which collides
    // with DNS_RESPONSE_ABSENT -- so this test pins the value, not just "some
    // distinct value", for every constant.
    @Test
    void everyBitIsPinnedDistinctAndIndependentlyCombinable() {
        assertEquals(1, QualityFlags.CONN_ENRICHMENT_ABSENT);
        assertEquals(2, QualityFlags.DNS_RESPONSE_ABSENT);
        assertEquals(4, QualityFlags.MODBUS_OUT_OF_ORDER);
        assertEquals(8, QualityFlags.MODBUS_WINDOW_SATURATED);

        int[] bits = {QualityFlags.CONN_ENRICHMENT_ABSENT, QualityFlags.DNS_RESPONSE_ABSENT,
            QualityFlags.MODBUS_OUT_OF_ORDER, QualityFlags.MODBUS_WINDOW_SATURATED};

        // Pairwise distinctness: no two constants may ever share a bit position.
        int all = 0;
        for (int bit : bits) {
            assertEquals(1, Integer.bitCount(bit), "each flag must be a single bit");
            assertEquals(0, all & bit, "flag " + bit + " overlaps an earlier one");
            all |= bit;
        }
        assertEquals(15, all, "all four bits must be distinct and non-overlapping");

        // Each bit must be independently recoverable from the combined value --
        // this is what lets a ClickHouse query filter on one protocol's own
        // condition without the other protocols' bits interfering -- and a
        // value carrying one bit alone must not read back as carrying any other.
        for (int bit : bits) {
            assertEquals(bit, all & bit);
            for (int other : bits) {
                if (other != bit) {
                    assertEquals(0, bit & other, "flag " + bit + " reads back as carrying " + other);
                }
            }
        }
    }
}
