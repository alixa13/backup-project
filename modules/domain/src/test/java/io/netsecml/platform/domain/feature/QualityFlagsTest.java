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
    // silently merge two different provenance facts into one flag value.
    @Test
    void theTwoAbsenceBitsAreDistinctAndIndependentlyCombinable() {
        assertEquals(1, QualityFlags.CONN_ENRICHMENT_ABSENT);
        assertEquals(2, QualityFlags.DNS_RESPONSE_ABSENT);

        int both = QualityFlags.CONN_ENRICHMENT_ABSENT | QualityFlags.DNS_RESPONSE_ABSENT;
        assertEquals(3, both, "the two bits must not overlap");

        // Each bit must be independently recoverable from the combined value --
        // this is what lets a ClickHouse query filter on one condition without
        // caring about the other.
        assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT, both & QualityFlags.CONN_ENRICHMENT_ABSENT);
        assertEquals(QualityFlags.DNS_RESPONSE_ABSENT, both & QualityFlags.DNS_RESPONSE_ABSENT);
    }
}
