package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import io.netsecml.platform.domain.feature.RecordTimingState;
import io.netsecml.platform.domain.feature.SourceWindowState;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// The common tier's construction. Every per-protocol schema embeds this block,
// so an index error here would misalign all of them at once.
class CommonFeatureExtractorTest {

    private static final Instant START = Instant.parse("2026-09-10T10:00:00Z");

    private static SourceWindowState windowWithThreeRecords() {
        long minute = START.getEpochSecond() / 60L;
        return SourceWindowState.empty()
            .record(minute, 100L, false)
            .record(minute, 200L, true)
            .record(minute, 300L, false);
    }

    private static RecordTimingState evenlySpacedTiming() {
        return RecordTimingState.empty()
            .observe(START)
            .observe(START.plusMillis(100))
            .observe(START.plusMillis(200));
    }

    // The vector's width is the contract. A schema that embeds this tier computes
    // its own offsets from FEATURE_COUNT, so a wrong length corrupts every
    // protocol feature that follows it.
    @Test
    void producesExactlyTheFrozenFeatureCount() {
        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);

        assertEquals(CommonFeatureTierV1.FEATURE_COUNT, values.length);
    }

    // Indices 0-5 come from ml-platform's own state and are always present.
    @Test
    void mapsWindowAndTimingToTheirFrozenIndices() {
        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);

        assertEquals(3.0f, values[0], 0.0001f, "record_count_5m");
        assertEquals(600.0f, values[1], 0.0001f, "byte_sum_5m");
        assertEquals(1.0f, values[2], 0.0001f, "failed_count_5m");
        assertEquals(100.0f, values[3], 0.0001f, "inter_arrival_mean_ms");
        assertEquals(0.0f, values[4], 0.0001f, "inter_arrival_stddev_ms");
        assertEquals(1.0f, values[5], 0.0001f, "is_orig");
    }

    // The non-blocking left join: no snapshot means zeroes, and index 11 records
    // that they are absent rather than measured.
    @Test
    void absentEnrichmentYieldsZeroesAndClearsThePresenceFlag() {
        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), false, null);

        assertEquals(0.0f, values[6], 0.0001f, "conn_orig_bytes");
        assertEquals(0.0f, values[7], 0.0001f, "conn_resp_bytes");
        assertEquals(0.0f, values[8], 0.0001f, "conn_orig_pkts");
        assertEquals(0.0f, values[9], 0.0001f, "conn_resp_pkts");
        assertEquals(0.0f, values[10], 0.0001f, "conn_age_seconds");
        assertEquals(0.0f, values[11], 0.0001f, "conn_enrichment_present must be 0 when absent");
    }

    // A present snapshot populates 6-10 and sets the flag. Without the flag a
    // model could not tell a genuinely idle connection from one whose first
    // snapshot has not been emitted yet.
    @Test
    void presentEnrichmentPopulatesItsIndicesAndSetsTheFlag() {
        ConnSnapshotDelta delta = new ConnSnapshotDelta(500L, 600L, 4L, 7L, 600L);

        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, delta);

        assertEquals(500.0f, values[6], 0.0001f, "conn_orig_bytes");
        assertEquals(600.0f, values[7], 0.0001f, "conn_resp_bytes");
        assertEquals(4.0f, values[8], 0.0001f, "conn_orig_pkts");
        assertEquals(7.0f, values[9], 0.0001f, "conn_resp_pkts");
        assertEquals(600.0f, values[10], 0.0001f, "conn_age_seconds");
        assertEquals(1.0f, values[11], 0.0001f, "conn_enrichment_present must be 1 when present");
    }

    // A zero-valued snapshot is NOT the same as an absent one, and the flag is
    // the only thing that distinguishes them. This is the case the flag exists
    // for, so it is asserted directly rather than implied.
    @Test
    void aZeroValuedSnapshotIsDistinguishableFromAnAbsentOne() {
        ConnSnapshotDelta idle = new ConnSnapshotDelta(0L, 0L, 0L, 0L, 600L);

        float[] present = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, idle);
        float[] absent = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);

        assertEquals(0.0f, present[6], 0.0001f);
        assertEquals(0.0f, absent[6], 0.0001f);
        assertNotEquals(present[11], absent[11],
            "an idle connection and a missing snapshot must not look identical");
    }

    // is_orig is a direction flag, not a count. Both values are pinned so a
    // boolean-to-float slip cannot pass.
    @Test
    void isOrigIsEncodedAsZeroOrOne() {
        float[] originator = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);
        float[] responder = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), false, null);

        assertEquals(1.0f, originator[5], 0.0001f);
        assertEquals(0.0f, responder[5], 0.0001f);
    }
}
