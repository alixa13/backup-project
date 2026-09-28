package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// A prediction becomes one row: ClickHouse timestamps, enum names, nulls kept.
class S7commDetectorPredictionRowMapperTest {

    @Test
    void aWarmupMapsWithANullScoreAndPValue() {
        S7commDetectorPrediction p = new S7commDetectorPrediction("a".repeat(64), "s:C1:25:REQUEST:1",
            Instant.parse("2026-09-28T12:00:00.123Z"), new SensorId("s"), "C1", "10.0.0.5", "10.0.0.9",
            "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1", "c".repeat(64),
            DetectorVerdict.WARMUP, null, null, S7commScoreGroup.WRITE_REQUEST, 0.015625, 7, 16, 0L,
            Instant.parse("2026-09-28T12:00:00.456Z"));
        S7commDetectorPredictionRow row = new S7commDetectorPredictionRowMapper().toRow(p);
        assertEquals("2026-09-28 12:00:00.123", row.eventTime());
        assertEquals("2026-09-28 12:00:00.456", row.rowVersion(), "row_version is producedAt");
        assertEquals("WARMUP", row.verdict());
        assertEquals("WRITE_REQUEST", row.scoreGroup());
        assertNull(row.score());
        assertNull(row.pValue());
        assertEquals(0.015625, row.alpha());
        assertEquals(7, row.eventsSinceReset());
        assertEquals("10.0.0.5", row.clientIp());
    }
}
