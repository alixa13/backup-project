package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.ModbusDetectorPredictionRow;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// A prediction becomes one row: ClickHouse timestamps, enum names, null scores kept.
class ModbusDetectorPredictionRowMapperTest {

    @Test
    void aWarmupMapsWithNullScores() {
        ModbusDetectorPrediction p = new ModbusDetectorPrediction("a".repeat(64), "s:u:7:REQUEST:1",
            Instant.parse("2026-09-26T12:00:00.123Z"), new SensorId("s"), "u", "10.0.0.5", "10.0.0.9", "1",
            "modbus-stage1-detector", "v1", "b".repeat(64), "modbus-feature-v1", "c".repeat(64),
            DetectorVerdict.WARMUP, null, null, 0.2483385f, 0.4121148f, DetectorTrigger.NONE, 3, 8, 0L,
            Instant.parse("2026-09-26T12:00:00.456Z"));
        ModbusDetectorPredictionRow row = new ModbusDetectorPredictionRowMapper().toRow(p);
        assertEquals("2026-09-26 12:00:00.123", row.eventTime());
        assertEquals("2026-09-26 12:00:00.456", row.rowVersion(), "row_version is producedAt");
        assertEquals("WARMUP", row.verdict());
        assertEquals("NONE", row.trigger());
        assertNull(row.denseScore());
        assertEquals("s", row.sensor());
        assertEquals(3, row.windowEvents());
    }
}
