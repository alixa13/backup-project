package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.ModbusDetectorPredictionRow;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;

import java.io.Serializable;

// A prediction as one row. row_version is producedAt, as for feature vectors,
// so a replay's later copy wins the ReplacingMergeTree merge.
public final class ModbusDetectorPredictionRowMapper implements Serializable {

    public ModbusDetectorPredictionRow toRow(ModbusDetectorPrediction p) {
        return new ModbusDetectorPredictionRow(
            p.predictionId(), p.eventId(), ClickHouseTimestamps.format(p.eventTime()), p.sensor().value(),
            p.connectionUid(), p.clientIp(), p.serverIp(), p.unitId(), p.modelName(), p.modelVersion(),
            p.modelSha(), p.schemaId(), p.schemaHash(), p.verdict().name(), p.denseScore(), p.temporalScore(),
            p.denseThreshold(), p.temporalThreshold(), p.trigger().name(), p.windowEvents(), p.qualityFlags(),
            p.inferenceMicros(), ClickHouseTimestamps.format(p.producedAt()));
    }
}
