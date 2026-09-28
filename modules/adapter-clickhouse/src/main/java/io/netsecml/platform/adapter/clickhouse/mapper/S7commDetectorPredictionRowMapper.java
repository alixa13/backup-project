package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;

import java.io.Serializable;

// A prediction as one row. row_version is producedAt, as for the Modbus
// predictions, so a replay's later copy wins the ReplacingMergeTree merge.
public final class S7commDetectorPredictionRowMapper implements Serializable {

    public S7commDetectorPredictionRow toRow(S7commDetectorPrediction p) {
        return new S7commDetectorPredictionRow(
            p.predictionId(), p.eventId(), ClickHouseTimestamps.format(p.eventTime()), p.sensor().value(),
            p.connectionUid(), p.clientIp(), p.serverIp(), p.modelName(), p.modelVersion(), p.modelSha(),
            p.schemaId(), p.schemaHash(), p.verdict().name(), p.score(), p.pValue(), p.scoreGroup().name(),
            p.alpha(), p.eventsSinceReset(), p.qualityFlags(), p.inferenceMicros(),
            ClickHouseTimestamps.format(p.producedAt()));
    }
}
