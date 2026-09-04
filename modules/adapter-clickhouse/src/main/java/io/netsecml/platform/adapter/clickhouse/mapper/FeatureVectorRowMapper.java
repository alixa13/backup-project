package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.domain.feature.FeatureVector;
import java.io.Serializable;

// Turns a domain FeatureVector into a feature_vectors row.
//
// Serializable because Flink map functions hold an instance across the network.
// Stateless, so one instance per subtask is enough.
public final class FeatureVectorRowMapper implements Serializable {

    // Copies every FeatureVector field across, unwrapping value objects (SensorId,
    // LogType) to their plain String/wire form and reformatting both Instants for
    // DateTime64(3) — the row itself carries only JSON-serializable primitives.
    public FeatureVectorRow toRow(FeatureVector vector) {
        return new FeatureVectorRow(
            vector.eventId(),
            ClickHouseTimestamps.format(vector.eventTime()),
            vector.sensor().value(),
            vector.logType().wireName(),
            vector.connectionUid(),
            vector.schemaId(),
            vector.schemaHash(),
            vector.values(),
            vector.qualityFlags(),
            // producedAt becomes row_version. After a checkpoint restore the
            // online job re-stamps it, so the replayed emission is strictly newer
            // and wins argMax deduplication — which is correct, because its
            // indices 17-19 came from the restored window state.
            ClickHouseTimestamps.format(vector.producedAt()));
    }
}
