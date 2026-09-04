package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;

// One feature_vectors row.
//
// The @JsonProperty names ARE the ClickHouse column names: the sink serializes
// this record straight to a JSONEachRow line, so renaming a property renames a
// column in the insert.
//
// Timestamps are already formatted strings, not Instants — ClickHouse parses
// DateTime64(3) from 'yyyy-MM-dd HH:mm:ss.SSS', and doing the conversion in the
// mapper keeps the serializer free of date logic.
//
// archived_at is absent on purpose: that column carries DEFAULT now64(3) and is
// set server-side, so sending it would only let the client disagree with the server.
public record FeatureVectorRow(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("sensor") String sensor,
    @JsonProperty("log_type") String logType,
    @JsonProperty("connection_uid") String connectionUid,
    @JsonProperty("schema_id") String schemaId,
    @JsonProperty("schema_hash") String schemaHash,
    @JsonProperty("values") float[] values,
    @JsonProperty("quality_flags") int qualityFlags,
    @JsonProperty("row_version") String rowVersion) {

    public FeatureVectorRow {
        // Defensive copy in: the row outlives the call that built it, travelling
        // in a buffer until the next flush.
        values = Arrays.copyOf(values, values.length);
    }

    // Defensive copy out.
    @Override
    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }
}
