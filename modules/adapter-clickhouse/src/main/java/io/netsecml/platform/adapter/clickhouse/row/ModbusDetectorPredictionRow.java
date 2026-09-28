package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;

// One modbus_detector_predictions row, column for column (created_at is the
// server's default). SchemaDriftTest pins these names to the DDL.
public record ModbusDetectorPredictionRow(
    @JsonProperty("prediction_id") String predictionId,
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("sensor") String sensor,
    @JsonProperty("connection_uid") String connectionUid,
    @JsonProperty("client_ip") String clientIp,
    @JsonProperty("server_ip") String serverIp,
    @JsonProperty("unit_id") String unitId,
    @JsonProperty("model_name") String modelName,
    @JsonProperty("model_version") String modelVersion,
    @JsonProperty("model_sha") String modelSha,
    @JsonProperty("schema_id") String schemaId,
    @JsonProperty("schema_hash") String schemaHash,
    @JsonProperty("verdict") String verdict,
    @JsonProperty("dense_score") Float denseScore,
    @JsonProperty("temporal_score") Float temporalScore,
    @JsonProperty("dense_threshold") float denseThreshold,
    @JsonProperty("temporal_threshold") float temporalThreshold,
    @JsonProperty("trigger") String trigger,
    @JsonProperty("window_events") int windowEvents,
    @JsonProperty("quality_flags") int qualityFlags,
    @JsonProperty("inference_us") long inferenceUs,
    @JsonProperty("row_version") String rowVersion) {
}
