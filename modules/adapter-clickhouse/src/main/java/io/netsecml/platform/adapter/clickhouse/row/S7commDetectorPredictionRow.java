package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;

// One s7comm_detector_predictions row, column for column (created_at is the
// server's default). SchemaDriftTest pins these names to the DDL.
public record S7commDetectorPredictionRow(
    @JsonProperty("prediction_id") String predictionId,
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("sensor") String sensor,
    @JsonProperty("connection_uid") String connectionUid,
    @JsonProperty("client_ip") String clientIp,
    @JsonProperty("server_ip") String serverIp,
    @JsonProperty("model_name") String modelName,
    @JsonProperty("model_version") String modelVersion,
    @JsonProperty("model_sha") String modelSha,
    @JsonProperty("schema_id") String schemaId,
    @JsonProperty("schema_hash") String schemaHash,
    @JsonProperty("verdict") String verdict,
    @JsonProperty("score") Float score,
    @JsonProperty("p_value") Double pValue,
    @JsonProperty("score_group") String scoreGroup,
    @JsonProperty("alpha") double alpha,
    @JsonProperty("events_since_reset") long eventsSinceReset,
    @JsonProperty("quality_flags") int qualityFlags,
    @JsonProperty("inference_us") long inferenceUs,
    @JsonProperty("row_version") String rowVersion) {
}
