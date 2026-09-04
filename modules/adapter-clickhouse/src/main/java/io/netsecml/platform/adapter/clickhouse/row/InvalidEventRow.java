package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;

// One invalid_events row. As with FeatureVectorRow, the @JsonProperty names are
// the ClickHouse column names.
//
// eventTime is null for every rejection this pipeline currently produces — the
// column is Nullable(DateTime64(3,'UTC')) and is reserved for a future map-stage
// improvement that recovers the source timestamp.
//
// created_at is absent on purpose: server-side DEFAULT now64(3).
public record InvalidEventRow(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("received_at") String receivedAt,
    @JsonProperty("stage") String stage,
    @JsonProperty("reason_code") String reasonCode,
    @JsonProperty("detail") String detail,
    @JsonProperty("source_version") String sourceVersion,
    @JsonProperty("raw_payload_hash") String rawPayloadHash) {
}
