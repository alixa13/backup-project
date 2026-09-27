-- Modbus Stage 1 detector predictions (docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md,
-- section 6): one row per Modbus event, from netsec.modbus.prediction.v1. The
-- existing predictions table's rules -- ReplacingMergeTree, the same ORDER BY
-- and 180-day TTL -- so a replayed prediction collapses into one row. Scores
-- are null unless the verdict is NORMAL or ANOMALY. Idempotent.
CREATE TABLE IF NOT EXISTS modbus_detector_predictions (
  prediction_id      FixedString(64),
  event_id           String,
  event_time         DateTime64(3, 'UTC'),
  sensor             LowCardinality(String),
  connection_uid     String,
  client_ip          String,
  server_ip          String,
  unit_id            LowCardinality(String),
  model_name         LowCardinality(String),
  model_version      LowCardinality(String),
  model_sha          FixedString(64),
  schema_id          LowCardinality(String),
  schema_hash        FixedString(64),
  verdict            LowCardinality(String),
  dense_score        Nullable(Float32),
  temporal_score     Nullable(Float32),
  dense_threshold    Float32,
  temporal_threshold Float32,
  trigger            LowCardinality(String),
  window_events      UInt8,
  quality_flags      UInt32,
  inference_us       UInt32,
  created_at         DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version        DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (model_name, model_version, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY;
