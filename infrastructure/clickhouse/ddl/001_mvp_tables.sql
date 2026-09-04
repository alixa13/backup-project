-- ClickHouse MVP tables for the network-security ML platform.
--
-- Applied by scripts/database/apply-ddl.sh and, unchanged, by the Testcontainers
-- integration tests. There is deliberately no second copy of this schema.
--
-- Every statement is CREATE ... IF NOT EXISTS, so re-applying is a no-op.
-- Table names are unqualified: the target database comes from CLICKHOUSE_DATABASE.
--
-- The runner splits this file on the semicolon after stripping line comments, so
-- no statement may contain a semicolon inside a string literal.
--
-- `values` is backtick-quoted everywhere because VALUES is INSERT syntax.

-- feature_vectors: the training source, and the only table this job writes on the
-- happy path. row_version is the producer's producedAt, so a replayed emission is
-- strictly newer and wins deduplication. archived_at is server-set on insert.
CREATE TABLE IF NOT EXISTS feature_vectors (
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  sensor        LowCardinality(String),
  log_type      LowCardinality(String),
  connection_uid String,
  schema_id     LowCardinality(String),
  schema_hash   FixedString(64),
  `values`      Array(Float32),
  quality_flags UInt32,
  archived_at   DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (schema_hash, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

-- invalid_events: low-volume forensic quarantine. Plain MergeTree, because a
-- replay legitimately re-rejects and re-stamps a record; duplicates are expected
-- after failures and are deduplicated by raw_payload_hash at query time.
--
-- event_id is populated for MAP-stage rejections, where a DTO parsed before
-- domain validation refused it, and is '' for PARSE-stage. event_time is
-- reserved for a future map-stage improvement and is null today.
CREATE TABLE IF NOT EXISTS invalid_events (
  event_id         String,
  event_time       Nullable(DateTime64(3, 'UTC')),
  received_at      DateTime64(3, 'UTC'),
  stage            LowCardinality(String),
  reason_code      LowCardinality(String),
  detail           String,
  source_version   LowCardinality(String),
  raw_payload_hash FixedString(64),
  created_at       DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (stage, received_at, raw_payload_hash)
TTL toDateTime(received_at) + INTERVAL 30 DAY;

-- predictions: created now, first written on Roadmap Day 9 when inference lands.
CREATE TABLE IF NOT EXISTS predictions (
  prediction_id FixedString(64),
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  model_name    LowCardinality(String),
  model_version LowCardinality(String),
  model_sha     FixedString(64),
  schema_hash   FixedString(64),
  score         Float32,
  decision      UInt8,
  threshold     Float32,
  inference_us  UInt32,
  quality_flags UInt32,
  created_at    DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (model_name, model_version, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY;

-- network_events: optional normalized-event audit. Created but never written by
-- this plan — the online job has no normalized-event sink. This is the only
-- IP-bearing table; enabling it requires a least-privilege ClickHouse user and a
-- documented retention approval first. See docs/clickhouse.md.
CREATE TABLE IF NOT EXISTS network_events (
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  ingested_at   DateTime64(3, 'UTC'),
  sensor        LowCardinality(String),
  src_ip        IPv6,
  src_port      UInt16,
  dst_ip        IPv6,
  dst_port      UInt16,
  protocol      LowCardinality(String),
  service       LowCardinality(String),
  conn_state    LowCardinality(String),
  duration_ms   UInt64,
  orig_bytes    UInt64,
  resp_bytes    UInt64,
  orig_pkts     UInt64,
  resp_pkts     UInt64,
  quality_flags UInt32,
  row_version   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (sensor, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 14 DAY;

-- model_releases: small release audit mirror, not artifact storage. No TTL.
-- Created now, first written on Roadmap Day 11.
CREATE TABLE IF NOT EXISTS model_releases (
  model_name    LowCardinality(String),
  model_version LowCardinality(String),
  model_sha     FixedString(64),
  schema_hash   FixedString(64),
  dataset_hash  FixedString(64),
  code_hash     String,
  threshold     Float32,
  metrics_json  String,
  status        LowCardinality(String),
  released_by   String,
  released_at   DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (model_name, model_version);
