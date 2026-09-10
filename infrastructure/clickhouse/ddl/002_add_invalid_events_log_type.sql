-- Adds the log type to invalid_events.
--
-- The success path has always been protocol-aware: feature_vectors carries
-- log_type and the feature-vector-v1 contract carries logType. The failure path
-- was not. With one protocol that was invisible; with six it means "is the
-- S7comm parser rejecting everything?" cannot be answered, because every
-- rejection lands in one undifferentiated pile.
--
-- The value does NOT come from the message. dlq-v1 is frozen and carries no
-- protocol field, deliberately: the archive job knows the log type from the
-- topic it is reading, bound at wiring time. See the design's section 9.
--
-- LowCardinality(String) to match log_type on feature_vectors, and DEFAULT ''
-- so rows written before this migration remain readable rather than erroring.
ALTER TABLE invalid_events
    ADD COLUMN IF NOT EXISTS log_type LowCardinality(String) DEFAULT '' AFTER received_at;
