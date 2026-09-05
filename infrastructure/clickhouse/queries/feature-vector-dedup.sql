-- Canonical deduplication for the training snapshot.
--
-- feature_vectors is a ReplacingMergeTree, which compacts eventually and never
-- immediately. No query may assume physical deduplication has happened, so the
-- snapshot resolves duplicates explicitly.
--
-- row_version is the producer's producedAt. After a checkpoint restore the online
-- job replays the source event and re-stamps it, so the replayed emission is
-- strictly newer and wins — which is correct, because its indices 17-19 come from
-- the restored rolling-window state and are the values consistent with it.
--
-- log_type and connection_uid are the multi-protocol envelope columns the
-- feature_vectors table carries alongside the feature values. Both are
-- projected here via argMax, same as every other non-key column, so a training
-- snapshot never silently loses them: log_type is how training tells protocols
-- apart (the entire point of the multi-protocol redesign), and connection_uid
-- is the Zeek cross-protocol correlation key. Neither joins the GROUP BY --
-- event_id is already unique on its own by construction, and grouping on
-- log_type too would let two rows sharing an event_id but differing in
-- log_type both survive, which is exactly the event-ID collision the design
-- forbids.
--
-- Parameter: hash (FixedString(64)) — the frozen feature schema this snapshot is for.
--
-- `values` is backtick-quoted because VALUES is INSERT syntax.
SELECT
    event_id,
    schema_hash,
    argMax(`values`, row_version)       AS `values`,
    argMax(sensor, row_version)         AS sensor,
    argMax(log_type, row_version)       AS log_type,
    argMax(connection_uid, row_version) AS connection_uid,
    argMax(event_time, row_version)     AS event_time,
    argMax(quality_flags, row_version)  AS quality_flags,
    max(row_version)                    AS row_version
FROM feature_vectors
WHERE schema_hash = {hash:FixedString(64)}
GROUP BY event_id, schema_hash
