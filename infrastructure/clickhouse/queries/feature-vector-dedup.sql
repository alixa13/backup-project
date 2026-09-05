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
-- This assumes no two rows for one event_id share an exact row_version —
-- ClickHouse does not document that independent argMax(colA, v)/argMax(colB, v)
-- calls resolve a tie to the same row, so an exact tie could in principle mix
-- columns across rows. Not a proven guarantee; not yet observed in practice.
--
-- log_type and connection_uid are the multi-protocol envelope columns the
-- feature_vectors table carries alongside the feature values. Both are
-- projected here via argMax, same as every other non-key column, so a training
-- snapshot never silently loses them: log_type is how training tells protocols
-- apart (the entire point of the multi-protocol redesign), and connection_uid
-- is the Zeek cross-protocol correlation key. Neither joins the GROUP BY.
--
-- That is correct only because the feature schema design (§5.4) obligates each
-- log type's mapper to derive an event_id that is unique on its own — this
-- query depends on that obligation, it does not enforce it. If it were ever
-- violated, this GROUP BY would silently merge the colliding rows and argMax
-- would discard one protocol's row with no error — the exact silent-collapse
-- failure §5.4 warns about, not a safeguard against it. The obligation holds
-- trivially today because conn is the only implemented log type; §5.4 gives
-- dns (sensor:uid:<trans_id>) and http (sensor:uid:<trans_depth>) structurally
-- identical id formats with no cross-log-type disjointness argument, so this
-- must be re-checked when either lands.
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
