# ADR 0001 — ClickHouse table set and naming

**Date:** 2026-08-31
**Status:** Accepted
**Context:** Repository_Structure.md Section E Step 8 / Roadmap.md Day 6

## Context

Two planning documents disagree about the ClickHouse schema.

`Roadmap.md`, under "Minimum ClickHouse scope", says to create **only four
tables**: `network_events`, `feature_vectors`, `predictions`, `model_metadata`.

`FINAL_ARCHITECTURE.md` Step 8 lists **five**, with full column types:
`network_events` (marked optional), `feature_vectors`, `predictions`,
`invalid_events`, `model_releases`.

The archive job archives the DLQ stream, which needs `invalid_events` — a table
only the second list contains. The four-table list therefore cannot stand as
written.

## Decision

Follow `FINAL_ARCHITECTURE.md`: five tables, and `model_releases` rather than
`model_metadata`.

Reasons:

1. It contains `invalid_events`, which the chosen archive scope requires.
2. It specifies exact column types, engines, partitioning and ordering. The
   Roadmap's list is a summary; this one is implementable.
3. `model_releases` is the more accurate name. `FINAL_ARCHITECTURE.md` describes
   the table as "an audit mirror, not artifact storage", which is a release log,
   not metadata.

## Deviations from FINAL_ARCHITECTURE.md

Two, both in `invalid_events`, both deliberate.

**`ORDER BY` changed** from `(stage, created_at, event_id)` to
`(stage, received_at, raw_payload_hash)`. The specified key does not work: it
sorts on `created_at`, which is a server-side `DEFAULT` and therefore absent from
the insert, and on `event_id`, which is empty for exactly the parse-stage failures
this table exists to hold. Producer-stamped `received_at` and the payload hash give
real locality, and make query-time deduplication a `GROUP BY raw_payload_hash`.

**"Optional redacted sample" column dropped.** The pipeline carries a payload hash
and never the payload, so the column would be empty on every row.

## Note: the multi-protocol schema redesign

`docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md` added
`log_type` and `connection_uid` to `feature_vectors` after this ADR's table set
was accepted. That redesign is a schema-content change within the `feature_vectors`
table this ADR already decided to create — it does not revisit the five-table
decision, the `model_releases` naming, or either `invalid_events` deviation above.
See `docs/clickhouse.md` for the current column inventory.

## Consequences

- `predictions`, `model_releases` and `network_events` exist from day one with no
  writer. Applying the DDL is what verifies them; Day 9 and Day 11 arrive to
  tables that already exist.
- `network_events` stays disabled in practice. It is the only IP-bearing table and
  needs a least-privilege user and retention approval first.
- `infrastructure/clickhouse/README.md` was rewritten; it previously repeated the
  four-table claim.
- A `model_metadata` table is never created. Anything referring to that name means
  `model_releases`.
