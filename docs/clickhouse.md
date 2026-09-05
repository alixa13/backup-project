# ClickHouse

ClickHouse is an archive and training-query system. It is never on the online
scoring path. The online job publishes to internal Kafka topics; a separate
archive job batches those into ClickHouse. If ClickHouse is unreachable, the
archive job stalls and Kafka lag grows — feature production continues.

## Tables

`infrastructure/clickhouse/ddl/001_mvp_tables.sql` defines five tables. Two are
written today.

| Table | Engine | Partition | Order | TTL | Written by |
|---|---|---|---|---|---|
| `feature_vectors` | `ReplacingMergeTree(row_version)` | `toYYYYMMDD(event_time)` | `(schema_hash, event_time, event_id)` | 90 days | archive job |
| `invalid_events` | `MergeTree` | `toYYYYMM(received_at)` | `(stage, received_at, raw_payload_hash)` | 30 days | archive job |
| `predictions` | `ReplacingMergeTree(row_version)` | `toYYYYMMDD(event_time)` | `(model_name, model_version, event_time, event_id)` | 180 days | Roadmap Day 9 |
| `network_events` | `ReplacingMergeTree(row_version)` | `toYYYYMMDD(event_time)` | `(sensor, event_time, event_id)` | 14 days | nothing — optional |
| `model_releases` | `MergeTree` | none | `(model_name, model_version)` | none | Roadmap Day 11 |

`DateTime64(3, 'UTC')` throughout. `IPv6` for IP columns, so IPv4 is consistently
mapped. `LowCardinality(String)` only for bounded values — sensor, log type,
protocol, service, state, stage, reason code.

The `values` column is backtick-quoted in every statement, because `VALUES` is
INSERT syntax and an unquoted identifier invites ambiguity.

### `feature_vectors` in full

The DDL is authoritative; this is an inventory, not a substitute for reading it.
`feature_vectors` carries the multi-protocol envelope alongside the feature
values:

| Column | Type | Notes |
|---|---|---|
| `event_id` | `String` | Per-log-type derivation; see "Deduplication" below. |
| `event_time` | `DateTime64(3, 'UTC')` | |
| `sensor` | `LowCardinality(String)` | |
| `log_type` | `LowCardinality(String)` | Which protocol mapper produced this row — `conn` today. |
| `connection_uid` | `String` | Zeek's connection UID. A correlation key, not an address — see "Access and retention". |
| `schema_id` | `LowCardinality(String)` | |
| `schema_hash` | `FixedString(64)` | Identifies the frozen feature schema this row was written under. |
| `values` | `Array(Float32)` | Deliberately variable-length — see "Feature count" below. |
| `quality_flags` | `UInt32` | |
| `archived_at` | `DateTime64(3, 'UTC')` | Server-set on insert. |
| `row_version` | `DateTime64(3, 'UTC')` | Producer's `producedAt`; the `ReplacingMergeTree` version column. |

`log_type` and `connection_uid` were added by the multi-protocol schema
redesign (`docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md`);
they postdate the archive job's original design spec and are not visible in
earlier prose describing this table.

### Feature count

The `values` column is `Array(Float32)`, not a fixed-width tuple. The table
does not fix the feature count — the schema's numeric features do: 20 for
`conn-feature-v1`, ordered per `contracts/features/conn-feature-schema-v1.json`.
A different log type's schema is free to carry a different feature count in
the same column; `schema_hash` identifies which schema a given row was written
under.

## Applying the schema

```sh
CLICKHOUSE_HOST=localhost CLICKHOUSE_DATABASE=netsec_ml ./scripts/database/apply-ddl.sh
```

Every statement is `CREATE ... IF NOT EXISTS`, so re-running is a no-op. The
script creates the database first, then applies `infrastructure/clickhouse/ddl/*.sql`
in lexical order. It reads the `CLICKHOUSE_*` variables documented in `.env.example`.

`DdlMigrationTest` applies this exact file against a ClickHouse container and also
runs the script itself, so there is no test-only copy of the schema to drift.
Docker is unavailable in this repository's current working environment, so this
test currently skips rather than runs; it has not been executed here and this
document makes no claim that it has passed.

## Deduplication

`ReplacingMergeTree` compacts eventually, never immediately. **No query may assume
physical deduplication has happened.** The canonical resolution is committed at
`infrastructure/clickhouse/queries/feature-vector-dedup.sql`, which is
authoritative — the excerpt below is illustrative only, and if it ever disagrees
with the file on disk, the file wins:

```sql
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
```

Every non-key column is projected via `argMax`, including `sensor`, `log_type`,
`connection_uid`, `event_time` and `quality_flags`, so a training snapshot never
silently drops the multi-protocol envelope columns.

`row_version` is the producer's `producedAt`. After a checkpoint restore the online
job replays the source event and re-stamps it, so the replayed emission is strictly
newer and wins. That is the correct outcome: its feature indices 17-19 come from
the restored rolling-window state and are the values consistent with it.

This resolution is correct only because event-id uniqueness is a per-log-type
obligation, not a property this query enforces. `conn`'s `event_id` is
`sensor:uid`, unique because Zeek emits one `conn` record per connection. Other
log types must derive an equally unique `event_id` from their own fields —
`docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md` §5.4
works through why `uid` alone does not generalize (a connection can produce many
`dns` or `http` records) and requires each log type's mapper to state and test its
own uniqueness argument. If that obligation were ever violated, this `GROUP BY`
would silently merge colliding rows and `argMax` would discard one protocol's row
with no error. The committed query's header comment carries the fuller caveat,
including an unproven assumption about tie-breaking when two rows share an exact
`row_version`; read it before relying on this query for anything precise.

`invalid_events` is duplicate-tolerant by design rather than deduplicated. A replay
re-rejects and re-stamps the record, so duplicate rows accumulate. It is low-volume
30-day forensic data; deduplicate with `GROUP BY raw_payload_hash` at query time if
a count needs to be exact.

## Batching and back pressure

The archive sink flushes on whichever comes first:

| Trigger | Value |
|---|---|
| Rows | 5,000 |
| Serialized payload | 4 MiB |
| Processing-time timer | 1 s |
| Checkpoint barrier | always |

These are initial values to benchmark, not settled ones. They are constructor
parameters on `ClickHouseBatchSink`, with the defaults above.

A failed insert is retried three times with 200/400/800 ms backoff, then throws.
Throwing fails the checkpoint; the Kafka offsets in that checkpoint do not advance;
the job restarts and replays. This is how the job satisfies "commit consumer
progress only after successful insert acknowledgement" without a manual commit.

Retrying without bound would convert a ClickHouse outage into silent backpressure.
Throwing converts it into visible Kafka lag. **Lag, not RAM** — the buffer cannot
grow past one batch, because a trigger either flushes it or throws.

## When ClickHouse is down

1. The archive job's inserts fail, exhaust their retries, and throw.
2. Its checkpoints fail, so its Kafka offsets stop advancing.
3. The restart strategy backs off; consumer lag on the internal topics grows.
4. **The online job is unaffected.** It shares no process, no checkpoint and no
   state with the archive job, and keeps publishing to Kafka throughout.
5. When ClickHouse returns, the archive job replays from its last successful
   checkpoint. Replayed rows are duplicates; `ReplacingMergeTree` absorbs them in
   `feature_vectors`, and `invalid_events` tolerates them.

`ClickHouseOutageTest` (`modules/bootstrap-archive-job`) asserts points 4 and 5.
It is a Testcontainers test; Docker is unavailable in this repository's current
working environment, so it currently skips. It has been written but not executed
here — this document does not claim it has passed.

## Metrics

Per table, on the sink writer's metric group:

| Metric | Type |
|---|---|
| `archive.table.<name>.batch.rows` | gauge, last batch |
| `archive.table.<name>.batch.bytes` | gauge, last batch |
| `archive.table.<name>.flush.latency.ms` | gauge, last flush |
| `archive.table.<name>.insert.failures` | counter, per failed attempt |

Failures are counted per attempt rather than per batch, so the counter
distinguishes a flaky server from a dead one. Real observability — dashboards,
alerting, histograms — is Roadmap Day 12.

## Access and retention

`network_events` is the only table with columns holding raw IP addresses.
Before it is enabled it needs a least-privilege ClickHouse user and a documented
retention approval; neither is in place, and nothing writes to it today.

`feature_vectors` carries no address columns itself — it holds the schema's
numeric features (20 for `conn-feature-v1`), `log_type`, the sensor name, and
the composite event id. But it also carries `connection_uid`, which is Zeek's
connection UID: a correlation key, not an address, but one that joins straight
back to Zeek logs that DO carry addresses (`conn.log`, and any future
`network_events` rows). Anyone deciding who may query `feature_vectors` needs
to weigh that join path, not just the columns physically present in the table.
`invalid_events` carries a payload hash, never the payload.

TTL values are initial. Calculate storage from observed compressed bytes per event
times events per second times retention before treating any of them as settled.
