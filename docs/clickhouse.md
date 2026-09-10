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
protocol, service, connection state, stage, reason code.

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

## The common feature tier

Every per-protocol feature schema begins with the same 12 protocol-agnostic
features, frozen at `contracts/features/common-feature-tier-v1.json` and mirrored
in `CommonFeatureTierV1`. A protocol's own features start at index 12.

Indices 0-5 come from the online job's own keyed state and are always populated.
Indices 6-11 come from `conn.log` enrichment, which is a **non-blocking left
join**: a connection's first `conn.log` snapshot does not exist until it has been
alive five minutes, so those features are zero until one arrives. Index 11,
`conn_enrichment_present`, is what distinguishes a genuinely idle connection from
one whose snapshot has not yet been emitted — without it the two are identical to
a model.

`conn.log` counters are cumulative, so indices 6-9 carry the *delta* between
consecutive snapshots rather than the raw totals.

## The failure path is protocol-aware, without changing the DLQ contract

`invalid_events` carries `log_type`, so rejections can be attributed to a
protocol — "is the S7comm parser rejecting everything?" is answerable rather than
lost in one undifferentiated pile.

The value does **not** come from the message. `contracts/stream/dlq-v1.json` is
frozen at six fields and carries no protocol identifier, deliberately. Each
protocol has its own DLQ topic, and the archive job knows the log type from the
topic it is reading, bound at wiring time — the same compile-time binding the
input side uses. There is no runtime string parse of a topic name or payload.

The column arrived in `002_add_invalid_events_log_type.sql` rather than an edit
to `001_mvp_tables.sql`, which is immutable. It is `DEFAULT ''`, so rows written
before the migration stay readable.

The DDL directory is applied as an ordered migration sequence by both
`scripts/database/apply-ddl.sh` and the Java test support. Reading only the base
file would leave tests running against a schema missing whatever migrations add.

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
    max(row_version)                    AS latest_row_version
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

## Poison records (known limitation)

`FeatureVectorRowMapFunction.map` and `InvalidEventRowMapFunction.map` let
`IllegalArgumentException` escape on any message that fails to deserialize.
There is no DLQ or side output for the archive job's own input topics, unlike
the online job's `ParseMapValidateFunction`, which routes bad input to a DLQ
side output.

The practical effect: one undeserializable message on `featureVectorTopic` or
`dlqTopic` fails the map, which fails the checkpoint, so the offset in front of
it never advances. The job retries the same message on restart. Under the
failure-rate restart strategy (3 failures / 10 min) that eventually trips the
limit and the job terminates rather than looping forever — and it stays down,
since nothing removes the poison message from the front of the topic.

The exposure is narrower than it sounds: only the online job ever produces to
these internal topics, so a malformed message here means a serializer bug or a
bad deploy, not untrusted external input the way the raw `conn` topic is. That
lowers the likelihood; it does not remove it — a partial write, a schema
mismatch between a redeployed online job and this archive job, or a hand-edited
message could all still produce one.

The follow-up fix is a DLQ side output for both map functions, mirroring the
online job's `ParseMapValidateFunction` pattern, so a poison record is
quarantined instead of stalling the job. That is new feature work and is out of
scope for this branch.

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
