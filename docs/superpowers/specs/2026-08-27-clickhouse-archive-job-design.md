# ClickHouse Archive Job — Design

**Date:** 2026-08-27
**Covers:** `Repository_Structure.md` Section E, Step 8 — "ClickHouse DDL and independent archive job"; `Roadmap.md` Day 6, Software Engineer half.
**Predecessor:** `docs/conn-foundation-pipeline.md` (Steps 2-7, merged at `88285d6`).

---

## 1. Context

The online job is merged and runs: external `conn` topic → parse → validate → bounded
keyed state → 20-value `FeatureVector` → two internal Kafka topics.

```
netsec.conn.feature-vector.v1    FeatureVector JSON
netsec.conn.dlq.v1               rejected records (parse + map failures)
```

Nothing consumes either topic. This step adds the consumer: an independent Flink job
that batches both streams into ClickHouse, plus the schema it writes into.

The load-bearing property is negative. ClickHouse must never be able to stop feature
production. Everything below — separate job, separate deployment, bounded retry that
fails fast into Kafka lag — exists to preserve that.

State of the modules this step fills in:

| Module | Today |
|---|---|
| `modules/adapter-clickhouse` | Step-1 skeleton, `package-info.java` only |
| `modules/bootstrap-archive-job` | Step-1 skeleton, `package-info.java` only |
| `contracts/stream/` | empty — no frozen record shapes |
| `infrastructure/clickhouse/ddl/` | `.gitkeep` only |

---

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Archive **feature-vector and DLQ** streams only | Both already flow. Predictions do not exist until Day 9; `network_events` is optional in both source documents. |
| D2 | **Five tables** from `FINAL_ARCHITECTURE.md`, not the Roadmap's four | The five-table list carries exact column types and includes `invalid_events`, which D1 requires. |
| D3 | The archive job is a **Flink job** | `FINAL_ARCHITECTURE.md` Step 9 and `CLAUDE.md` both specify it. Reuses the existing JAR packaging, checkpoint volumes and metrics surface. |
| D4 | **Extend `FeatureVector` with `sensor` and `producedAt`** before freezing v1 | `contracts/` is immutable; adding either later costs a `-v2`. Neither touches the frozen 20 values or their order. |
| D5 | ClickHouse access via **`com.clickhouse:client-v2`** | Maintained path; native `Array(Float32)` / `DateTime64` handling, pooling, compression. |
| D6 | **Two source→sink chains**, no `ArchiveRouter` class | The Flink job graph already routes. A router class on top of a routing topology is an abstraction with no behaviour. |

### D3 in detail — how a Flink job satisfies "commit after insert acknowledgement"

`Roadmap.md` Day 6 requires: *"Commit consumer progress only after successful insert
acknowledgement; expect duplicates after failures."* Under Flink this is not a manual
`commitSync` but a consequence of checkpoint ordering:

1. `SinkWriter.flush(endOfInput)` runs when the checkpoint barrier reaches the writer.
2. If the ClickHouse insert fails, `flush` throws.
3. A throwing `flush` fails the checkpoint.
4. Kafka offsets are part of that checkpoint, so they do not advance.
5. The job restarts from the last successful checkpoint and replays.

Replayed rows arrive as duplicates. `ReplacingMergeTree` absorbs them in
`feature_vectors`; `invalid_events` tolerates them by design (§6.3).

---

## 3. Job topology

One job, two independent chains, one `StreamExecutionEnvironment`, one checkpoint
spanning both.

```
KafkaSource(netsec.conn.feature-vector.v1)
  → FeatureVectorDeserializer  → FeatureVectorRowMapper  → ClickHouseBatchSink → feature_vectors

KafkaSource(netsec.conn.dlq.v1)
  → RejectedEventDeserializer  → InvalidEventRowMapper   → ClickHouseBatchSink → invalid_events
```

Entry point mirrors `OnlineFeatureJob`:

```java
public static void build(StreamExecutionEnvironment env,
                         String bootstrapServers,
                         String featureVectorTopic,
                         String dlqTopic,
                         ClickHouseConfig clickHouse)
```

`main()` reads configuration from the environment: `KAFKA_BOOTSTRAP_SERVERS`,
`FEATURE_VECTOR_TOPIC`, `DLQ_TOPIC`, `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`,
`CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.

Only the `CLICKHOUSE_*` and `KAFKA_BOOTSTRAP_SERVERS` names are in `.env.example` today.
The topic names are not — `OnlineFeatureJob` already reads `CONN_INPUT_TOPIC`,
`FEATURE_VECTOR_TOPIC`, `DLQ_TOPIC` and `SENSOR_ID` from the environment with inline
defaults, and none of the four were ever added. `.env.example` gains all four plus
nothing new, since this job reuses the same topic variables.

Consumer group `conn-archive-job`. Both sources start at
`OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)` so a redeploy
resumes rather than re-archiving from the beginning.

The two chains share checkpoint fate: an insert failure on either stalls both. This is
intended. Both write to the same ClickHouse, so if it is unreachable both should stall
and let Kafka lag grow.

### 3.1 `ClickHouseBatchSink`

A Flink Sink V2 implementation. The writer holds no state across flushes, so there is
no committer and no `snapshotState`.

| Trigger | Threshold |
|---|---|
| Row count | 5,000 |
| Serialized payload | 4 MiB |
| Processing-time timer | 1,000 ms |
| Checkpoint barrier | `flush(endOfInput)` drains whatever remains |

All four are initial values to benchmark, per `FINAL_ARCHITECTURE.md` Step 8, and are
constructor parameters rather than constants.

Insert path: buffer rows as `JSONEachRow` lines, POST the batch through `client-v2`,
await acknowledgement. On failure, retry **3 times with exponential backoff — 200 ms,
400 ms, 800 ms** — then throw. Like the flush thresholds, these are constructor
parameters with those defaults, not constants.

Retrying without bound inside `write()` would convert a ClickHouse outage into silent
backpressure on the archive job. Throwing converts it into a job restart, the restart
strategy backs off, and Kafka lag grows visibly. Lag, not RAM — which is the stated
requirement. The buffer is bounded by the flush thresholds above, so a failing flush
cannot grow it.

Metrics, registered on the writer's Flink metric group:

```
archive.batch.rows            histogram
archive.batch.bytes           histogram
archive.flush.latency.ms      histogram
archive.insert.failures       counter
```

`adapter-monitoring` stays empty until Day 12. Four counters do not justify an
abstraction layer.

---

## 4. Frozen wire contracts

Both files are new, immutable once committed, and never edited in place — a change
creates `-v2`.

### 4.1 `contracts/stream/feature-vector-v1.json`

Eight fields. `sensor` and `producedAt` are the additions (D4).

```json
{
  "eventId":    "3f2b…",
  "eventTime":  "2026-08-27T10:03:11.250Z",
  "sensor":     "sensor-01",
  "schemaId":   "conn-feature-v1",
  "schemaHash": "f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b",
  "values":     [ 20 float32 values, ordered per conn-feature-schema-v1 ],
  "qualityFlags": 0,
  "producedAt": "2026-08-27T10:03:11.402Z"
}
```

`sensor` exists because `feature_vectors` must be self-sufficient for training. Sensor
identity is technically recoverable today — `EventId.derive` composes
`"<sensor>:<upstreamId>"`, so it is the prefix of `eventId` — but that requires training
SQL to split an opaque identifier on its internal delimiter, which breaks silently the
moment the ID format changes. The only other source is `network_events`, which both
source documents mark optional and which may never be enabled. A first-class column is
the only durable answer.

`producedAt` becomes the `row_version` of the ClickHouse row, which is what makes
deduplication meaningful. After a checkpoint restore the online job re-processes the
source event and re-stamps `producedAt`, so the replayed emission is strictly newer and
wins. That is the correct outcome: its indices 17-19 derive from the restored window
state and are the values consistent with it.

Adding these two fields does **not** change `ConnFeatureSchemaV1.CONTENT_HASH`. That
hash covers the 20 features and their order, not the record envelope. The existing test
asserting the hash must continue to pass unmodified.

### 4.2 `contracts/stream/dlq-v1.json`

Five fields.

```json
{
  "stage":          "PARSE",
  "reasonCode":     "MALFORMED_JSON",
  "detail":         "Unexpected character at position 42",
  "rawPayloadHash": "sha256 hex, 64 chars",
  "receivedAt":     "2026-08-27T10:03:11.250Z"
}
```

`netsec.conn.dlq.v1` carries both parse-stage and map-stage rejections.
`FINAL_ARCHITECTURE.md` distinguishes them — malformed JSON is a DLQ concern, an invalid
port is a domain concern — and `invalid_events` has a `stage` column for exactly that
split. One topic with a `stage` discriminator is sufficient until a consumer needs them
separated; a distinct `invalid-event-v1` topic is deferred.

### 4.3 Contract drift tests

For each contract: parse the frozen JSON, assert the serializer emits exactly that field
set, and assert the new deserializer round-trips it. Contract-versus-code drift is the
failure mode that actually occurs, and it is cheap to catch.

---

## 5. Changes to merged code

Nine files. All are small and all are consequences of D4 and §7's boundary rule.

### 5.1 `domain`

```java
// FeatureVector — two new components. Defensive float[] copy in the compact
// constructor and in the accessor is unchanged and still required.
public record FeatureVector(String eventId, Instant eventTime, SensorId sensor,
                            String schemaId, String schemaHash,
                            float[] values, int qualityFlags, Instant producedAt)
```

```java
// ReasonCode — each code declares which pipeline stage produced it.
public enum ReasonCode {
    MALFORMED_JSON(Stage.PARSE),
    MISSING_REQUIRED_FIELD(Stage.PARSE),
    INVALID_TIMESTAMP(Stage.MAP),
    INVALID_PORT(Stage.MAP),
    INVALID_COUNTER(Stage.MAP);

    public enum Stage { PARSE, MAP }

    private final Stage stage;
    ReasonCode(Stage stage) { this.stage = stage; }
    public Stage stage() { return stage; }
}
```

```java
// RejectedEvent — new. The neutral type adapter-kafka and adapter-clickhouse
// meet at, so neither has to import the other (see §7).
public record RejectedEvent(String rawPayloadHash, ReasonCode reason,
                            String detail, Instant receivedAt)
```

`domain` gains no dependency. `SensorId`, `ReasonCode` and `Instant` are already there.

### 5.2 `application`

`BuildFeaturesUseCaseImpl` takes an injected `Clock` so `producedAt` is deterministic
under test. The no-argument constructor is retained so
`ConnFeatureProcessFunction.open()`'s existing `new BuildFeaturesUseCaseImpl()` compiles
unchanged.

```java
public BuildFeaturesUseCaseImpl()            { this(Clock.systemUTC()); }
public BuildFeaturesUseCaseImpl(Clock clock) { this.clock = requireNonNull(clock); }
```

`producedAt` is `clock.instant().truncatedTo(ChronoUnit.MILLIS)`. `DateTime64(3)` stores
milliseconds; an untruncated `Instant` would fail its own round-trip assertion.

The vector gains `event.sensor()` and the truncated `producedAt`; the 20 values and
their order are untouched.

### 5.3 `adapter-kafka`

- `FeatureVectorSerializer` — emits `sensor` and `producedAt`.
- `FeatureVectorDeserializer` — **new**, exact inverse.
- `RejectedRecordPayload` — gains `stage` and `receivedAt`.
- `RejectedRecordSerializer` — emits `stage`; **stops calling `Instant.now()`** (§5.5).
- `RejectedEventDeserializer` — **new**, JSON → `RejectedEvent`.

The DLQ path is deliberately asymmetric, and the asymmetry is not an oversight. The
producer side holds the raw bytes and so `RejectedRecordPayload` keeps carrying them —
the serializer computes the SHA-256, exactly as it does today. The consumer side never
sees bytes; the hash is already a field in the JSON. So the deserializer returns
`RejectedEvent`, which holds the hash and no payload. `RejectedRecordPayload` is
therefore a producer-only type and `RejectedEvent` a consumer-only one; they are not two
spellings of the same thing.

### 5.4 `adapter-flink` / `bootstrap-online-job`

`RejectedRecord` gains `receivedAt`, stamped in `ParseMapValidateFunction` at the moment
of rejection. `OnlineFeatureJob`'s DLQ lambda passes `r.reason().stage().name()` and
`r.receivedAt()` through to the payload.

### 5.5 The `Instant.now()` fix, and its deliberate limit

`RejectedRecordSerializer` currently calls `Instant.now()` inside `serialize()`. This is
wrong twice: `receivedAt` records when the sink ran rather than when the record was
rejected, and it is re-stamped on every serialization attempt. Moving the stamp to
`ParseMapValidateFunction` fixes the semantics and removes data-minting from a
serializer.

It does **not** make replay idempotent. After a restart the record is re-rejected and
re-stamped, so `invalid_events` accumulates duplicate rows. That is accepted:
`Roadmap.md` says "expect duplicates after failures", the table is low-volume 30-day
forensic data, and `raw_payload_hash` supports `GROUP BY` deduplication at query time.
`invalid_events` therefore stays a plain `MergeTree`.

---

## 6. ClickHouse schema

One file, `infrastructure/clickhouse/ddl/001_mvp_tables.sql`. Every statement is
`CREATE TABLE IF NOT EXISTS`, so re-running is a no-op. Table names are unqualified; the
database comes from `CLICKHOUSE_DATABASE`, matching `.env.example`.

`DateTime64(3, 'UTC')` throughout. `values` is backtick-quoted in DDL and in every query
— `VALUES` is ClickHouse INSERT syntax and an unquoted identifier invites ambiguity.

### 6.1 `feature_vectors` — written this step

```sql
CREATE TABLE IF NOT EXISTS feature_vectors (
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  sensor        LowCardinality(String),
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
```

`row_version` is the wire record's `producedAt`. `archived_at` is server-set by DEFAULT;
the writer never sends it.

### 6.2 `invalid_events` — written this step

```sql
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
```

`event_id` is `''` and `event_time` is null for parse-stage rejections, where no
identity was recoverable. `source_version` is the constant `zeek-conn-source-v1`.

### 6.3 Deviations from `FINAL_ARCHITECTURE.md` in `invalid_events`

Its specified `ORDER BY (stage, created_at, event_id)` does not work as written.
`created_at` is a server default and so is absent from the insert, and `event_id` is
empty for precisely the parse failures the table exists to hold — a dead sort key.
Producer-stamped `received_at` and `raw_payload_hash` are used instead, which also makes
query-time deduplication a `GROUP BY raw_payload_hash`.

The specified "optional redacted sample" column is dropped. Only a hash is carried, so
the column would be empty on every row.

### 6.4 Created, not written

| Table | Engine / order | TTL | First writer |
|---|---|---|---|
| `network_events` | `ReplacingMergeTree(row_version)`, `PARTITION BY toYYYYMMDD(event_time)`, `ORDER BY (sensor, event_time, event_id)` | 14 days | not scheduled — optional audit |
| `predictions` | `ReplacingMergeTree(row_version)`, `PARTITION BY toYYYYMMDD(event_time)`, `ORDER BY (model_name, model_version, event_time, event_id)` | 180 days | Day 9 |
| `model_releases` | `MergeTree`, `ORDER BY (model_name, model_version)` | none | Day 11 |

Columns follow `FINAL_ARCHITECTURE.md` Step 8 exactly. Applying the file is what
verifies them; Day 9 and Day 11 then arrive to tables that already exist.

`network_events` is the only IP-bearing table. `IPv6` columns accept IPv4 literals and
map them to IPv4-mapped form; that behaviour needs verifying whenever the table gains a
writer, not now. Its least-privilege access and retention approval are a prerequisite of
enabling it, documented in `docs/clickhouse.md`.

### 6.5 The deduplication contract

Committed and tested now, because training depends on it from Day 7.

```sql
SELECT event_id, argMax(`values`, row_version) AS `values`
FROM feature_vectors
WHERE schema_hash = {hash:FixedString(64)}
GROUP BY event_id, schema_hash
```

`ReplacingMergeTree` compacts eventually, never immediately. No query may assume
physical deduplication has happened.

### 6.6 Migration

`scripts/database/apply-ddl.sh` applies `infrastructure/clickhouse/ddl/*.sql` in lexical
order over HTTP, creating the database first. It reads the `CLICKHOUSE_*` variables from
`.env.example`. Idempotent, because the DDL is.

The integration tests mount **that same file** into the container's
`/docker-entrypoint-initdb.d/` rather than keeping a test-only copy, so schema drift
between test and production is structurally impossible. If the init-script path proves
unreliable for the pinned image, the fallback is to execute the same file against the
container after startup — still one artifact.

---

## 7. Module boundaries

`adapter-clickhouse` imports **no Kafka types and no Flink connector types**. Its entire
dependency surface is `domain`, `client-v2`, and Flink's Sink V2 interface.

```
adapter-kafka       JSON ⇄ typed values.  Owns both serializers and both new
                    deserializers. Already carries Jackson.
adapter-clickhouse  typed values → rows → insert. Knows nothing about Kafka.
bootstrap-archive-job  wires the two. Already depends on both.
```

This creates no new module edge. It is a deliberate contrast with the online job, where
`adapter-flink` imports `adapter-kafka` — a cross-adapter dependency that the
`conn-foundation-pipeline` review flagged and that was allowed only because the plan
mandated it. That shape is not repeated here.

`RejectedEvent` in `domain` (§5.1) is what makes it work: `adapter-kafka` deserializes
into it and `adapter-clickhouse` maps out of it, so neither imports the other.
`adapter-flink`'s `RejectedRecord` is unchanged in kind — it legitimately holds raw
bytes in order to compute the hash — and maps to `RejectedEvent` at the sink boundary.

New dependencies, pinned in root `dependencyManagement` beside the Flink and Kafka
versions:

- `com.clickhouse:client-v2` — `adapter-clickhouse`, compile
- `org.testcontainers:clickhouse` — `bootstrap-archive-job`, test

**Open at implementation time:** the exact versions. `org.testcontainers:clickhouse`
must match the `1.21.4` already pinned for `kafka` and `junit-jupiter`, so that one is
determined. `com.clickhouse:client-v2` takes the latest release resolvable at
implementation time, subject to two constraints — it must run on Java 21, and its
transitive Apache HttpClient 5 and LZ4 must shade cleanly, since the production Flink
cluster is air-gapped and receives a fat JAR. Verify the insert API against whichever
version is pinned. The design depends only on "POST a `JSONEachRow` batch and await
acknowledgement", which the client has offered across versions.

---

## 8. Tests

### 8.1 Unit — no Docker

| Test | Module | Asserts |
|---|---|---|
| `BatchBufferTest` | adapter-clickhouse | Flushes at 5,000 rows; at 4 MiB; on the 1 s timer; on `flush(endOfInput)`; buffer empty afterwards |
| `ClickHouseSinkWriterTest` | adapter-clickhouse | Bounded retry then throw; `archive.insert.failures` increments; a batch is never silently dropped. Uses a fake inserter |
| `FeatureVectorRowMapperTest` | adapter-clickhouse | Field mapping; `row_version` = `producedAt`; millisecond round-trip is exact |
| `InvalidEventRowMapperTest` | adapter-clickhouse | Stage and reason mapping; empty `event_id` and null `event_time` for parse-stage rejections |
| `ReasonCodeStageTest` | domain | Every `ReasonCode` maps to a stage — fails when a new code is added without one |
| `BuildFeaturesUseCaseImplTest` | application | Fixed `Clock` gives deterministic `producedAt`; sensor propagates from the event; truncation to millis |
| Contract drift ×2 | adapter-kafka | Serializer field set equals the frozen contract's; deserializer round-trips |

### 8.2 Integration — Testcontainers

All three carry `@Testcontainers(disabledWithoutDocker = true)`, matching
`OnlineFeatureJobE2ETest`.

**`ArchiveJobE2ETest`** — Roadmap test 1. Kafka and ClickHouse containers. Produce one
feature-vector record; assert one `feature_vectors` row with all 20 values intact, plus
correct sensor, schema hash and `row_version`. Produce one DLQ record; assert one
`invalid_events` row with the correct stage and reason code.

**`ArchiveDeduplicationTest`** — Roadmap test 2. Insert the same `(event_id,
schema_hash)` twice with differing `values` and differing `row_version`. Assert the §6.5
query returns exactly one row and that it carries the higher `row_version`'s values. The
test must not call `OPTIMIZE TABLE … FINAL`; the point is that the query is correct
without physical compaction.

**`ClickHouseOutageTest`** — Roadmap test 3, and the Definition of Done's teeth. Start
both jobs, stop the ClickHouse container mid-stream, keep producing to the input topic.
Assert that feature-vector records keep arriving on `netsec.conn.feature-vector.v1` after
ClickHouse dies, and that the online job stays `RUNNING`.

Both jobs run the way `OnlineFeatureJobE2ETest` already runs one — `env.executeAsync` on
a daemon thread, with a polling consumer and a deadline rather than a fixed sleep.
Follow that shape rather than inventing a second one.

This is the most flake-prone test in the set — two Flink jobs in one JVM, a container
stopped mid-flight. Its assertions are deliberately coarse. It must **not** assert on lag
values, restart counts, or timing, all of which are nondeterministic and would buy
flakiness in exchange for no information.

### 8.3 Extended

`OnlineFeatureJobE2ETest` keeps passing unchanged — its assertions are `contains` checks
on `"schemaId"` and on the composite event ID, and adding fields breaks neither. It is
therefore **extended, not repaired**: add assertions that `sensor` and `producedAt` are
present and correct on the published record. Two new fields that no test looks at is how
a wire contract quietly rots.

---

## 9. Documentation

- `docs/clickhouse.md` — named Day 6 deliverable. Table purposes and retention; the
  deduplication contract from §6.5; batch tuning knobs and how to change them; how to
  apply DDL; what happens when ClickHouse is unreachable; the least-privilege and
  retention-approval prerequisite for enabling `network_events`.
- `docs/adr/0001-clickhouse-table-set.md` — records D2, the `model_metadata` versus
  `model_releases` naming resolution, and the §6.3 deviations.
- `infrastructure/clickhouse/README.md` — currently claims four tables including
  `model_metadata`. Rewrite for the five-table set.
- `contracts/stream/README.md` — currently says "not yet committed". Update for the two
  frozen contracts and note that `network-event-v1`, `prediction-v1` and
  `invalid-event-v1` remain unfrozen.
- `.env.example` — add the four topic variables the online job already reads but that
  were never documented (§3), so both jobs are configurable from one file.
- `CLAUDE.md` — implementation-state section.

---

## 10. Deviations from source documents

Each is deliberate, and each is recorded in the ADR.

| Source | Says | This design | Why |
|---|---|---|---|
| `Roadmap.md` Day 6 deliverables | `ArchiveRouter.java` | No such file | The Flink topology routes. A router class over a routing topology has no behaviour. |
| `Roadmap.md` minimum scope | Four tables, `model_metadata` | Five tables, `model_releases` | D2. The four-table list omits `invalid_events`, which archiving the DLQ requires. |
| `Roadmap.md` Day 6 | "Commit consumer progress only after successful insert acknowledgement" | Checkpoint-ordered flush | D3. Same guarantee, expressed through Flink's checkpoint barrier rather than a manual commit. |
| `Roadmap.md` Day 6 | Internal topics `netsec.network-event.v1`, `netsec.prediction.v1`, `netsec.alert.v1`, `netsec.invalid-event.v1` | Not created | D1. No producer and no consumer exists for any of them yet. |
| `FINAL_ARCHITECTURE.md` Step 8 | `invalid_events ORDER BY (stage, created_at, event_id)` | `(stage, received_at, raw_payload_hash)` | §6.3. The specified key uses a server-default column and an empty column. |
| `FINAL_ARCHITECTURE.md` Step 8 | `invalid_events` "optional redacted sample" | Dropped | §6.3. Only a hash is carried; the column would always be empty. |

---

## 11. Out of scope

- **The Python half of Day 6** — `snapshot.py`, `clickhouse_reader.py`,
  `dataset-manifest-v1.json`, and the first-archived-data profiling. That is the AI
  engineer's track and wants real rows to profile, so it gets its own cycle after this
  one lands.
- **Predictions** and the `netsec.prediction.v1` stream — Day 9.
- **`network_events`** — created, never written. Enabling it needs a normalized-event
  sink on the online job plus the access and retention approvals in §6.4.
- **`adapter-monitoring`** — stays empty. Day 12 owns observability.
- **Watermarks, RocksDB state backend, custom `SourceWindowState` serializer** — known
  deferrals from `conn-foundation-pipeline`, unrelated to this step, still open.

---

## 12. Definition of done

1. `./mvnw clean verify` passes from a clean tree.
2. A feature vector produced to `netsec.conn.feature-vector.v1` is queryable in
   `feature_vectors` with all 20 values intact.
3. A rejected record produced to `netsec.conn.dlq.v1` is queryable in `invalid_events`
   with the correct stage.
4. The §6.5 deduplication query returns exactly one deterministic row per
   `(event_id, schema_hash)` in the presence of duplicates, without `OPTIMIZE … FINAL`.
5. With ClickHouse stopped, the online job keeps publishing to Kafka and stays `RUNNING`.
6. `001_mvp_tables.sql` applies cleanly to an empty ClickHouse and is idempotent on a
   second run.
7. Both stream contracts are committed, and their drift tests pass.
8. `docs/clickhouse.md` and the ADR are committed; the two stale READMEs are corrected.
