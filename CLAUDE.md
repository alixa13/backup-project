# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Java (Maven reactor)
```sh
./mvnw clean verify                          # build + test all modules
./mvnw test -pl modules/domain               # test a single module
./mvnw test -pl modules/domain -Dtest=ProtocolTest   # run one test class
./mvnw package -pl modules/bootstrap-online-job -am  # build deployable JAR
```

Three things about running the tests that will otherwise cost you an hour:

- **`clean verify` does not complete on a small machine.** The container tests are
  OOM-killed on 5.7 GiB. Run them in stages instead: `./mvnw install -DskipTests`
  once, then one module at a time. Testcontainers' own Ryuk reaper removes each
  stage's containers on its own; if a manual sweep is ever needed between stages,
  scope it to this project's own containers
  (`docker ps -aq --filter label=org.testcontainers=true | xargs -r docker rm -f`)
  — never `docker ps -aq | xargs -r docker rm -f`, which removes every container
  on the machine, including ones this project never started. Everything passes
  staged; nothing proves the whole reactor green in one command until CI has more
  memory.
- **`-pl <module>` without `-am` resolves siblings from `~/.m2`**, producing
  phantom "cannot find symbol" errors against code that is fine.
- **`-Dtest=X` with `-Dsurefire.failIfNoSpecifiedTests=false` reports BUILD SUCCESS
  having run zero tests.** Always confirm the actual `Tests run:` count before
  believing a green build.

### Python (training/)
```sh
cd training
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                           # all tests
pytest tests/unit/               # unit tests only
pytest -k test_name              # single test
```

## Architecture

**Data flow:** External Kafka (`conn` and `dns` topics) → Online Flink job (parse → validate → bounded keyed state → per-schema feature vector — conn's own 20 values, dns's 24 (the common tier, whose conn-derived fields come from a non-blocking left join against conn.log snapshots, plus dns's own protocol tier)) → internal Kafka topics (separate feature-vector and DLQ topics per protocol) → Archive Flink job → ClickHouse. ONNX inference is not yet implemented (see Implementation state).

**Hexagonal, one-way dependency chain:**
```
domain → ports → application → adapters → bootstrap
```
- `domain` — pure Java value objects and formulas. Zero imports of Kafka, Flink, ClickHouse, ONNX Runtime, Jackson, or Docker. Enforced by the module's own compile-time classpath.
- `ports` — input/output port interfaces. Depends only on `domain`.
- `application` — use cases and feature orchestration. Depends only on `domain` + `ports`. No Jackson, no Flink.
- `adapter-kafka/flink/onnx/clickhouse/registry-filesystem/monitoring` — each implements ports and carries framework imports. Adapters do not import each other, with one recorded
  exception: `adapter-flink` depends on `adapter-kafka`, because the operators that
  drive parsing (`ConnParseMapValidateFunction`, `DnsParseMapValidateFunction`)
  construct the kafka-side parser and mapper themselves. That dependency is
  one-directional, and no other adapter pair is coupled — every other adapter's pom
  names only itself.
- `bootstrap-online-job` / `bootstrap-archive-job` — the **only** modules that wire concrete adapters together into a runnable Flink job.
- `training/` — independent Python project. Reads archived `FeatureVector` rows and contracts. Never reimplements Zeek parsing, normalization, windowing, categorical mapping, defaults, or feature ordering.

**Key invariants:**
- Java package root: `io.netsecml.platform`
- Java 21 throughout — use `record` for immutable data carriers, `sealed interface` + records + pattern-matching `switch` for closed hierarchies. Records with array components need defensive copies in compact constructor *and* in the accessor.
- Feature vector: exactly `schema.featureCount()` `float32` values, frozen per schema. Conn's registered schema reports 20, ordered per `contracts/features/conn-feature-schema-v1.json`; dns's registered schema reports 24, ordered per `contracts/features/dns-feature-schema-v1.json`. Feature order is frozen once defined — a change creates a new schema version.
- A protocol's feature schema is the common tier (12 values, frozen) followed by that
  protocol's own tier. `conn-feature-v1` predates the tier and is frozen without it; every
  schema from `dns-feature-v1` onward leads with it.
- Event identity is a per-log-type obligation with its own stated argument: `CONN` is
  `sensor:uid`, `DNS` is `sensor:uid:trans_id`. A log type whose uniqueness cannot be
  evidenced from its own fields is not ready to be added.
- CPU-only: no GPU, no CUDA, no deep-learning frameworks. ONNX Runtime Java with intra/inter-op threads pinned to 1 per subtask.
- Model bundle is pinned in job config and loaded once in `open()`. No live hot reload.
- ClickHouse is never on the online scoring path. Predictions go to Kafka first; the archive job writes to ClickHouse asynchronously.
- ClickHouse inserts are idempotent (`ReplacingMergeTree`). Do not promise exactly-once for the archive sink.
- Bounded per-`(sensor, sourceIp)` state only — no unbounded per-IP maps or event history.
  This is the rule the code aims at and holds per key (five one-minute buckets each), but not
  yet in aggregate: neither `ConnFeatureProcessFunction`'s `rolling-counters` state nor
  `DnsFeatureProcessFunction`'s `dns-window-state` carries a TTL, so the KEY SET keeps every
  `(sensor, sourceIp)` ever seen, for either protocol, forever (see `OnlineFeatureJob`'s KNOWN
  GAP comment).
- `NetworkEvent` is a **sealed interface** over a shared `EventEnvelope`, with one record per log
  type. `permits` lists only log types that have a parser, mapper and feature schema — adding a
  record ahead of its implementation defeats the exhaustiveness checking that sealing buys.
- Feature schemas resolve through `FeatureSchemaRegistry`, by `LogType` at wiring time or by
  `schemaId` for an archived row. Both throw on an unknown key: an unresolvable schema is a
  deployment error, not a runtime condition. A vector's width, id and content hash all come from
  its registered schema, never from a literal.
- `BuildFeaturesUseCase<E, S>` has one implementation per log type, so no implementation casts or
  switches to discover what it was given.

**Contracts (`contracts/`)** are immutable, content-hashed, and language-neutral. A schema change creates a new version (`-v2`), never edits an existing file.

**Tech stack:** Java 21 / Flink 2.2.1 / `flink-connector-kafka:5.0.0-2.2` (Sink V2 / FLIP-27 Source API only — `SourceFunction`/`SinkFunction` V1 were removed in Flink 2.x) / Jackson 2.17 / ONNX Runtime Java / JUnit 5 / Python 3.10+ / scikit-learn / skl2onnx / onnxruntime / Testcontainers 1.21.4 (Kafka) for E2E.

## Implementation state

The conn foundation pipeline (Steps 2-7) is implemented and merged to `main`.
The ClickHouse archive job (Step 8) is complete on `feat/clickhouse-archive-job`
but not yet merged to `main`. Implementation order is tracked in
`Repository_Structure.md` Section E (18 steps).

The DNS unit adds the platform's second protocol: 38 commits on top of `c309aad`
(`git rev-list --count c309aad..HEAD`, recounted at this documentation update —
recount again rather than trust this number once further commits land), on
this branch (`feat/dns-protocol`). This branch is a linear continuation of
`feat/clickhouse-archive-job` and then `feat/common-feature-tier` (whose own tip
is `c309aad`) — both are ancestors of `HEAD` here, and neither is merged to
`main` yet (`feat/common-feature-tier` has its own open PR). Deliverables:

- `LogType.DNS` and `DnsEvent` added to the sealed `NetworkEvent` hierarchy
- `dns-feature-v1`: 24 values, the 12-value common tier followed by dns's own
  12-value protocol tier
- the dns parser, mapper, `DnsBuildFeaturesUseCase`, and the Flink operators
  that window it
- the conn.log enrichment left join (`ConnSnapshotJoinFunction`), giving a dns
  record access to conn's common-tier fields
- both the online and archive jobs wired for two protocols, each with its own
  per-protocol operator uids and Kafka topics
- `contracts/source/zeek-dns-source-v1.json`, the dns source contract

The pipeline is now: external `conn` and `dns` topics → parse/validate →
bounded keyed state → per-schema `FeatureVector` (conn: 20 values, on
`netsec.conn.feature-vector.v1` / `netsec.conn.dlq.v1`; dns: 24 values, on
`netsec.dns.feature-vector.v1` / `netsec.dns.dlq.v1`) → archive job (four
Kafka-to-ClickHouse chains, one feature-vector and one DLQ chain per protocol,
built by `ArchiveJob.connAndDnsChains(...)`) → ClickHouse `feature_vectors` and
`invalid_events`, both now holding rows for either log type.

**`main` cannot currently run the online job at all.** Three serialization defects
(`SensorId` and both Kafka serializers not `Serializable`; two
`setValueSerializationSchema` lambdas erasing their generic type) make
`env.execute()` fail before a single record is read on `main`. **This branch is
not affected** — it already carries an equivalent fix, inherited from
`feat/clickhouse-archive-job` (commit `721c0cf`), not from
`fix/flink-job-serializability` (commit `eb4db7c`), which fixed the same three
defects independently on its own branch off the same `main` commit and is
confirmed NOT an ancestor of this branch. Neither fix is merged to `main`
itself, so `main` still cannot run the online job; this branch can —
`OnlineFeatureJobE2ETest` (2/2 at `6824194`, against real containers) is the
proof.

### Verification state

Docker was unavailable for most of this branch's development, so every
Testcontainers test SKIPPED and read as neutral. When Docker became available the
skips were hiding real defects — including a deduplication query that was
syntactically invalid and could never have executed. **Do not read a skipped
container test as a passing one.**

Verified fresh at `f22d426` (the commit immediately before this documentation
update lands — see Commands for why no single command proves the whole
reactor at once), no containers involved. Two fix waves have landed since the
counts below were last recorded (three correctness defects, then this
accuracy-and-limits pass); `domain`, `application` and `adapter-kafka` moved
as a result, and `adapter-flink`/`adapter-kafka` each gained one test in this
pass's own D6 (a blank-uid harness test and a dns/common-tier JSON field
comparison, respectively):

| Suite | Result |
|---|---|
| `domain` | 96/96, 0 skipped |
| `ports` | no tests exist (no test sources in the module) |
| `application` | 38/38, 0 skipped |
| `adapter-kafka` | 71/71, 0 skipped |
| `adapter-flink` | 30/30, 0 skipped |
| `adapter-clickhouse`, `InvalidEventRowMapperTest` only (filtered; the module's container tests were not run here) | 9/9, 0 skipped |
| `bootstrap-online-job`, `OnlineFeatureJobTopologyTest` only (filtered) | 5/5, 0 skipped |
| `bootstrap-archive-job`, `ArchiveJobTopologyTest` only (filtered) | 8/8, 0 skipped |

Verified earlier against real containers. Not re-run at this commit: this pass
ran only the filtered class per module shown above, never `adapter-clickhouse`
or either bootstrap module unfiltered (both hold further container tests, and
this machine OOM-kills those — see Commands):

| Suite | Result | What it actually proves |
|---|---|---|
| `adapter-clickhouse` (full suite, on `feat/clickhouse-archive-job`, before this unit) | 37/37, 0 skipped at the time — now stale | Includes `DdlMigrationTest` — `001_mvp_tables.sql` has now been executed by a real ClickHouse 25.8 server, not merely read. Stale because this unit's commit `22465c4` added a ninth test to `InvalidEventRowMapperTest` (`everyLogTypeHasASourceContractFileOnDisk`); that class alone is 9/9 fresh at `f22d426` (row above), so the true full-suite count is at least 38 and has not been re-verified against containers |
| `FeatureVectorDeduplicationTest` | 3/3 | The committed dedup query runs and resolves duplicates |
| `ClientV2InserterTest` | 4/4 | An unknown column is rejected, not silently skipped |
| `OnlineFeatureJobE2ETest` (at `6824194`) | 2/2, 0 skipped | conn and dns feature vectors both arrive from a real broker; the joined dns record carries `qualityFlags()==NONE`, an orphan dns record carries `CONN_ENRICHMENT_ABSENT`, and a malformed dns record reaches dns's own DLQ (`netsec.dns.dlq.v1`), never conn's |
| `ArchiveJobE2ETest` (at `6d50912`/`038057e`) | 2/2, 0 skipped | A 24-value dns feature vector and a dns rejection both reach ClickHouse under `log_type = 'dns'`, and a conn vector/rejection under `log_type = 'conn'`, through the exact four chains `ArchiveJob.main()` wires via `connAndDnsChains(...)` |

**Not verified:** `ClickHouseOutageTest` — the Definition of Done's headline claim
that a ClickHouse failure cannot stop feature production. It is OOM-killed during
container startup (two Flink mini-clusters plus two containers do not fit in
5.7 GiB) and has never run. It was deliberately not weakened to fit the machine.

**Known limits:**
- Conn.log enrichment will be absent for nearly every dns vector in production:
  Zeek writes a connection's conn.log line only after the connection ends (for
  UDP, after an inactivity timeout) — after the dns.log records inside it. The
  end-to-end test publishes conn first, deliberately, which is what lets it
  prove the join at all rather than model Zeek's real write order (see
  `ConnSnapshotJoinFunction`'s "AN HONEST LIMIT" comment).
- `byte_sum_5m` is always 0 for dns: dns.log carries no byte counts, so
  `DnsBuildFeaturesUseCase` folds `bytes = 0` for every record; several other
  common-tier values are near-constant for dns as a result.
- The rolling-window key set has no TTL — see the bounded-state invariant above
  and `OnlineFeatureJob`'s KNOWN GAP comment.
- `DnsWindowState` itself resolves to Flink's record/POJO serializer, but its two
  components, `RollingCounters` and `RecordTimingState`, have no public no-arg
  constructor, so Flink cannot treat them as nested POJOs: both fall back to
  `GenericTypeInfo`, backed by `KryoSerializer` (confirmed by resolving
  `TypeExtractor.createTypeInfo` for all three classes at this commit). Their
  state evolution story is Kryo's, unlike `ConnEnrichment`'s fully-POJO state.
- `ClickHouseOutageTest` has never run on this machine and must never be
  described as passing.
- The conn.log enrichment join is processing-order dependent: both Kafka
  sources use `noWatermarks()` and `KeyedCoProcessFunction` drains its two
  inputs in arrival order, not event-time order, so whether a given dns record
  gets enriched can depend on read timing, not only on what Zeek wrote.
  Reprocessing identical Kafka data can produce a different vector for the
  same record — see `ConnSnapshotJoinFunction`'s "PROCESSING-ORDER DEPENDENT"
  comment. The join must stay non-blocking regardless.
- The enrichment join's 30-minute TTL is processing-time, but
  `OnlineFeatureJob`'s sources start at `earliest()`, so a replay compresses
  hours of event time into minutes of processing time and the TTL may never
  fire: `ConnSnapshotJoinFunction`'s uid-keyed state then grows with one entry
  per distinct connection uid in the replayed history. This is a different,
  separate gap from the rolling-window key set's own missing TTL above.
- DNS event identity (`sensor:uid:trans_id`) has residual collision risk:
  `trans_id` is a client-chosen 16-bit value that can repeat within one flow
  (DNS over TCP/53, a reused UDP source port inside Zeek's inactivity timeout,
  or a retransmission). The 2026-09-10 per-protocol spec's §5.1 added
  direction and a timestamp to the OT protocols' ids for exactly this reason;
  DNS was declared safe without it. See `DnsEventMapper`'s eventId derivation.
- The two-protocol abstraction has known seams for a third protocol: the
  enrichment carrier is per-record-type (`DnsEvent.withEnrichment`), and
  `OnlineFeatureJob.build(conn, dns, sensor)` and
  `ArchiveJob.connAndDnsChains(4 topics)` are both shaped and named for
  exactly two protocols rather than genuinely N (unlike
  `ArchiveJob.build(List<LogTypeChain<?>>)`, which already is).
- `RollingCounters.record(...)` resets a bucket slot whenever its stored
  minute merely differs from the incoming one, not only when the incoming one
  is newer — an out-of-order arrival for an older minute erases a newer
  minute's counts for that key. Nothing requires the external `conn`/`dns`
  topics to be partitioned by `id_orig_h`, so the sensor's Kafka producer
  should partition by source IP to keep per-key arrival ordered.
- Offset-initialiser asymmetry: the online job always starts at `earliest()`
  while the archive job resumes from `committedOffsets(EARLIEST)`, so an
  online restart without a usable checkpoint replays the whole retention
  window — which compounds the enrichment-TTL gap above.

The common feature tier (`contracts/features/common-feature-tier-v1.json`) and
its `conn.log` enrichment carrier are implemented and now consumed:
`conn-feature-v1` predates the tier and is frozen without it, but
`dns-feature-v1` leads with it at indices 0-11.

Not yet implemented: ONNX inference (Day 9), predictions and `netsec.prediction.v1`
(Day 9), the model registry (Day 7), and the Python training project.

Design records: `docs/conn-foundation-pipeline.md` (Steps 2-7),
`docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md` and
`docs/clickhouse.md` (Step 8).

## Key reference files

| File | Purpose |
|---|---|
| `FINAL_ARCHITECTURE.md` | Architecture decisions and rationale |
| `PILOT_ARCHITECTURE.md` | Phased architecture implementation guide |
| `Repository_Structure.md` | Annotated tree, build map, 18-step implementation order |
| `Roadmap.md` | 20-day execution plan |
| `contracts/` | Canonical, immutable interface contracts |
| `.env.example` | All environment variable names (never commit `.env`) |
