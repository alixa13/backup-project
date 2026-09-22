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

**Data flow:** External Kafka (`conn`, `dns` and `netsec.modbus.raw.v1` topics) → Online Flink job (parse → validate → bounded keyed state → per-schema feature vector — conn's own 20 values, dns's 24 (the common tier, whose conn-derived fields come from a non-blocking left join against conn.log snapshots, plus dns's own protocol tier), modbus's 42 (exactly the externally frozen upstream contract, keyed per `(sensor, client, server, unit)`)) → internal Kafka topics (separate feature-vector and DLQ topics per protocol: `netsec.<protocol>.feature-vector.v1` and `netsec.<protocol>.dlq.v1` for `conn`, `dns` and `modbus`) → Archive Flink job → ClickHouse. ONNX inference is not yet wired into either job (see Implementation state).

**Hexagonal, one-way dependency chain:**
```
domain → ports → application → adapters → bootstrap
```
- `domain` — pure Java value objects and formulas. Zero imports of Kafka, Flink, ClickHouse, ONNX Runtime, Jackson, or Docker. Enforced by the module's own compile-time classpath.
- `ports` — input/output port interfaces. Depends only on `domain`.
- `application` — use cases and feature orchestration. Depends only on `domain` + `ports`. No Jackson, no Flink.
- `adapter-kafka/flink/onnx/clickhouse/registry-filesystem/monitoring` — each implements ports and carries framework imports. Adapters do not import each other, with one recorded
  exception: `adapter-flink` depends on `adapter-kafka`, because the operators that
  drive parsing (`ConnParseMapValidateFunction`, `DnsParseMapValidateFunction`,
  `ModbusParseMapValidateFunction`) construct the kafka-side parser and mapper
  themselves. That dependency is one-directional, and no other adapter pair is coupled — every other adapter's pom
  names only itself.
- `bootstrap-online-job` / `bootstrap-archive-job` — the **only** modules that wire concrete adapters together into a runnable Flink job.
- `training/` — independent Python project. Reads archived `FeatureVector` rows and contracts. Never reimplements Zeek parsing, normalization, windowing, categorical mapping, defaults, or feature ordering.

**Key invariants:**
- Java package root: `io.netsecml.platform`
- Java 21 throughout — use `record` for immutable data carriers, `sealed interface` + records + pattern-matching `switch` for closed hierarchies. Records with array components need defensive copies in compact constructor *and* in the accessor.
- Feature vector: exactly `schema.featureCount()` `float32` values, frozen per schema. Conn's registered schema reports 20, ordered per `contracts/features/conn-feature-schema-v1.json`; dns's registered schema reports 24, ordered per `contracts/features/dns-feature-schema-v1.json`; modbus's registered schema reports 42, ordered per `contracts/features/modbus-feature-schema-v1.json`. Feature order is frozen once defined — a change creates a new schema version.
- A platform-designed feature schema is the common tier (12 values, frozen) followed by
  that protocol's own tier. `conn-feature-v1` predates the tier and is frozen without it;
  every platform-designed schema from `dns-feature-v1` onward leads with it. A schema that
  mirrors an externally frozen contract is not platform-designed: it carries exactly what
  that contract specifies, no common tier added. `modbus-feature-v1` (42 values, mirroring
  the upstream model team's frozen contract, committed as
  `tests/fixtures/contracts/modbus_feature_contract_v1.json`) is the first of those.
- Event identity is a per-log-type obligation with its own stated argument: `CONN` is
  `sensor:uid`, `DNS` is `sensor:uid:trans_id`, `MODBUS` is
  `sensor:uid:tid:direction:ts_millis`. Modbus folds in direction because a request and its
  response are two records sharing one `tid`, and the millisecond timestamp because `tid` is
  a 16-bit client counter that a 10 Hz poll loop wraps in under two hours (see
  `ModbusEventMapper`'s eventId derivation). A log type whose uniqueness cannot be
  evidenced from its own fields is not ready to be added.
- Modbus endpoint orientation is the mapper's job. `ModbusEvent.sourceIp`/`destinationIp`
  are the source and destination of THIS record (per-packet), not the connection's
  originator and responder. Zeek's `id_orig_h`/`id_resp_h` are connection-level — identical
  on a request and on its response — so `ModbusEventMapper` orients them by direction
  (request: orig → resp; response: resp → orig), and passes per-packet
  `source_h`/`destination_h` through unchanged; when a record carries both pairs, the
  connection-level pair wins. `ModbusEntityKey.of` then swaps a response back into client/
  server order, which is correct only for per-packet input. This mirrors the upstream
  engine, which consumes per-packet source/destination and does the swap itself. Break it
  and every request and its response silently land in different state buckets, so no
  response ever matches. The mapper's `resolvePerPacketEndpoints` comment is the account to
  trust. Two documents predate this rule and still describe the two kinds of field as
  interchangeable: the design spec's §5 and §7
  (`docs/superpowers/specs/2026-09-21-modbus-stage1-design.md`), and the `id_orig_h`/`id_resp_h`
  notes in `contracts/source/zeek-modbus-source-v1.json`, which call `source_h`/`destination_h`
  aliases.
- CPU-only: no GPU, no CUDA, no deep-learning frameworks. ONNX Runtime Java with intra/inter-op threads pinned to 1 per subtask.
- Model bundle is pinned in job config and loaded once in `open()`. No live hot reload.
- ClickHouse is never on the online scoring path. Predictions go to Kafka first; the archive job writes to ClickHouse asynchronously.
- ClickHouse inserts are idempotent (`ReplacingMergeTree`). Do not promise exactly-once for the archive sink.
- Bounded per-key state only — no unbounded per-IP maps or event history. Conn and dns key
  per `(sensor, sourceIp)`; modbus keys per `(sensor, clientIp, serverIp, unitId)`
  (`ModbusEntityKey`). This is the rule the code aims at and holds per key — five one-minute
  buckets each for conn and dns; for modbus, trailing windows holding at most the last 60
  seconds of events plus a pending-TID map capped at 4096 entries — but not yet in aggregate:
  none of `ConnFeatureProcessFunction`'s `rolling-counters`, `DnsFeatureProcessFunction`'s
  `dns-window-state` or `ModbusFeatureProcessFunction`'s `modbus-entity-state` carries a TTL,
  so the KEY SET keeps every key ever seen, for all three protocols, forever (see
  `OnlineFeatureJob`'s KNOWN GAP comment, which covers conn and dns).
- `NetworkEvent` is a **sealed interface** over a shared `EventEnvelope`, with one record per log
  type. `permits` lists only log types that have a parser, mapper and feature schema — adding a
  record ahead of its implementation defeats the exhaustiveness checking that sealing buys.
- Feature schemas resolve through `FeatureSchemaRegistry`, by `LogType` at wiring time or by
  `schemaId` for an archived row. Both throw on an unknown key: an unresolvable schema is a
  deployment error, not a runtime condition. A vector's width, id and content hash all come from
  its registered schema, never from a literal. (Modbus is a partial exception: its extractor
  sizes the vector from a hardcoded schema constant — see "Modbus limits and decisions".)
- `BuildFeaturesUseCase<E, S>` has one implementation per log type, so no implementation casts or
  switches to discover what it was given.

**Contracts (`contracts/`)** are immutable, content-hashed, and language-neutral. A schema change creates a new version (`-v2`), never edits an existing file.

**Tech stack:** Java 21 / Flink 2.2.1 / `flink-connector-kafka:5.0.0-2.2` (Sink V2 / FLIP-27 Source API only — `SourceFunction`/`SinkFunction` V1 were removed in Flink 2.x) / Jackson 2.17 / ONNX Runtime Java / JUnit 5 / Python 3.10+ / scikit-learn / skl2onnx / onnxruntime / Testcontainers 1.21.4 (Kafka) for E2E.

## Implementation state

The conn foundation pipeline (Steps 2-7) is implemented and merged to `main`.
The ClickHouse archive job (Step 8) is complete on `feat/clickhouse-archive-job`
but not yet merged to `main`. Implementation order is tracked in
`Repository_Structure.md` Section E (18 steps).

The DNS unit added the platform's second protocol: every commit after `c309aad`
up to `149eff8`, the tip of `feat/dns-protocol`. That branch is a linear
continuation of `feat/clickhouse-archive-job` and then `feat/common-feature-tier`
(at `c309aad`), and neither of those is merged to `main` yet
(`feat/common-feature-tier` has its own open PR). DNS deliverables:

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

The Modbus unit (M1) adds the platform's third protocol, and its first OT one:
every commit on top of `f15f9f8` on this branch (`feat/modbus-stage1`). No
commit count is written here on purpose: an earlier one went stale twice within
a day as later commits landed. Run `git rev-list --count f15f9f8..HEAD` for the
current number. This branch continues linearly from `feat/dns-protocol` (tip
`149eff8`) and then `feat/conn-scoring-path` (tip `f15f9f8`). Both are
ancestors of `HEAD`, and neither is merged to `main`. `feat/conn-scoring-path`
is the conn scoring unit, parked partway through its plan
(`docs/superpowers/plans/2026-09-19-conn-scoring-serving-path.md`) when the
project turned to the OT protocols. Modbus deliverables:

- `modbus-feature-v1`: 42 values, mirroring the upstream model team's frozen
  contract feature for feature, with no common tier (see Key invariants)
- `LogType.MODBUS` and `ModbusEvent` added to the sealed `NetworkEvent` hierarchy
- the ICSNPP `modbus_detailed.log` parser and mapper (`JsonZeekModbusParser`,
  `ModbusEventMapper`) and `contracts/source/zeek-modbus-source-v1.json`, the
  modbus source contract
- the causal state machine (`ModbusEntityState`, keyed by `ModbusEntityKey`),
  `ModbusFeatureExtractor`, `ModbusBuildFeaturesUseCase`, and the Flink
  operators that run them (`ModbusParseMapValidateFunction`,
  `ModbusEntityKeySelector`, `ModbusFeatureProcessFunction`)
- both jobs wired for three protocols: `OnlineFeatureJob`'s three-protocol
  `build(env, bootstrapServers, conn, dns, modbus, sensor)` overload and
  `ArchiveJob.connDnsAndModbusChains(...)`, which each job's `main()` now calls.
  Each protocol has its own operator uids and Kafka topics. A uid is checkpoint
  identity. A DUPLICATE uid within one job fails loudly: in Flink 2.2.1,
  `StreamGraphHasherV2` throws `Hash collision on user-specified ID` while
  building the JobGraph, and both topology tests catch it earlier, without
  submitting a job. RENAMING a uid between versions is the risk no build step
  catches: the renamed operator's savepoint state no longer maps to it, so the
  restore fails, or, under `allowNonRestoredState`, that operator quietly starts
  empty.

The pipeline is now: external `conn`, `dns` and `netsec.modbus.raw.v1` topics →
parse/validate → bounded keyed state → per-schema `FeatureVector` (conn: 20
values, on `netsec.conn.feature-vector.v1` / `netsec.conn.dlq.v1`; dns: 24
values, on `netsec.dns.feature-vector.v1` / `netsec.dns.dlq.v1`; modbus: 42
values, on `netsec.modbus.feature-vector.v1` / `netsec.modbus.dlq.v1`) →
archive job (six Kafka-to-ClickHouse chains, one feature-vector and one DLQ
chain per protocol, built by `ArchiveJob.connDnsAndModbusChains(...)`; the
four-chain `connAndDnsChains(...)` is still public and still tested, but
`main()` no longer calls it) → ClickHouse `feature_vectors` and
`invalid_events`, both holding rows for all three log types.

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
`OnlineFeatureJobE2ETest` (3/3 against real containers, see Verification state)
is the proof.

### Verification state

Docker was unavailable for most of the DNS unit's development, so every
Testcontainers test SKIPPED and read as neutral. When Docker became available the
skips were hiding real defects — including a deduplication query that was
syntactically invalid and could never have executed. **Do not read a skipped
container test as a passing one.**

Verified fresh for the Modbus unit's last task, at `9f2e97d` plus the two
end-to-end test files that task's commit changes (the commit that records this
changes only those two files and this one). Each suite was run on its own, one
module at a time — see Commands for why no single command proves the whole
reactor at once. No containers were involved in this table:

| Suite | Result |
|---|---|
| `domain` | 155/155, 0 skipped |
| `ports` | no tests exist (no test sources in the module) |
| `application` | 68/68, 0 skipped |
| `adapter-kafka` | 120/120, 0 skipped |
| `adapter-flink` | 37/37, 0 skipped |
| `adapter-clickhouse`, `InvalidEventRowMapperTest` only (filtered; the module's container tests were not run here) | 9/9, 0 skipped |
| `bootstrap-online-job`, `OnlineFeatureJobTopologyTest` only (filtered) | 6/6, 0 skipped |
| `bootstrap-archive-job`, `ArchiveJobTopologyTest` only (filtered) | 9/9, 0 skipped |

Verified fresh at the same point against real containers (Kafka
`confluentinc/cp-kafka:7.6.1`, ClickHouse 25.8), each suite run alone and
filtered to its one class, never either bootstrap module unfiltered:

| Suite | Result | What it actually proves |
|---|---|---|
| `OnlineFeatureJobE2ETest` | 3/3, 0 skipped | conn and dns feature vectors both arrive from a real broker; the joined dns record carries `qualityFlags()==NONE`, an orphan dns record carries `CONN_ENRICHMENT_ABSENT`, and a malformed dns record reaches dns's own DLQ (`netsec.dns.dlq.v1`), never conn's. Through the three-protocol `build(...)` that `main()` calls, a modbus request and its response that carry IDENTICAL, unswapped connection-level `id_orig_h`/`id_resp_h` (direction from `request_response` alone) share one entity state: the response's 42-value vector has `outstanding_requests_before_event` 1, `response_without_request` 0, `rtt_valid` 1 and `rtt_s` equal to the published 0.25 s gap. A malformed modbus record reaches `netsec.modbus.dlq.v1`, and conn's and dns's DLQs stay empty. The same modbus method, run against the pre-fix DTO and mapper (from `7d0f685`), fails: the response finds no pending request (`outstanding_requests_before_event` 0) |
| `ArchiveJobE2ETest` | 3/3, 0 skipped | A 24-value dns feature vector and a dns rejection both reach ClickHouse under `log_type = 'dns'`, and a conn vector/rejection under `log_type = 'conn'`, through `connAndDnsChains(...)`. A 42-value modbus vector (carrying the `MODBUS_OUT_OF_ORDER` bit) and a map-stage modbus rejection both reach ClickHouse under `log_type = 'modbus'`, alongside exactly one conn and one dns row per table, through the exact six chains `ArchiveJob.main()` wires via `connDnsAndModbusChains(...)` |

Verified earlier against real containers, and not re-run for this update
(`adapter-clickhouse` was run only filtered, above, so its own container tests
were not re-run):

| Suite | Result | What it actually proves |
|---|---|---|
| `adapter-clickhouse` (full suite, on `feat/clickhouse-archive-job`, before the DNS unit) | 37/37, 0 skipped at the time — now stale | Includes `DdlMigrationTest` — `001_mvp_tables.sql` has now been executed by a real ClickHouse 25.8 server, not merely read. Stale because the DNS unit's commit `22465c4` added a ninth test to `InvalidEventRowMapperTest` (`everyLogTypeHasASourceContractFileOnDisk`); that class alone is 9/9 fresh (row above), so the true full-suite count is at least 38 and has not been re-verified against containers |
| `FeatureVectorDeduplicationTest` | 3/3 | The committed dedup query runs and resolves duplicates |
| `ClientV2InserterTest` | 4/4 | An unknown column is rejected, not silently skipped |

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
- No keyed feature state has a TTL, for any of the three protocols — see the
  bounded-state invariant above and `OnlineFeatureJob`'s KNOWN GAP comment.
  `ModbusFeatureProcessFunction`'s state inherits the gap from its conn and dns
  siblings, so its `(sensor, clientIp, serverIp, unitId)` key set grows forever
  too. That was ruled on, not missed: fixing all three belongs in its own unit.
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
  separate gap from the keyed feature state's own missing TTL above.
- DNS event identity (`sensor:uid:trans_id`) has residual collision risk:
  `trans_id` is a client-chosen 16-bit value that can repeat within one flow
  (DNS over TCP/53, a reused UDP source port inside Zeek's inactivity timeout,
  or a retransmission). The 2026-09-10 per-protocol spec's §5.1 added
  direction and a timestamp to the OT protocols' ids for exactly this reason;
  DNS was declared safe without it. See `DnsEventMapper`'s eventId derivation.
- The multi-protocol abstraction has known seams, and modbus paid for them
  rather than removing them: it arrived as a third `OnlineFeatureJob.build(conn,
  dns, modbus, sensor)` overload and a new
  `ArchiveJob.connDnsAndModbusChains(6 topics)`, each named and shaped for
  exactly three protocols, beside the two-protocol ones it left in place. A
  fourth protocol is another overload and another chains method (unlike
  `ArchiveJob.build(List<LogTypeChain<?>>)`, which is already genuinely N). The
  enrichment carrier is per-record-type too (`DnsEvent.withEnrichment`).
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

**Modbus limits and decisions** — each ruled on deliberately, not overlooked:
- **Per-key arrival order is a deployment requirement, not an enforced one.** The
  sensor's Kafka producer must partition the modbus topic by `(client_ip,
  server_ip)`. The upstream offline engine checks that its event index is
  strictly increasing and refuses to run otherwise; Kafka gives no such
  guarantee across partitions. The pending-TID machine, `rtt_s`, both deltas,
  `function_changed`, `inter_arrival_s` and the segment boundary all depend on
  arrival order. An out-of-order record (timestamp before its key's last one)
  resets the segment and sets `MODBUS_OUT_OF_ORDER` rather than failing the job.
  This is the modbus counterpart of the conn/dns `RollingCounters` ordering item
  above.
- **The pending-TID map is capped at 4096 entries per entity key.** Uncapped, it
  would grow without limit under a flood of unanswered requests, which is one of
  the attack shapes this detector exists to find. The upstream engine has no cap
  (it processes one finite capture). Over the cap, the eldest entry BY INSERTION
  is evicted (a `LinkedHashMap`, O(1), and never the entry just added). Under the
  per-key arrival order above, insertion order equals timestamp order.
- **`modbus-feature-v1` carries no conn-derived context.** A later model that
  wants it needs a `-v2` schema. The archived vectors are raw, so refitting
  stays possible.
- **A modbus DLQ row's `event_id` is a correlation key, not a join key.** A
  map-stage rejection's id is `sensor:uid:tid` only, because direction and
  timestamp are exactly what such a rejection can be about, so it never equals
  the `sensor:uid:tid:direction:ts_millis` id a successful mapping of the same
  record would get. Unlike dns, whose rejection id equals its success id, a join
  from `invalid_events.event_id` to `feature_vectors.event_id` returns nothing
  for modbus. Nothing is lost by it: `invalid_events` is a plain `MergeTree`
  with `event_id` outside its sort key.
- **The extractor and the registry are two sources for the modbus schema.**
  `ModbusFeatureExtractor` sizes and orders the vector from the hardcoded
  `ModbusFeatureSchemaV1.SCHEMA`, while `ModbusBuildFeaturesUseCase` labels it
  (id, hash) from the registry-resolved schema. A constructor fail-fast in the
  use case rejects any divergence at startup; making them one source is deferred.
- **Two ways to narrow from `NetworkEvent` now exist across three protocols.**
  `ParseMapValidateFunction` emits `NetworkEvent` for every protocol. Conn and dns
  narrow inside each consuming operator (each switches over the sealed permits,
  with a throw arm for every event type its chain never receives); modbus
  narrows once, at its chain's boundary, in a dedicated stateless operator
  (`modbus-event-narrow`), so its key selector and process function are typed on
  `ModbusEvent`. See `OnlineFeatureJob`'s `NarrowToModbusEvent` comment, so the
  next protocol's author chooses between the two deliberately.

The common feature tier (`contracts/features/common-feature-tier-v1.json`) and
its `conn.log` enrichment carrier are implemented and now consumed:
`conn-feature-v1` predates the tier and is frozen without it, but
`dns-feature-v1` leads with it at indices 0-11. `modbus-feature-v1` carries
none of it, by the scope clause in Key invariants.

Not yet implemented: ONNX inference in either job (Day 9), predictions on
`netsec.prediction.v1` (Day 9), and the Python training project. The parked conn
scoring unit (`feat/conn-scoring-path`, an ancestor of this branch) built some
of the pieces — the `ModelScorer` port and `ScoreFeaturesUseCase`,
`FilesystemModelRegistry`, `OnnxModelScorer` and `PredictionSerializer` — but
neither job calls any of them, so nothing scores a vector or publishes a
prediction yet.

Design records: `docs/conn-foundation-pipeline.md` (Steps 2-7),
`docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md` and
`docs/clickhouse.md` (Step 8), and
`docs/superpowers/specs/2026-09-21-modbus-stage1-design.md` (the Modbus unit;
see the endpoint-orientation invariant for the two sections of it that are
out of date).

## Key reference files

| File | Purpose |
|---|---|
| `FINAL_ARCHITECTURE.md` | Architecture decisions and rationale |
| `PILOT_ARCHITECTURE.md` | Phased architecture implementation guide |
| `Repository_Structure.md` | Annotated tree, build map, 18-step implementation order |
| `Roadmap.md` | 20-day execution plan |
| `contracts/` | Canonical, immutable interface contracts |
| `.env.example` | All environment variable names (never commit `.env`) |
