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

### Deployment (`deploy/`)
```sh
./deploy/deploy.sh help              # every command
bash deploy/tests/run-all.sh         # every deploy check that needs no running stack
```

The stack (`deploy.sh up`, `selftest`, `zeek-check --live`) runs on the server
only: the development machine does not have the hardware, so never start it
here. `deploy/README.md` is the operator guide.

## Architecture

**Data flow:** External Kafka (`conn`, `dns`, `netsec.modbus.raw.v1` and `netsec.s7comm.raw.v1` topics) → Online Flink job (parse → validate → bounded keyed state → per-schema feature vector — conn's own 20 values, dns's 24 (the common tier, whose conn-derived fields come from a non-blocking left join against conn.log snapshots, plus dns's own protocol tier), modbus's 42 (exactly the externally frozen upstream contract, keyed per `(sensor, client, server, unit)`), s7comm's 16 (exactly the externally frozen upstream contract, keyed per `(sensor, uid)`)) → internal Kafka topics (separate feature-vector and DLQ topics per protocol: `netsec.<protocol>.feature-vector.v1` and `netsec.<protocol>.dlq.v1` for `conn`, `dns`, `modbus` and `s7comm`) → Archive Flink job → ClickHouse. ONNX inference is not yet wired into either job (see Implementation state).

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
  `ModbusParseMapValidateFunction`, `S7commParseMapValidateFunction`) construct the kafka-side parser and mapper
  themselves. That dependency is one-directional, and no other adapter pair is coupled — every other adapter's pom
  names only itself.
- `bootstrap-online-job` / `bootstrap-archive-job` — the **only** modules that wire concrete adapters together into a runnable Flink job.
- `training/` — independent Python project. Reads archived `FeatureVector` rows and contracts. Never reimplements Zeek parsing, normalization, windowing, categorical mapping, defaults, or feature ordering.

**Key invariants:**
- Java package root: `io.netsecml.platform`
- Java 21 throughout — use `record` for immutable data carriers, `sealed interface` + records + pattern-matching `switch` for closed hierarchies. Records with array components need defensive copies in compact constructor *and* in the accessor.
- Feature vector: exactly `schema.featureCount()` `float32` values, frozen per schema. Conn's registered schema reports 20, ordered per `contracts/features/conn-feature-schema-v1.json`; dns's registered schema reports 24, ordered per `contracts/features/dns-feature-schema-v1.json`; modbus's registered schema reports 42, ordered per `contracts/features/modbus-feature-schema-v1.json`; s7comm's registered schema reports 16, ordered per `contracts/features/s7comm-feature-schema-v1.json`. Feature order is frozen once defined — a change creates a new schema version.
- A platform-designed feature schema is the common tier (12 values, frozen) followed by
  that protocol's own tier. `conn-feature-v1` predates the tier and is frozen without it;
  every platform-designed schema from `dns-feature-v1` onward leads with it. A schema that
  mirrors an externally frozen contract is not platform-designed: it carries exactly what
  that contract specifies, no common tier added. `modbus-feature-v1` (42 values, mirroring
  the upstream model team's frozen contract, committed as
  `tests/fixtures/contracts/modbus_feature_contract_v1.json`) is the first of those.
  `s7comm-feature-v1` (16 values, mirroring upstream's `STAGE1_RAW_FEATURES`, transcribed as
  `tests/fixtures/contracts/s7comm_stage1_raw_features_v1.json`) is the second; its two
  categorical features travel as codes (`S7commCategories`) whose decode reproduces
  upstream's category strings exactly.
- Event identity is a per-log-type obligation with its own stated argument: `CONN` is
  `sensor:uid`, `DNS` is `sensor:uid:trans_id`, `MODBUS` is
  `sensor:uid:tid:direction:ts_millis`. Modbus folds in direction because a request and its
  response are two records sharing one `tid`, and the millisecond timestamp because `tid` is
  a 16-bit client counter that a 10 Hz poll loop wraps in under two hours (see
  `ModbusEventMapper`'s eventId derivation). `S7COMM` is
  `sensor:uid:pdu_reference:direction:ts_millis`, for the same two reasons: a request and its
  response share the 16-bit PDU reference, which wraps within hours (see
  `S7commEventMapper`). A log type whose uniqueness cannot be
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
  trust.
- S7comm endpoint orientation follows upstream's own adapter (`kafka_source.py`), not modbus's
  precedence: complete per-packet `source_*`/`destination_*` are used as they are; otherwise
  the connection-level `id_orig_*`/`id_resp_*` pair (underscored, dotted, nested `id`, or bare)
  is oriented by `is_orig`, which is then required. A record is a request iff its destination
  port is 102. `S7commEventMapper` holds the rule; state is keyed by uid, so a request and its
  response share it whatever their endpoints.
- CPU-only: no GPU, no CUDA, no deep-learning frameworks. ONNX Runtime Java with intra/inter-op threads pinned to 1 per subtask.
- Model bundle is pinned in job config and loaded once in `open()`. No live hot reload.
- ClickHouse is never on the online scoring path. Predictions go to Kafka first; the archive job writes to ClickHouse asynchronously.
- ClickHouse inserts are idempotent (`ReplacingMergeTree`). Do not promise exactly-once for the archive sink.
- Bounded per-key state only — no unbounded per-IP maps or event history. Conn and dns key
  per `(sensor, sourceIp)`; modbus keys per `(sensor, clientIp, serverIp, unitId)`
  (`ModbusEntityKey`). This is the rule the code aims at and holds per key — five one-minute
  buckets each for conn and dns; for modbus, a pending-TID map capped at 4096 entries and three
  trailing windows bounded by TIME (the last 1, 10 and 60 seconds, as the frozen contract
  requires) and also by count: at most 100,000 entries each
  (`ModbusEntityState.MAX_WINDOW_ENTRIES`), beyond which a vector carries
  `MODBUS_WINDOW_SATURATED` (see "Modbus limits and decisions" below, F2); s7comm keys per
  `(sensor, uid)` (`S7commConnectionKey`): nine fixed rings of 16 or 32 entries and an
  outstanding set bounded by the 16-bit PDU reference space, and -- the only feature state in
  this codebase with one -- a one-hour processing-time idle TTL (`S7COMM_STATE_TTL_MINUTES`).
  Not yet in aggregate for the other three: none of `ConnFeatureProcessFunction`'s
  `rolling-counters`, `DnsFeatureProcessFunction`'s `dns-window-state` or
  `ModbusFeatureProcessFunction`'s `modbus-entity-state` carries a TTL, so the KEY SET keeps
  every key ever seen, for conn, dns and modbus, forever (see `OnlineFeatureJob`'s KNOWN GAP
  comment, which covers conn and dns).
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
every commit on top of `f15f9f8` on `feat/modbus-stage1`. No
commit count is written here on purpose: an earlier one went stale twice within
a day as later commits landed. Run `git rev-list --count f15f9f8..8770a08` for the
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

The S7comm unit adds the platform's fourth protocol, and its second OT one:
every commit after `8770a08` on `feat/s7comm-stage1`, which continues linearly
from `feat/modbus-stage1` (no commit count, for the reason above). S7comm
deliverables:

- `s7comm-feature-v1`: 16 values, mirroring the upstream model team's frozen
  raw contract, which both frozen S7 models (Stage 1 autoencoder, Stage 2
  COMMAND/FLOODING router) consume; no common tier
- `LogType.S7COMM` and `S7commEvent` added to the sealed `NetworkEvent` hierarchy
- the ICSNPP `s7comm.log` parser and mapper (`JsonZeekS7commParser`,
  `S7commEventMapper`, `S7commFieldValues`) and
  `contracts/source/zeek-s7comm-source-v1.json`
- the causal state (`S7commConnectionState`, keyed by `S7commConnectionKey`),
  `S7commFeatureExtractor`, `S7commBuildFeaturesUseCase`, and the Flink
  operators (`S7commParseMapValidateFunction`, `S7commConnectionKeySelector`,
  `S7commFeatureProcessFunction`, with the idle TTL)
- both jobs wired for four protocols: `OnlineFeatureJob`'s four-protocol
  `build(env, bootstrapServers, conn, dns, modbus, s7comm, sensor)` (and an
  overload taking the TTL, which `main()` calls) and
  `ArchiveJob.connDnsModbusAndS7commChains(...)`, which each job's `main()` now calls
- the parity oracle: `tests/fixtures/s7comm/upstream_oracle_v1.jsonl`, generated
  by running upstream's own builders (`tests/fixtures/s7comm/generate_upstream_oracle.py`)

The deployment unit (`feat/deploy-mvp`, from `s7`) puts the Modbus + S7comm
pipeline on one server: `deploy/deploy.sh` (doctor, install, tune, build, up,
down, restart, status, logs, sql, selftest, zeek-check, uninstall) over a
`netsec-ml` Compose project -- KRaft Kafka, ClickHouse, a Flink 2.2.1 session
cluster with a resuming job supervisor, and a pinned Zeek 7.0.9 sensor
(icsnpp-modbus v1.0.0, icsnpp-s7comm 7ebeb03, zeek-kafka v1.2.0). Both job
modules build a shaded `-all` JAR. `ZeekRecordCheck` (bootstrap-online-job)
runs real Zeek output through the production parsers; its test is pinned to
`tests/fixtures/zeek/`, real ICSNPP output. Design:
`docs/superpowers/specs/2026-09-24-server-deployment-design.md`.

The pipeline is now: external `conn`, `dns`, `netsec.modbus.raw.v1` and
`netsec.s7comm.raw.v1` topics →
parse/validate → bounded keyed state → per-schema `FeatureVector` (conn: 20
values, on `netsec.conn.feature-vector.v1` / `netsec.conn.dlq.v1`; dns: 24
values, on `netsec.dns.feature-vector.v1` / `netsec.dns.dlq.v1`; modbus: 42
values, on `netsec.modbus.feature-vector.v1` / `netsec.modbus.dlq.v1`; s7comm:
16 values, on `netsec.s7comm.feature-vector.v1` / `netsec.s7comm.dlq.v1`) →
archive job (eight Kafka-to-ClickHouse chains, one feature-vector and one DLQ
chain per protocol, built by `ArchiveJob.connDnsModbusAndS7commChains(...)`;
the six- and four-chain methods are still public and still tested, but
`main()` no longer calls them) → ClickHouse `feature_vectors` and
`invalid_events`, both holding rows for all four log types.

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
`OnlineFeatureJobE2ETest` (4/4 against real containers, see Verification state)
is the proof.

### Verification state

Docker was unavailable for most of the DNS unit's development, so every
Testcontainers test SKIPPED and read as neutral. When Docker became available the
skips were hiding real defects — including a deduplication query that was
syntactically invalid and could never have executed. **Do not read a skipped
container test as a passing one.**

Verified fresh for the S7comm unit and its final-review fixes, at `698487b`
(the review-fix commit; the commit after it, this file, changes no code).
The minor-fix commit after that (import order, comments, spec wording and one
new topology test) was re-verified for every module whose sources it touches:
`domain` 210/210, `adapter-flink` 61/61, `OnlineFeatureJobTopologyTest` 8/8 and
`ArchiveJobTopologyTest` 10/10. Its only change to the two E2E classes moves one
import, so they were compiled but not re-run against containers.
Supersedes the flood-fix verification at `ca51ff6`, which this repeats in full
and extends with the S7comm tests. Each suite was run on its own, one module at a time,
`target/surefire-reports/` cleared first — see Commands for why no single
command proves the whole reactor at once. No containers were involved in this
table:

| Suite | Result |
|---|---|
| `domain` | 210/210, 0 skipped (+38 over the prior 172: `S7commFeatureSchemaV1Test` ×6, `S7commCategoriesTest` ×7, `LongRingTest` ×4, `S7commConnectionKeyTest` ×3, `S7commConnectionStateTest` ×18 (one pins the run lengths past `Integer.MAX_VALUE`); `QualityFlagsTest`, `NetworkEventTest`, `NetworkEventSurfaceTest` and `LogTypeTest` updated for the fourth protocol with unchanged counts) |
| `ports` | no tests exist (no test sources in the module) |
| `application` | 82/82, 0 skipped (+8: `S7commBuildFeaturesUseCaseTest` ×7 and `S7commFloodTest` — a million events on one connection in ~6.5 s) |
| `adapter-kafka` | 145/145, 0 skipped (+24: `JsonZeekS7commParserTest` ×5, `S7commFieldValuesTest` ×8 (one rejects a 100,000-digit malformed number in well under 2 s), `S7commEventMapperTest` ×11) |
| `adapter-flink` | 61/61, 0 skipped (+16: `S7commUpstreamOracleTest` ×2 — the S7 parity proof: all 2,121 records of the upstream-generated oracle through the real parser, mapper and use case, every one of the 16 features bit-identical to upstream, zero mismatches, while a planted one-line bug gives 10,842 — `S7commParseMapValidateFunctionTest` ×5, `S7commFeatureProcessFunctionTest` ×6 including the TTL expiry and the reset after an outage longer than the TTL, `S7commConnectionStateSerializerTest` ×3. `ModbusGoldenVectorTest` gained only its required `S7commEvent` switch arm) |
| `adapter-clickhouse`, `InvalidEventRowMapperTest` and `SourceVersionContractTest` only (filtered; the module's container tests were not run here) | 10/10, 0 skipped |
| `bootstrap-online-job`, `OnlineFeatureJobTopologyTest` only (filtered) | 8/8, 0 skipped (the four-protocol topology: 32 distinct uids; the eighth test, added by the minor-fix commit, proves the TTL `main()` passes reaches `s7comm-features` -- a planted bug that drops it for the default fails it) |
| `bootstrap-archive-job`, `ArchiveJobTopologyTest` only (filtered) | 10/10, 0 skipped (eight chains: 24 distinct uids) |
| `bootstrap-online-job`, `ZeekRecordCheckTest` only (filtered) | 8/8, 0 skipped (real ICSNPP output: 84/84 s7comm accepted; modbus 45/48, the 3 rejections upstream's own engine makes) |
| `deploy/tests/run-all.sh` | every file 0 failed, shellcheck clean (no stack started: builds, stubs, `compose config`, offline `zeek -r`) |

Verified fresh at the same point against real containers (Kafka
`confluentinc/cp-kafka:7.6.1`, ClickHouse 25.8), each suite run alone and
filtered to its one class, never either bootstrap module unfiltered:

| Suite | Result | What it actually proves |
|---|---|---|
| `OnlineFeatureJobE2ETest` | 4/4, 0 skipped | conn and dns feature vectors both arrive from a real broker; the joined dns record carries `qualityFlags()==NONE`, an orphan dns record carries `CONN_ENRICHMENT_ABSENT`, and a malformed dns record reaches dns's own DLQ (`netsec.dns.dlq.v1`), never conn's. Through the three-protocol `build(...)`, a modbus request and its response that carry IDENTICAL, unswapped connection-level `id_orig_h`/`id_resp_h` (direction from `request_response` alone) share one entity state: the response's 42-value vector has `outstanding_requests_before_event` 1, `response_without_request` 0, `rtt_valid` 1 and `rtt_s` equal to the published 0.25 s gap, and a malformed modbus record reaches only `netsec.modbus.dlq.v1`. Through the four-protocol `build(...)` that `main()` calls, an S7 request and its response carrying the SAME connection-level `id_orig_*`/`id_resp_*`, told apart only by `is_orig`, come out with the response matched (`s7_outstanding_requests` 0, `s7_response_match_rate_16` 1), and a malformed S7 record reaches only `netsec.s7comm.dlq.v1` — conn's, dns's and modbus's DLQs stay empty |
| `ArchiveJobE2ETest` | 4/4, 0 skipped | A 24-value dns feature vector and a dns rejection both reach ClickHouse under `log_type = 'dns'`, and a conn vector/rejection under `log_type = 'conn'`, through `connAndDnsChains(...)`; a 42-value modbus vector (carrying `MODBUS_OUT_OF_ORDER`) and a map-stage modbus rejection under `log_type = 'modbus'` through `connDnsAndModbusChains(...)`; and a 16-value s7comm vector (carrying `S7COMM_OUT_OF_ORDER`) and a map-stage s7comm rejection (`source_version` `zeek-s7comm-source-v1`) under `log_type = 's7comm'`, with conn, dns and modbus still exactly one row each per table, through the exact eight chains `ArchiveJob.main()` wires via `connDnsModbusAndS7commChains(...)` |

Verified earlier against real containers, and not re-run for this update
(`adapter-clickhouse` was run only filtered, above, so its own container tests
were not re-run):

| Suite | Result | What it actually proves |
|---|---|---|
| `adapter-clickhouse` (full suite, on `feat/clickhouse-archive-job`, before the DNS unit) | 37/37, 0 skipped at the time — now stale | Includes `DdlMigrationTest` — `001_mvp_tables.sql` has now been executed by a real ClickHouse 25.8 server, not merely read. Stale because the DNS unit's commit `22465c4` added a ninth test to `InvalidEventRowMapperTest` (`everyLogTypeHasASourceContractFileOnDisk`); that class alone is 9/9 fresh (run with `SourceVersionContractTest` in the filtered row above), so the true full-suite count is at least 38 and has not been re-verified against containers |
| `FeatureVectorDeduplicationTest` | 3/3 | The committed dedup query runs and resolves duplicates |
| `ClientV2InserterTest` | 4/4 | An unknown column is rejected, not silently skipped |

**Not yet verified: the deployed stack itself.** `deploy.sh up`, `selftest`,
`zeek-check --live` and a `down`/`up` restore have not run anywhere: the
development machine cannot hold the stack. The first run on the server is that
verification.

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
- No keyed feature state has a TTL for conn, dns or modbus — see the
  bounded-state invariant above and `OnlineFeatureJob`'s KNOWN GAP comment.
  `ModbusFeatureProcessFunction`'s state inherits the gap from its conn and dns
  siblings, so its `(sensor, clientIp, serverIp, unitId)` key set grows forever
  too. That was ruled on, not missed: fixing those three belongs in its own unit.
  s7comm's state is the exception: keyed per connection, it carries an idle TTL
  from the start (see "S7comm limits and decisions").
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
  exactly three protocols, beside the two-protocol ones it left in place. s7comm,
  the fourth protocol, paid the same way: a fourth `build(...)` overload (plus one
  taking its TTL) and `ArchiveJob.connDnsModbusAndS7commChains(8 topics)`. A fifth
  protocol is another overload and another chains method (unlike
  `ArchiveJob.build(List<LogTypeChain<?>>)`, which is already genuinely N). The
  enrichment carrier is per-record-type too (`DnsEvent.withEnrichment`).
- `RollingCounters.record(...)` resets a bucket slot whenever its stored
  minute merely differs from the incoming one, not only when the incoming one
  is newer — an out-of-order arrival for an older minute erases a newer
  minute's counts for that key. Nothing requires the external `conn`/`dns`
  topics to be partitioned by `id_orig_h`, so the sensor's Kafka producer
  should partition by source IP to keep per-key arrival ordered.
- **Every topic must exist before either job starts.** A Kafka source on a
  missing topic fails its split enumerator, and each job's `main()` now
  subscribes to every protocol's topics unconditionally (online: the four raw
  topics; archive: all eight feature-vector and DLQ topics). A site that runs
  no S7 (or Modbus) sensor must still create those topics, empty; otherwise
  the whole job -- all four protocols -- restarts in a loop.
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
- **Two ways to narrow from `NetworkEvent` now exist across four protocols.**
  `ParseMapValidateFunction` emits `NetworkEvent` for every protocol. Conn and dns
  narrow inside each consuming operator (each switches over the sealed permits,
  with a throw arm for every event type its chain never receives); modbus
  narrows once, at its chain's boundary, in a dedicated stateless operator
  (`modbus-event-narrow`), so its key selector and process function are typed on
  `ModbusEvent`. s7comm chose the modbus shape (`s7comm-event-narrow`). See
  `OnlineFeatureJob`'s `NarrowToModbusEvent` comment, so the next protocol's author
  chooses between the two deliberately.
- **Flood cost (F2): fixed, and bounded.** `ModbusEntityState` is mutable and
  updated in place (`advance` returns a `BeforeEvent` snapshot of the values the
  causal features read), with `07b`'s running 10 s counts restored, so each event
  costs amortized O(1) on the default heap state backend: every window entry is
  appended once and leaves once. Measured on the real use case, single thread,
  one key, 60 s of event time, no Flink or Kryo overhead: before, ~820 events/s
  against a 1,000/s answered flood (falling behind real time) and ~510-560
  events/s at 10,000/s; after, ~250,000-350,000 events/s steady at both rates,
  answered or not. `ModbusFloodTest` (300,000 events at 5,000/s on one key, 30 s
  bound, run uncapped so the 60 s window really grows to 300,000 entries) timed
  out before and takes ~1.5 s after; a planted O(window) regression times it out
  again. Memory is O(60 × rate) per key,
  capped at 100,000 entries per window (1 s, 10 s, 60 s): over the cap the oldest
  entry is evicted, and for exactly as long as an evicted entry would still be
  inside the uncapped window, the vector carries `MODBUS_WINDOW_SATURATED` (bit 3,
  value 8): some of its window features (indices 35-41) differ from `07b`'s. Every
  vector without that bit has exactly the uncapped engine's window values, and
  indices 0-34 are exact either way (`ModbusWindowSaturationTest`). The bit does
  not say which window: the three share one cap, so the 60 s window reaches it
  first (above ~1,667 events/s on one key), and in that common case only
  `event_rate_60s` (index 37) differs. Above 10,000/s the 10 s window saturates
  too — its event rate is then lower, its unique counts can be, and its read/write
  ratios can move either way — and the 1 s window needs 100,000/s. A consumer that
  cannot tell must treat all of 35-41 as suspect. The bit never appears together
  with `MODBUS_OUT_OF_ORDER`: an out-of-order event resets the state. Parity with the pre-fix
  engine is proved event by event, bit for bit, by `ModbusEngineParityTest`
  against a verbatim copy of the engine at `1d0878e` (the application module's
  `feature.reference` test package, never to be edited to follow production).
  **Caveats.** The in-place safety and the O(1) cost hold on the heap backend
  only: Flink 2.2.1's `CopyOnWriteStateMap.get` hands out a serializer copy of a
  state object a running checkpoint still holds (O(state size), once per key per
  checkpoint), which is safe only because `ModbusFeatureProcessFunction` reads the
  state through `value()` on every call and never caches it. On RocksDB/ForSt,
  every `value()`/`update()` (de)serializes the whole state, so per-event cost
  returns to O(window size) there; nothing in the online job pins the heap backend,
  so a cluster-level `state.backend.type` of `rocksdb` or `forst` would bring the
  flood cost back silently (correctness holds, because the operator still calls
  `update()`). The copy-on-write argument also assumes Kryo copies these
  collections deeply, which the default serialization config does
  (`ModbusEntityStateSerializerTest`) and the online job does not override.
  **Memory is still not bounded in aggregate.** A fully saturated key holds
  ~300,000 window entries — on the order of 10 MB of heap, and of checkpoint — and
  the key includes the unit id, so one client/server pair flooding across all 256
  unit ids is 256 keys, ~2.5 GB. With no TTL on the key set (see the
  bounded-state invariant), none of it is ever released, and an idle key keeps
  whatever its windows held at its last event, since nothing purges without an
  event. `ArrayDeque`/`HashMap` never shrink, so a key whose windows once grew
  keeps that capacity until its segment ends; `reset()` allocates fresh
  collections so a new segment does not inherit it. Bounding the aggregate needs
  the TTL unit, or a cap on keys.
- **`ModbusEntityState` falls back to Kryo (`GenericTypeInfo`)**, like
  `DnsWindowState`'s two components (`RollingCounters`, `RecordTimingState`):
  Flink's POJO analysis requires a public no-arg constructor and bean-style
  get/is-prefixed no-argument accessors, and this class has neither (private
  constructor; accessors are not get/is-prefixed). Its savepoint-evolution story is
  therefore Kryo's, not the POJO serializer's. F2's repair changed its field layout
  (running counts, the per-window cap and the cap-eviction timestamps), which was
  free only because the job had never been deployed, so no savepoint existed; any
  later change to its fields is not free.
- **Modbus event-id residual collision.** Two records sharing the same uid, tid
  and direction inside one millisecond get one event id
  (`sensor:uid:tid:direction:ts_millis`). Under a flood the tid is
  attacker-controlled, so `ReplacingMergeTree` can collapse archived rows onto
  each other; the Kafka stream itself is unaffected (its records still carry
  distinct offsets). The DNS equivalent (`trans_id` collision risk) is already
  listed above; this is modbus's.
- **Exception, unknown and vendor function names are DLQ'd (F4), and their
  requests stay pending.** `ModbusFunctionCode.codeOf` resolves the 21
  transcribed upstream names, digits, `FUNCTION_<n>` and `(FC|FUNCTION)_?<n>`
  only. Zeek's `<NAME>_EXCEPTION` PDUs, vendor names such as `PROGRAM_484`, and
  `unknown-<n>` all reach the DLQ as `MISSING_REQUIRED_FIELD` and are never
  scored — so exception storms and function-code scans, classic Modbus attack
  signatures, are not scored — and a DLQ'd exception response leaves its
  request pending in state, which inflates `outstanding_requests_before_event`
  on that key's later, scored records and can set `request_overwrite_same_tid`
  when the tid comes round again. This matches `07b`, which raises on an
  unparseable function code, so it is not a parity defect: resolving these
  codes would feed out-of-distribution records to a frozen model, and is the
  model team's call, not a Java-side fix. Measured on real Zeek output
  (`tests/fixtures/zeek/`, `ZeekRecordCheckTest`): 3 of ICSNPP's 48 sample
  records are DLQ'd this way -- an `_EXCEPTION` response, and both halves of a
  function-43 exchange, because Zeek names function 43
  `ENCAP_INTERFACE_TRANSPORT` while upstream's `FUNCTION_NAME_TO_CODE` spells it
  `ENCAPSULATED_INTERFACE_TRANSPORT`, so upstream's engine rejects real FC-43
  records too.
- **The sensor must run icsnpp-modbus v1.0.0.** v2.0.0 (2025-09-03) writes
  `modbus_detailed` as one record per request/response pair (`matched`,
  `request_values`, `response_values`; no `is_orig`, no `request_response`, one
  `ts`). Such a record is rejected (`direction is required`), never misread
  (`ZeekRecordCheckTest`). The deployment's Zeek image pins v1.0.0.
- **Partitioning precision.** The per-key arrival-order requirement above,
  "partition the modbus topic by `(client_ip, server_ip)`", means the
  CONNECTION-level pair (Zeek's `id_orig_h`/`id_resp_h`), never the per-packet
  pair (`source_h`/`destination_h`): partitioning by the per-packet pair would
  send a request and its response — which carry OPPOSITE per-packet
  source/destination — to different partitions, defeating the whole point of
  partitioning by key.
- **Timestamp precision (F1, fixed 2026-09-23).** The causal engine reads
  `ModbusEvent.tsSeconds()` — the wire `ts` exactly, unrounded — never a value
  derived from `envelope().eventTime()`, which stays millisecond-rounded for
  the event id and the ClickHouse `DateTime64(3)` column only. Before the fix,
  both were derived from the same millisecond-rounded Instant, so
  `inter_arrival_s`/`rtt_s` and every window boundary were wrong on
  sub-millisecond wire timestamps (which Zeek's JSON writer routinely emits) --
  proved 84% high on a measured RTT pair, and shown to miss the 15s segment
  reset and the 1s window edge. `ModbusGoldenVectorTest`
  (`modules/adapter-flink`) is the regression test: it drives raw
  microsecond-timestamped JSON through the real parser, mapper and use case and
  asserts all 42 values, exactly, against a hand derivation of `07b`'s
  `process_capture`.

**S7comm limits and decisions** — each ruled on deliberately, not overlooked
(`docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md`, rulings R1-R9):
- **Per-uid arrival order is a deployment requirement.** Upstream processes
  records in stored order, and every history and run depends on order. The
  sensor's producer must partition the s7comm topic by uid. A record earlier than
  its connection's last is processed where it arrives and flagged
  `S7COMM_OUT_OF_ORDER` (bit 4, value 16); nothing is reset, since no
  s7comm-feature-v1 feature reads time.
- **Stricter than upstream on input (R1-R3).** uid is required (upstream falls
  back to an endpoint flow key), the PDU reference is required and 0-65535, and
  the two codes must be 0 <= n < 2^24 (exact in float32). Such records go to the
  DLQ where upstream would fall back or format them; Zeek always writes uid and
  the 16-bit reference.
- **A code-less unknown `function_name` is the unseen code -2 (R6).** Upstream
  uses the upper-cased name itself as the category; a float cannot carry it. It
  decodes to `"__UNSEEN__"`, which both trained encoders treat as unseen -- equal
  to upstream only if such a name never appeared in training. ICSNPP writes the
  code whenever it writes a name, so the case is not expected.
- **Entropy arithmetic cannot change the vector (R7).** Java uses plain summation
  and `ln(x)/ln(2)`; over every reachable input (all 65,534 ordered count
  sequences, n = 2..16) its float32 results equal Python's, compensated `sum` or
  not. A window of one distinct value yields upstream's `-0.0`, sign included,
  and the vector keeps it.
- **The idle TTL (R4) cuts connections after an outage.** One hour of
  processing time with no event (`S7COMM_STATE_TTL_MINUTES`). While the job
  runs, Zeek starts a new uid after its TCP inactivity timeout (5 minutes by
  default), so an idle hour normally ends nothing live. But the TTL counts
  PROCESSING time and each key's last-write time is restored with a
  checkpoint: after downtime or a stall longer than the TTL, every S7
  connection is expired on its next record and restarts from empty state
  (outstanding requests forgotten, so their responses count as unmatched,
  runs reset), silently -- no quality bit marks those vectors
  (`S7commFeatureProcessFunctionTest.aRestoreAfterAnOutageLongerThanTheTtlStartsTheConnectionFresh`
  pins it). The same happens to a connection Zeek keeps open with keepalives
  but no S7 PDUs for longer than the TTL. Only catch-up after a replay, which
  compresses event time, makes it fire late. Set the TTL above the longest
  expected outage.
- **s7comm event-id residual collision.** Two records sharing uid, PDU reference
  and direction inside one millisecond get one id
  (`sensor:uid:pdu_reference:direction:ts_millis`), and `ReplacingMergeTree`
  may collapse their archived rows. A map-stage rejection's id is
  `sensor:uid:pdu_reference` (or `sensor:uid`): a correlation key, not a join key.
- **`S7commConnectionState` is Kryo (`GenericTypeInfo`)**, like the modbus state;
  its layout is free to change only until the first savepoint. Its `BitSet` is
  serialized by Kryo 5.6.2's built-in `BitSetSerializer`.
- **No conn.log context and no scoring.** A later model wanting conn context needs
  a `-v2` schema; scoring (and Stage 1's 16-event sequence assembly) is a later
  unit, which decodes the two codes with `S7commCategories`.

The common feature tier (`contracts/features/common-feature-tier-v1.json`) and
its `conn.log` enrichment carrier are implemented and now consumed:
`conn-feature-v1` predates the tier and is frozen without it, but
`dns-feature-v1` leads with it at indices 0-11. `modbus-feature-v1` and
`s7comm-feature-v1` carry none of it, by the scope clause in Key invariants.

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
its §5 and §7 carried a defect-shaped description of the endpoint fields,
corrected in place 2026-09-23 with dated notes), and
`docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md` (the S7comm unit).

## Key reference files

| File | Purpose |
|---|---|
| `FINAL_ARCHITECTURE.md` | Architecture decisions and rationale |
| `PILOT_ARCHITECTURE.md` | Phased architecture implementation guide |
| `Repository_Structure.md` | Annotated tree, build map, 18-step implementation order |
| `Roadmap.md` | 20-day execution plan |
| `contracts/` | Canonical, immutable interface contracts |
| `.env.example` | All environment variable names (never commit `.env`) |
