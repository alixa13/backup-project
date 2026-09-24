# Server Deployment (Modbus + S7comm MVP) — Design

**Date:** 2026-09-24
**Branch:** `feat/deploy-mvp`, cut from `s7` (= `feat/s7comm-stage1`, `0af2c2f`)
**Status:** design approved in conversation (sections 1-3 and the resource-tuning
addition); this document is the written record.

## 1. Goal

Take the platform off the developer's machine and onto the user's real server, as one
self-contained stack driven by a single script, `deploy/deploy.sh`. On that server:

Zeek sniffs the OT network interface → writes ICSNPP `modbus_detailed` and `s7comm`
records to Kafka → the online Flink job turns them into 42-value (Modbus) and 16-value
(S7comm) feature vectors, with bad records on each protocol's DLQ → the archive Flink
job writes both into ClickHouse.

**Success** is: on a fresh server with internet, `deploy.sh install --interface <if>`,
`deploy.sh build` and `deploy.sh up` bring the whole stack up; `deploy.sh selftest`
passes; `deploy.sh status` shows real Modbus/S7 traffic arriving as rows in ClickHouse;
`deploy.sh down` then `up` resumes both jobs from their saved state.

## 2. Decisions already made (with the user)

| # | Decision | Why |
|---|---|---|
| D1 | **Our own complete stack**, not a reuse of the server's existing `new-project` containers | Their Flink is 1.x (our jobs need 2.2.1); their Zeek has no ICSNPP Modbus/S7 packages |
| D2 | **Docker Compose**, one project named `netsec-ml`, its own network | Docker is already on the server; clean start/stop/remove; pinned versions |
| D3 | **Build on the server** (it has full internet): JARs compiled inside a Maven container, Zeek image built locally | No Java/Maven on the host, no artifacts to copy by hand |
| D4 | **The existing `new-project` containers are never touched** — no restarts, no port changes, no shared network | They are live |
| D5 | **No scoring in this unit.** No Modbus/S7 model binaries exist. The `predictions` table is created and stays empty | The user will say when the S7 model is complete; scoring is then added to this deployment |
| D6 | **Resources are sized from the hardware** by a `tune` step (§7) | User request |
| D7 | **A real-Zeek parity check** on ICSNPP's own published sample traces is part of verification (§10) | The Modbus unit's two Criticals came from field names matching while meanings did not |

## 3. Services

All in `deploy/docker-compose.yml`, project `netsec-ml`:

| Service | Image (pinned) | Role |
|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.6.1`, KRaft single node, no zookeeper | All topics (§5). The same image the E2E tests prove against |
| `clickhouse` | `clickhouse/clickhouse-server:25.8` | `feature_vectors`, `invalid_events`, `predictions` — created by the existing `infrastructure/clickhouse/ddl/*.sql` |
| `flink-jobmanager`, `flink-taskmanager` | `flink:2.2.1-java21` | A session cluster running the two jobs. Exactly the Flink version the code is compiled against |
| `job-submitter` | same Flink image, one-shot, `restart: on-failure` | Waits for the JobManager, then submits each job that is not already RUNNING, resuming from its newest savepoint or retained checkpoint (§6) |
| `zeek` | built from `deploy/zeek/Dockerfile` (§4) | Sniffs `ZEEK_INTERFACE`, writes the two OT logs to Kafka |
| `init` (one-shot) | `cp-kafka` and `curl`-capable images | Creates topics; applies ClickHouse DDL. Both idempotent |

Every long-running service has `restart: unless-stopped`, so a server reboot brings the
stack back, and `job-submitter` brings the jobs back with their state.

**Ports** — bound to `127.0.0.1` only by default, all configurable in `deploy/.env`,
none colliding with the server's existing 8081/9092/2181/5432/5000/8000:

| Port | What |
|---|---|
| `127.0.0.1:18081` | Flink web UI / REST |
| `127.0.0.1:19092` | Kafka, host listener (Zeek uses it; also for debugging) |
| `127.0.0.1:18123` | ClickHouse HTTP |

Inside the compose network, services use `kafka:29092` and `clickhouse:8123`.

**Zeek networking:** `network_mode: host` with `NET_RAW` and `NET_ADMIN`, so it can
open the interface; it reaches Kafka on the host listener `127.0.0.1:19092`.

**Data** lives under `deploy/data/` (override: `NETSEC_DATA_DIR`), one folder per
service: `kafka/`, `clickhouse/`, `flink/checkpoints/`, `flink/savepoints/`. Both
`deploy/data/` and `deploy/.env` are git-ignored.

## 4. The Zeek sensor

**Image:** `zeek/zeek` (version pinned to the newest LTS that builds all three packages;
chosen at plan time by actually building) plus build tools and `librdkafka`, then `zkg`
installs, each **pinned**:

| Package | Pin | Why the pin matters |
|---|---|---|
| `icsnpp-modbus` | **`v1.0.0`** | v1.0.0 writes `modbus_detailed` **one record per packet** (`is_orig`, `source_h`/`destination_h`, `request_response`) — the shape upstream's frozen engine (`0761…py`, `07b…py`) and our `JsonZeekModbusParser` consume. **v2.0.0 (its current release) writes one record per request/response pair** (`matched`, `request_values`, `response_values`, no `request_response`, no `is_orig`): an unpinned `zkg install` would silently break every Modbus record |
| `icsnpp-s7comm` | a specific commit (the repo has no release tags) | Its `s7comm` record is per packet with `is_orig`, `source_*`/`destination_*`, `rosctr_code`, `pdu_reference`, `function_code` (hex string, e.g. `"0x04"`) and `function_name` — what `S7commEventMapper` reads. A future commit could change that |
| `zeek-kafka` (SeisoLLC) | a release tag | The Kafka log writer |

**Zeek configuration** (`deploy/zeek/local.zeek`):
- loads the three packages;
- `Log::default_scope_sep = "_"`, so the connection tuple is written `id_orig_h` —
  the spelling this platform's sensor has always used (both parsers also accept
  `id.orig_h`);
- JSON with epoch-seconds timestamps;
- Kafka filters sending **only** `Modbus::LOG_DETAILED` → `netsec.modbus.raw.v1` and
  `S7COMM::LOG_S7COMM` → `netsec.s7comm.raw.v1`, each message the bare record (no
  log-name wrapper);
- no local log files (Kafka is the record), so the sensor never fills the disk;
- checksum validation off (`-C`), which mirrored/SPAN traffic with NIC offload needs.

Exact identifiers (log IDs, filter syntax, option names) are confirmed at plan time
against the pinned sources, and the §10.3 parity check proves the result.

## 5. Kafka topics

Created by the `init` step before any job starts (the jobs subscribe to every protocol's
topics unconditionally, and a missing topic crash-loops the whole job). Auto-creation is
off, so a typo'd topic fails loudly instead of appearing silently.

| Topics | Partitions | Retention |
|---|---|---|
| `netsec.modbus.raw.v1`, `netsec.s7comm.raw.v1` | **1** | 7 days |
| `netsec.{modbus,s7comm}.feature-vector.v1` | 1 | 7 days |
| `netsec.{modbus,s7comm}.dlq.v1` | 1 | 14 days |
| `conn`, `dns`, `netsec.{conn,dns}.{feature-vector,dlq}.v1` | 1 | 1 day — created **empty**, only because the jobs require them |

**One partition on the raw topics is deliberate**: it gives total order per topic, which
satisfies both protocols' per-key arrival-order requirement (Modbus by connection pair,
S7comm by uid) without any producer-side keying. Its cost is that one online-job source
subtask reads each raw topic; OT traffic volumes make that acceptable for the MVP.

## 6. Flink

**Packaging.** Both bootstrap modules gain a `maven-shade-plugin` execution producing a
self-contained JAR (`online-feature-job.jar`, `archive-job.jar`): the Kafka connector,
Jackson, the ClickHouse client and our own modules inside; Flink's own runtime
(`flink-streaming-java`, `flink-clients`, their transitive Flink artifacts) left out,
because the Flink image provides them and two copies on one classpath break class
loading. `META-INF/services` files are merged. No job code changes.

**Cluster config** (via `FLINK_PROPERTIES`): checkpoint storage on the host volume,
checkpoints **retained on cancellation**, savepoint directory on the host volume, memory
and slots from §7. Both jobs already enable 30 s checkpoints, a failure-rate restart
strategy and at-least-once Kafka sinks in their own `main()`.

**Configuration reaches the jobs through the submitter.** Both jobs read every setting
(`KAFKA_BOOTSTRAP_SERVERS`, topic names, `SENSOR_ID`, `S7COMM_STATE_TTL_MINUTES`,
`CLICKHOUSE_*`) with `System.getenv` inside `main()`, which in a session cluster runs in
the submitting process. So `job-submitter` carries those variables; the JobManager and
TaskManager need none of them.

**Submission and resume** (`deploy/flink/submit-jobs.sh`, run by `job-submitter` and by
`deploy.sh up`):
1. Wait for the JobManager's REST endpoint.
2. For each job (`online-feature-job`, `archive-job`), skip it if a job with that name is
   already RUNNING — so the submitter is safe to re-run.
3. Otherwise find its newest restore point: the newest savepoint under
   `savepoints/<job>/` or retained checkpoint under `checkpoints/<job>/`, whichever is
   newer and has a `_metadata` file. Each job is submitted with its own checkpoint
   directory (`-D` at submission), which is what makes "newest checkpoint for this job"
   answerable.
4. Submit with `-s <restore point>` if one exists, else fresh.

**`deploy.sh down`** stops each job with a savepoint (`flink stop --savepointPath`)
before stopping containers, so a planned stop never loses state. An unplanned stop
(power loss) resumes from the newest retained checkpoint instead, at most ~30 s old.

## 7. Resource tuning (`deploy.sh tune`)

Runs automatically inside `install`; re-runnable any time, then `deploy.sh restart`
applies the new sizes. `--dry-run` prints without writing.

**Detects:** CPU cores (`nproc`), total RAM, and **available** RAM (`MemAvailable`),
which already excludes what the server's existing containers use. When our own stack is
running, its current usage (`docker stats`) is added back, so re-running `tune` is stable.

**Budget:** `B = min(MemAvailable − reserve, 0.8 × MemTotal)`, with
`reserve = max(1 GiB, 10% of MemTotal)` left for the OS and the other containers.
Overrides: `--memory-budget <size>`, `--cpus <n>`.

**Memory split of `B`** (each clamped to its floor and ceiling):

| Service | Share | Floor | Ceiling | Becomes |
|---|---|---|---|---|
| Flink TaskManager | 40% | 1280 MiB | 16 GiB | `taskmanager.memory.process.size` + container limit |
| ClickHouse | 25% | 768 MiB | 16 GiB | container limit; `max_server_memory_usage` = 80% of it |
| Kafka | 15% | 768 MiB | 6 GiB | container limit; JVM heap = half of it (the rest is page cache) |
| Flink JobManager | fixed | 768 MiB | 1 GiB | `jobmanager.memory.process.size` + container limit |
| Zeek | 10% | 384 MiB | 4 GiB | container limit |

The floors sum to ~4 GiB: below a 4 GiB budget `tune` refuses (the stack would be
OOM-killed) unless `--force` is given, and says why.

**CPU:** usable cores `C = cores − max(1, 25% of cores)`, the rest left to the host and
the existing containers. Flink parallelism is 1 below 8 cores, 2 below 16, else 4;
TaskManager slots = 2 × parallelism (two jobs). Each service gets a Docker `cpus` limit
from its share of `C` (TaskManager 45%, ClickHouse 25%, Kafka 15%, Zeek 15%), so our
stack can never starve the server's other services.

**Output:** a marked block in `deploy/.env` (`# --- resources: written by deploy.sh tune
<date> ---`) and a printed table — detected hardware, what other containers use, and
each service's allocation. `up` warns when the hardware differs from what the block
records.

## 8. `deploy.sh` commands

| Command | Does |
|---|---|
| `doctor` | Read-only: OS, Docker ≥ 24 and the compose plugin, CPU/RAM/disk (warns under 4 cores, 8 GB, 50 GB free), our ports free, `ZEEK_INTERFACE` exists (lists interfaces), internet reachable |
| `install [--interface IF]` | Installs Docker + compose plugin **only if missing** (official Docker repository for Debian/Ubuntu/RHEL-family); creates `deploy/.env` from `.env.template` (generated ClickHouse password, `SENSOR_ID` = hostname, the interface), mode 600; creates data folders; runs `tune`. Idempotent |
| `tune [--dry-run] [--memory-budget S] [--cpus N] [--force]` | §7 |
| `build [--with-tests]` | Shaded JARs via `maven:3.9-eclipse-temurin-21` (tests skipped unless asked; the container-backed suites are never run here); builds the Zeek image |
| `up` | Kafka + ClickHouse → topics → DDL → Flink → submit/resume jobs → Zeek; waits for each to be healthy; ends with `status` |
| `down` | Stop both jobs with a savepoint, then stop containers. Data kept |
| `restart` | `down` + `up`. Upgrading is `build` + `restart` |
| `status` | Container health; Flink job states and last-checkpoint age; message counts per raw topic; ClickHouse rows per `log_type` in the last 5 minutes; DLQ counts and top reasons |
| `logs [service]` | Follow logs |
| `sql "<query>"` | Run a ClickHouse query |
| `selftest` | Publishes one Modbus and one S7 request/response pair — in exactly the JSON Zeek writes, with `192.0.2.x` (documentation-only) addresses and `SELFTEST-` uids — waits for their vectors in ClickHouse, reports pass/fail, then deletes its rows |
| `uninstall [--purge]` | Remove containers and the built images; `--purge` also deletes all data, after a typed confirmation |

`deploy.sh` is `set -euo pipefail`, prints what it is about to do, never uses `sudo`
except to install Docker, and never touches containers outside the `netsec-ml` project.

## 9. Security posture (MVP)

- Every port bound to `127.0.0.1`; reaching the UI remotely is an SSH tunnel away. Binding
  wider is an explicit `.env` change.
- ClickHouse has a generated password; `deploy/.env` is mode 600 and git-ignored.
- Kafka is PLAINTEXT, reachable only on localhost and the compose network.
- Zeek has only the two capabilities it needs to sniff.

## 10. Verification (on the development machine, before handover)

1. **Shaded JARs** — a test per bootstrap module asserting its JAR contains the job's
   main class, the Kafka connector, Jackson and the ClickHouse client, and contains **no**
   `org.apache.flink.streaming.api.environment` classes.
2. **Full lifecycle, for real** — `install` (Docker already present) → `tune` → `build`
   → `up` → `selftest` passes → `down` (savepoints written) → `up` (both jobs restored
   from those savepoints, confirmed via the Flink REST API) → `status`. This machine has
   5.7 GiB, so it also establishes the real minimum.
3. **Real Zeek parity** — build the Zeek image and run it over ICSNPP's own published
   Modbus and S7comm sample traces (downloaded at test time, never committed, and never
   read by the platform itself); consume what it wrote to Kafka and require **every**
   record to pass our parser and mapper with zero DLQ. Any rejection or field-meaning
   mismatch stops the work and is reported, not patched — our parsers mirror the frozen
   models' training input and are not changed to fit a sensor.
4. `shellcheck` clean on every script; `docker compose config` valid.

**Not verifiable here:** the server's interface and traffic, and its hardware — `doctor`,
`tune` and `selftest` on the server cover those.

## 11. Known limits (accepted for the MVP)

- Single Kafka broker, single ClickHouse node, one Flink TaskManager: no high
  availability. A restart resumes from the newest checkpoint.
- If no checkpoint survives (e.g. `uninstall --purge` of Flink data only), the online job
  starts at `earliest()` and replays the raw topics' 7-day retention; ClickHouse's
  `ReplacingMergeTree` absorbs the duplicate feature rows.
- Our Zeek and the server's existing Zeek may capture the same interface: twice the
  capture CPU.
- One partition per raw topic caps the online job's read throughput for each protocol at
  one subtask.
- The s7comm idle TTL is still the 1-hour default (`S7COMM_STATE_TTL_MINUTES`), with the
  outage caveat recorded in CLAUDE.md.

## 12. Out of scope

Scoring and predictions (until the user reports the models complete — D5); conn and dns
ingestion (their topics exist, empty); Kafka TLS/SASL; multi-node or HA; dashboards;
the air-gapped offline bundle (not needed: this server has internet).
