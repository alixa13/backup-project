# Server Deployment (Modbus + S7comm MVP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One script, `deploy/deploy.sh`, that installs, sizes, builds, runs and checks the Modbus + S7comm feature pipeline on the user's server, as its own Docker Compose stack (Zeek → Kafka → Flink → ClickHouse).

**Architecture:** A `deploy/` folder holds a Compose project (`netsec-ml`: KRaft Kafka, ClickHouse, a Flink 2.2.1 session cluster, a job-supervisor container, and a pinned Zeek sensor) plus `deploy.sh` and its `lib/*.sh` modules. Both job modules gain a shaded "all" JAR. A new `ZeekRecordCheck` CLI in `bootstrap-online-job` pushes real Zeek output through the production parsers; it backs `deploy.sh zeek-check` on the server and a JUnit test pinned to real ICSNPP output here. No job code changes.

**Tech Stack:** Bash 5, Docker Compose v2, `flink:2.2.1-java21`, `confluentinc/cp-kafka:7.6.1` (KRaft), `clickhouse/clickhouse-server:25.8`, `zeek/zeek:7.0.9` + icsnpp-modbus v1.0.0 + icsnpp-s7comm `7ebeb03` + zeek-kafka v1.2.0, `maven-shade-plugin` 3.6.2, JUnit 5, `jq`, `koalaman/shellcheck:v0.10.0`.

**Spec:** `docs/superpowers/specs/2026-09-24-server-deployment-design.md` (commit `dffcd8c`).

## Global Constraints

- **Never start the stack on the development machine** (user, 2026-09-24: "dont run the project here bc there is no enogh hardware"). Allowed here: Maven builds, unit tests, the bash tests, `docker compose config` (static), `docker build`, and seconds-long single-container `docker run`s (`zeek -r` on a sample trace, `java` for `ZeekRecordCheck`, `shellcheck`, `--help`). Forbidden here: `deploy.sh up`/`selftest`/`zeek-check --live`, `docker compose up`, and the Testcontainers suites.
- Never act on a container outside Compose project `netsec-ml`. Every compose call goes through `compose()` (`-p netsec-ml -f deploy/docker-compose.yml --env-file deploy/.env`).
- Stage explicit paths only. Never stage `two-models-info/`, `new-models/`, `modbus_rf_attack_type_v1/`, `PROJECT_OVERVIEW.md`, `.claude/`, `graphify-out/`.
- Pins, verbatim: `flink:2.2.1-java21`, `confluentinc/cp-kafka:7.6.1`, `clickhouse/clickhouse-server:25.8`, `zeek/zeek:7.0.9`, icsnpp-modbus `v1.0.0`, icsnpp-s7comm `7ebeb03a0f954541369361651d1c27d09a64b5a3`, zeek-kafka `v1.2.0`, `maven:3.9.9-eclipse-temurin-21`, `koalaman/shellcheck:v0.10.0`, `maven-shade-plugin` `3.6.2`.
- Ports: Flink UI `18081` and ClickHouse HTTP `18123` bound to `BIND_ADDRESS` (default `127.0.0.1`); Kafka host listener `19092` always bound to `127.0.0.1`.
- Data under `deploy/data/` (`NETSEC_DATA_DIR`), owned by uid 1000 (Kafka), 101 (ClickHouse), 9999 (Flink).
- The parsers and mappers are never changed to fit a sensor's output: they mirror the frozen models' training input (spec §10).
- Every code block carries inline comments describing what it does (the user's standing preference).
- Java: no change to any job's behaviour. The only Java added is `ZeekRecordCheck` and its test.
- Run Maven per CLAUDE.md: `./mvnw install -DskipTests` first; bootstrap modules only filtered to one class; never `-Dtest` with `-am`; confirm the real `Tests run:` count.

## Rulings (plan-time deviations from the spec, each argued)

- **P1 (spec §10.2):** the full lifecycle is **not** run on this machine (user instruction above). It is run by the user on the server: `up`, `selftest`, `zeek-check`, `zeek-check --live`, `down`, `up`. Here: builds, unit tests, static checks, and the offline Zeek proofs. Cost if wrong: a stack-level defect surfaces on the server, not here.
- **P2 (spec §10.3 "zero DLQ"):** real ICSNPP output gives 3 Modbus rejections of 48, all the F4 class (CLAUDE.md "Modbus limits"): Zeek writes function 43 as `ENCAP_INTERFACE_TRANSPORT` while upstream's `FUNCTION_NAME_TO_CODE` says `ENCAPSULATED_INTERFACE_TRANSPORT`, and an `_EXCEPTION` PDU. Upstream's engine rejects them too. `ZeekRecordCheck` accepts exactly that class ("func did not resolve …" on Modbus) and fails on anything else. All 84 S7 records pass. Measured at plan time with the production classes.
- **P3 (spec §3 `init` service):** topics are created by `deploy.sh up` via `docker compose exec kafka`, and the DDL by the existing `scripts/database/apply-ddl.sh` from the host — no extra one-shot containers.
- **P4 (spec §7 JobManager 768–1024 MiB):** fixed at 1024 MiB (at 768 its heap is ~128 MiB). The floors then sum to 4224 MiB, so the minimum budget is **4352 MiB**, not "~4 GiB".
- **P5 (spec §7):** each Flink process size is its container allocation minus 64 MiB (headroom for native allocations outside Flink's model).
- **P6 (spec §3):** the Kafka host port is always loopback-bound: it advertises `127.0.0.1`, which is where Zeek (host network) reaches it. `BIND_ADDRESS` moves only the Flink UI and ClickHouse.
- **P7 (spec §4 "no local log files"):** `Log::default_writer = Log::WRITER_NONE` **and** removal of every default filter. Plan-time run: removal alone still wrote `packet_filter.log` (logged before the removal runs); with both, zero files.
- **P8 (additions):** `deploy.sh zeek-check [--live N]` + the `ZeekRecordCheck` CLI (the server-side form of spec §10.3), and `build --jars-only`.
- **P9 (spec §3 job-submitter "one-shot"):** a long-running supervisor (`restart: unless-stopped`, checks every 60 s). A one-shot container that exited 0 is not restarted after a reboot, so the jobs would not come back; a JobManager restart (no HA) also loses them.
- **P10 (spec silent):** the JobManager's CPU limit is a fixed `1.00`.

## Plan-time evidence (so no task has to rediscover it)

- `zeek/zeek:7.0.9` (Debian 12) builds all three pinned packages (~6.5 min). Module and stream names: `Modbus_Extended::LOG_DETAILED`, `S7COMM::LOG_S7COMM`. zeek-kafka options: `Kafka::topic_name`, `tag_json`, `json_timestamps`, `kafka_conf`.
- zeek-kafka formats each message with Zeek's own `threading::formatter::JSON` (the ASCII writer's JSON formatter), so `LogAscii::use_json` output with `JSON::TS_EPOCH` is byte-for-byte the Kafka message body — the offline checks are faithful.
- Offline run of the production policy against an unreachable broker: `modbus_detailed/Log::WRITER_KAFKAWRITER: Unable to deliver 48 message(s)` (modbus_example.pcap) and `s7comm/…: Unable to deliver 62 message(s)` (snap7.pcap) — every record reaches the Kafka writer.
- `flink-dist-2.2.1.jar` ships: Flink runtime/streaming/clients, `flink-connector-base`, `flink-connector-datagen`, Kryo, objenesis, commons-lang3/io/text/math3/compress/collections/cli, javassist, snakeyaml-engine, slf4j, async-profiler, jsr305, lz4 (`net.jpountz`), snappy. It does **not** ship: kafka-clients, zstd-jni, Jackson (unrelocated), `flink-connector-kafka`, ClickHouse client-v2, httpclient5, guava, RoaringBitmap.
- Flink 2.2.1 config keys: `execution.checkpointing.dir`, `execution.checkpointing.savepoint-dir`, `execution.checkpointing.externalized-checkpoint-retention` (`RETAIN_ON_CANCELLATION`), `execution.checkpointing.num-retained`, `taskmanager.memory.process.size`, `jobmanager.memory.process.size`, `taskmanager.numberOfTaskSlots`, `taskmanager.memory.managed.fraction`, `parallelism.default`, `rest.address`, `rest.port`. `flink run` accepts `-D key=value`, `-s`, `-d`, `-c`.
- The Flink image's entrypoint applies `JOB_MANAGER_RPC_ADDRESS` and `FLINK_PROPERTIES` to **every** command (so the supervisor container's CLI is configured the same way), runs as `flink` (uid 9999), binds `0.0.0.0`, and has `curl`.
- `cp-kafka:7.6.1` has `kafka-topics`, `kafka-get-offsets`, `kafka-console-producer/-consumer`; runs as `appuser` (1000). `clickhouse-server:25.8` has `wget`; runs as `clickhouse` (101); `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`/`CLICKHOUSE_DB` create the user and database.
- ICSNPP sample traces (SHA-256): `modbus_example.pcap` `a84656f9af62b2c948200ec288d51b81f03037c277a31a40efee0cfb244f1e30` (icsnpp-modbus v1.0.0 `tests/traces/`), `snap7.pcap` `2b91f6a8a203ec83e4f2dbb69d5e61602845f23da0baa31d95d029fb24cbd427` and `s7ident.pcap` `7a2ae7f2992669a3eb36a878f0e4f124c86ecdf3fc3a284f78e2e6e1deeea680` (icsnpp-s7comm `7ebeb03` `testing/traces/`). Zeek writes 48 `modbus_detailed` records for the first and 62 + 22 `s7comm` records for the other two.

## Review Focus

1. **A restore point that no longer fits the job** (e.g. after `build` + `restart` with changed state) — the supervisor must keep running and log, on every failed submission, how to start that job fresh. Pinned in Task 6 (`a failed submission logs the way out`).
2. **`ZEEK_INTERFACE` unset or wrong at `up`** — `up` must refuse before starting anything and list the host's interfaces, not leave Zeek crash-looping. Pinned in Task 8 (`preflight refuses an unknown interface`).
3. **`docker compose exec` inside a `while read` loop swallows the loop's stdin** — only the first topic would be created and both jobs would crash-loop on the missing ones. Pinned in Task 8 (`create_topics creates every topic`).
4. **A published port already held by another process** — `doctor` must name it. Pinned in Task 7 (`port_in_use` + doctor's FAIL line).
5. **`docker stats` with no container ids lists every container on the host** — `tune` would count the other project's memory as ours and oversize the stack. Pinned in Task 4 (`own_usage_mib is 0 with no netsec-ml containers`).

---

## File Structure

```
pom.xml                                         modify: shade plugin version in pluginManagement
modules/bootstrap-online-job/pom.xml            modify: shade execution ("all" JAR)
modules/bootstrap-archive-job/pom.xml           modify: shade execution ("all" JAR)
modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/ZeekRecordCheck.java   create
modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/ZeekRecordCheckTest.java  create
tests/fixtures/zeek/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl   create (generated, 48 lines)
tests/fixtures/zeek/icsnpp-s7comm-7ebeb03_s7comm.jsonl           create (generated, 84 lines)
tests/fixtures/zeek/README.md                                     create
.gitignore                                      modify: deploy/ per-host state
deploy/deploy.sh                                create: the CLI dispatcher
deploy/.env.template                            create: every setting, with defaults
deploy/.shellcheckrc                            create
deploy/README.md                                create: operator guide
deploy/docker-compose.yml                       create
deploy/lib/common.sh                            create: paths, logging, env, compose, REST/SQL clients
deploy/lib/tune.sh                              create: resource sizing
deploy/lib/doctor.sh                            create
deploy/lib/install.sh                           create: install + uninstall
deploy/lib/build.sh                             create
deploy/lib/stack.sh                             create: up/down/status
deploy/lib/traces.sh                            create: pinned ICSNPP traces + offline Zeek runs
deploy/lib/selftest.sh                          create: selftest + zeek-check
deploy/kafka/topics.conf                        create
deploy/clickhouse/config.d/netsec.xml           create
deploy/flink/submit-jobs.sh                     create: the job supervisor
deploy/zeek/Dockerfile                          create
deploy/zeek/netsec.zeek                         create: production sensor policy
deploy/zeek/offline-json.zeek                   create: check policy (JSON files)
deploy/zeek/run-zeek.sh                         create: image entrypoint
deploy/selftest/modbus.jsonl.template           create
deploy/selftest/s7comm.jsonl.template           create
deploy/tests/lib.sh                             create: assertions
deploy/tests/check-jars.sh                      create
deploy/tests/zeek-fixtures.sh                   create
deploy/tests/test_zeek_policy.sh                create
deploy/tests/test_common.sh                     create
deploy/tests/test_tune.sh                       create
deploy/tests/test_compose.sh                    create
deploy/tests/test_submit_jobs.sh                create
deploy/tests/test_install_doctor.sh             create
deploy/tests/test_stack.sh                      create
deploy/tests/test_selftest.sh                   create
deploy/tests/run-all.sh                         create
CLAUDE.md                                       modify
```

---

### Task 1: Shaded job JARs

**Files:**
- Create: `deploy/tests/lib.sh`, `deploy/tests/check-jars.sh`
- Modify: `pom.xml` (properties, pluginManagement), `modules/bootstrap-online-job/pom.xml`, `modules/bootstrap-archive-job/pom.xml`, `.gitignore`

**Interfaces:**
- Produces: `modules/bootstrap-online-job/target/bootstrap-online-job-0.1.0-SNAPSHOT-all.jar` and `modules/bootstrap-archive-job/target/bootstrap-archive-job-0.1.0-SNAPSHOT-all.jar`, each with a `Main-Class`. `deploy/tests/lib.sh`: `assert_eq EXPECTED ACTUAL MESSAGE`, `finish`.

- [ ] **Step 1: Write the assertion helpers**

`deploy/tests/lib.sh`:

```bash
#!/usr/bin/env bash
# Minimal assertions for the deploy/ bash tests (no bats on the target hosts).
# Each test file sources this, calls assert_eq, and ends with `finish`.

TESTS_RUN=0
TESTS_FAILED=0

# assert_eq EXPECTED ACTUAL MESSAGE -- record one check; print it only on failure.
assert_eq() {
  TESTS_RUN=$((TESTS_RUN + 1))
  if [ "$1" != "$2" ]; then
    TESTS_FAILED=$((TESTS_FAILED + 1))
    printf 'FAIL: %s\n  expected: %s\n  actual:   %s\n' "$3" "$1" "$2"
  fi
}

# Print the file's tally; the exit status is non-zero if any check failed.
finish() {
  printf '%s: %d checks, %d failed\n' "$(basename "$0")" "$TESTS_RUN" "$TESTS_FAILED"
  [ "$TESTS_FAILED" -eq 0 ]
}
```

- [ ] **Step 2: Write the failing JAR test**

`deploy/tests/check-jars.sh`:

```bash
#!/usr/bin/env bash
# Asserts each shaded job JAR (design §6) carries what the Flink 2.2.1 image
# lacks -- the Kafka connector and client, zstd, Jackson, our own modules, and
# for the archive job the ClickHouse client -- and none of what flink-dist
# already ships: two copies of Flink's runtime, Kryo or commons-* on one
# classpath break class loading. Run after './mvnw install -DskipTests'.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
REPO="$(cd "$HERE/../.." && pwd)"

# yes/no: does the JAR listing in $1 contain the entry $2?
has() { grep -qxF "$2" <<< "$1" && echo yes || echo no; }

# Classes flink-dist-2.2.1.jar already ships (checked against the image).
PROVIDED=(
  org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.class
  org/apache/flink/runtime/jobgraph/JobGraph.class
  org/apache/flink/client/cli/CliFrontend.class
  org/apache/flink/connector/base/DeliveryGuarantee.class
  com/esotericsoftware/kryo/Kryo.class
  org/objenesis/Objenesis.class
  org/apache/commons/lang3/StringUtils.class
  net/jpountz/lz4/LZ4Factory.class
  org/xerial/snappy/Snappy.class
  org/slf4j/Logger.class
)

# Needed by both jobs at run time and absent from the Flink image.
NEEDED=(
  org/apache/flink/connector/kafka/source/KafkaSource.class
  org/apache/kafka/clients/producer/KafkaProducer.class
  com/github/luben/zstd/Zstd.class
  com/fasterxml/jackson/databind/ObjectMapper.class
  io/netsecml/platform/domain/event/S7commEvent.class
)

for job in online archive; do
  # The shaded JAR carries the classifier "all".
  jar="$(ls "$REPO"/modules/bootstrap-${job}-job/target/bootstrap-${job}-job-*-all.jar 2>/dev/null | head -n 1)"
  assert_eq yes "$([ -n "$jar" ] && [ -f "$jar" ] && echo yes || echo no)" "${job}: shaded JAR exists"
  [ -n "$jar" ] || continue
  entries="$(unzip -Z1 "$jar")"

  # What the image lacks must be inside; what it ships must not be.
  for c in "${NEEDED[@]}"; do assert_eq yes "$(has "$entries" "$c")" "${job} JAR contains ${c}"; done
  for c in "${PROVIDED[@]}"; do assert_eq no "$(has "$entries" "$c")" "${job} JAR lacks ${c}"; done

  # The manifest names the job, so the submitter's -c is a belt, not a brace.
  main="$(unzip -p "$jar" META-INF/MANIFEST.MF | sed -n 's/^Main-Class: *//p' | tr -d '\r')"
  case "$job" in
    online)
      assert_eq io.netsecml.platform.bootstrap.online.OnlineFeatureJob "$main" "online Main-Class"
      assert_eq yes "$(has "$entries" io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParser.class)" "online JAR contains the Modbus parser"
      ;;
    archive)
      assert_eq io.netsecml.platform.bootstrap.archive.ArchiveJob "$main" "archive Main-Class"
      assert_eq yes "$(has "$entries" com/clickhouse/client/api/Client.class)" "archive JAR contains the ClickHouse client"
      assert_eq yes "$(has "$entries" org/apache/hc/client5/http/impl/classic/CloseableHttpClient.class)" "archive JAR contains httpclient5"
      ;;
  esac
done

finish
```

- [ ] **Step 3: Run it to verify it fails**

Run: `bash deploy/tests/check-jars.sh`
Expected: FAIL — `online: shaded JAR exists` and `archive: shaded JAR exists` (expected `yes`, actual `no`); exit status 1.

- [ ] **Step 4: Add the shade plugin**

`pom.xml` — add to `<properties>` after `maven.surefire.plugin.version`:

```xml
    <maven.shade.plugin.version>3.6.2</maven.shade.plugin.version>
```

and to `<pluginManagement><plugins>` after the surefire entry:

```xml
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-shade-plugin</artifactId>
          <version>${maven.shade.plugin.version}</version>
        </plugin>
```

`modules/bootstrap-online-job/pom.xml` — add before `</project>`:

```xml
  <build>
    <plugins>
      <!-- The deployable JAR (deploy/, design §6): this job, our modules and the
           libraries the Flink 2.2.1 image lacks. Everything flink-dist already
           ships is excluded, because two copies on one classpath break class
           loading; deploy/tests/check-jars.sh pins both halves. Attached with
           the classifier "all", so the plain JAR is unchanged. -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <shadedArtifactAttached>true</shadedArtifactAttached>
              <shadedClassifierName>all</shadedClassifierName>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <artifactSet>
                <excludes>
                  <!-- Flink's own runtime: the image's flink-dist provides it. -->
                  <exclude>org.apache.flink:flink-annotations</exclude>
                  <exclude>org.apache.flink:flink-clients</exclude>
                  <exclude>org.apache.flink:flink-connector-base</exclude>
                  <exclude>org.apache.flink:flink-connector-datagen</exclude>
                  <exclude>org.apache.flink:flink-core</exclude>
                  <exclude>org.apache.flink:flink-core-api</exclude>
                  <exclude>org.apache.flink:flink-datastream</exclude>
                  <exclude>org.apache.flink:flink-datastream-api</exclude>
                  <exclude>org.apache.flink:flink-file-sink-common</exclude>
                  <exclude>org.apache.flink:flink-hadoop-fs</exclude>
                  <exclude>org.apache.flink:flink-metrics-core</exclude>
                  <exclude>org.apache.flink:flink-queryable-state-client-java</exclude>
                  <exclude>org.apache.flink:flink-rpc-akka-loader</exclude>
                  <exclude>org.apache.flink:flink-rpc-core</exclude>
                  <exclude>org.apache.flink:flink-runtime</exclude>
                  <exclude>org.apache.flink:flink-shaded-asm-9</exclude>
                  <exclude>org.apache.flink:flink-shaded-guava</exclude>
                  <exclude>org.apache.flink:flink-shaded-jackson</exclude>
                  <exclude>org.apache.flink:flink-shaded-netty</exclude>
                  <exclude>org.apache.flink:flink-shaded-zookeeper-3</exclude>
                  <exclude>org.apache.flink:flink-streaming-java</exclude>
                  <!-- Libraries flink-dist bundles unrelocated. -->
                  <exclude>com.esotericsoftware:*</exclude>
                  <exclude>org.objenesis:*</exclude>
                  <exclude>org.apache.commons:*</exclude>
                  <exclude>commons-cli:*</exclude>
                  <exclude>commons-collections:*</exclude>
                  <exclude>commons-io:*</exclude>
                  <exclude>org.javassist:*</exclude>
                  <exclude>org.snakeyaml:*</exclude>
                  <exclude>org.slf4j:*</exclude>
                  <exclude>tools.profiler:*</exclude>
                  <exclude>com.google.code.findbugs:*</exclude>
                  <exclude>org.lz4:*</exclude>
                  <exclude>at.yawk.lz4:*</exclude>
                  <exclude>org.xerial.snappy:*</exclude>
                </excludes>
              </artifactSet>
              <filters>
                <!-- Signatures of shaded JARs no longer match; module
                     descriptors do not apply inside an uber JAR. -->
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                    <exclude>module-info.class</exclude>
                  </excludes>
                </filter>
              </filters>
              <transformers>
                <!-- Kafka and Jackson register implementations via ServiceLoader. -->
                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>io.netsecml.platform.bootstrap.online.OnlineFeatureJob</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
```

`modules/bootstrap-archive-job/pom.xml` — the identical `<build>` block, with `<mainClass>io.netsecml.platform.bootstrap.archive.ArchiveJob</mainClass>`.

`.gitignore` — append:

```
# deploy/: per-host state, never committed
deploy/data/
deploy/jars/
deploy/.m2/
deploy/.cache/
```

(`deploy/.env` is already covered by the existing `.env` pattern.)

- [ ] **Step 5: Build and run the test**

Run: `./mvnw -q install -DskipTests && bash deploy/tests/check-jars.sh`
Expected: `check-jars.sh: 37 checks, 0 failed`, exit 0 (per JAR: 1 exists + 5 needed + 10 provided + 1 Main-Class; plus 1 online-only and 2 archive-only).

- [ ] **Step 6: Confirm the bootstrap topology tests still pass with the new build section**

Run: `./mvnw -o test -pl modules/bootstrap-online-job -Dtest=OnlineFeatureJobTopologyTest 2>&1 | grep "Tests run:" | tail -1` and the same for `modules/bootstrap-archive-job -Dtest=ArchiveJobTopologyTest`
Expected: `Tests run: 8, Failures: 0, Errors: 0, Skipped: 0` and `Tests run: 10, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 7: Commit**

```bash
git add pom.xml modules/bootstrap-online-job/pom.xml modules/bootstrap-archive-job/pom.xml .gitignore deploy/tests/lib.sh deploy/tests/check-jars.sh
git commit -m "build(deploy): shade both jobs into self-contained JARs for the Flink 2.2.1 image"
```

---

### Task 2: The Zeek sensor image and the real-output fixtures

**Files:**
- Create: `deploy/zeek/Dockerfile`, `deploy/zeek/netsec.zeek`, `deploy/zeek/offline-json.zeek`, `deploy/zeek/run-zeek.sh`, `deploy/lib/common.sh` (first version — completed in Task 4), `deploy/lib/traces.sh`, `deploy/tests/test_zeek_policy.sh`, `deploy/tests/zeek-fixtures.sh`, `tests/fixtures/zeek/README.md`, `tests/fixtures/zeek/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl`, `tests/fixtures/zeek/icsnpp-s7comm-7ebeb03_s7comm.jsonl`

**Interfaces:**
- Produces: image `netsec-ml/zeek:1` (entrypoint `/opt/netsec/run-zeek.sh`; env `NETSEC_KAFKA_BROKERS`, `ZEEK_INTERFACE` or `ZEEK_READ_FILE`, `MODBUS_RAW_TOPIC`, `S7COMM_RAW_TOPIC`; `/opt/netsec/offline-json.zeek` for checks). `lib/traces.sh`: `fetch_traces DIR`, `zeek_offline_records IMAGE TRACES_DIR OUT_DIR` → writes `OUT_DIR/modbus_detailed.jsonl` and `OUT_DIR/s7comm.jsonl`. The two fixture files (48 and 84 lines).

- [ ] **Step 1: Write the Zeek policy, the check policy and the entrypoint**

`deploy/zeek/netsec.zeek`:

```zeek
##! netsec-ml sensor policy (design §4): ICSNPP modbus_detailed and s7comm
##! records to their Kafka topics as bare JSON objects; nothing to local disk.

@load packages

module NetSec;

export {
    ## Kafka topic for ICSNPP modbus_detailed records (one per packet).
    const modbus_topic = "netsec.modbus.raw.v1" &redef;
    ## Kafka topic for ICSNPP s7comm records (one per packet).
    const s7comm_topic = "netsec.s7comm.raw.v1" &redef;
}

# The connection tuple is written id_orig_h, not id.orig_h: the spelling this
# platform's sensor has always used (both parsers also accept the dotted form).
redef Log::default_scope_sep = "_";

# No local log files: every stream's default filter writes nowhere from the
# first record on (this also catches packet_filter.log, which is written
# before any zeek_init handler below can run).
redef Log::default_writer = Log::WRITER_NONE;

# Each Kafka message is the bare record with epoch-second timestamps -- no
# log-name wrapper, which the parsers would reject.
redef Kafka::topic_name = "";
redef Kafka::tag_json = F;
redef Kafka::json_timestamps = JSON::TS_EPOCH;

event zeek_init() &priority=-10
    {
    # Only the two OT logs go to Kafka, each to its own topic.
    Log::add_filter(Modbus_Extended::LOG_DETAILED, [$name="netsec-kafka-modbus",
        $writer=Log::WRITER_KAFKAWRITER, $path="modbus_detailed",
        $config=table(["topic_name"] = modbus_topic)]);
    Log::add_filter(S7COMM::LOG_S7COMM, [$name="netsec-kafka-s7comm",
        $writer=Log::WRITER_KAFKAWRITER, $path="s7comm",
        $config=table(["topic_name"] = s7comm_topic)]);

    # The default filters now only feed the no-op writer; dropping them saves
    # formatting every other log's records for nothing.
    for ( id in Log::active_streams )
        Log::remove_filter(id, "default");
    }
```

`deploy/zeek/offline-json.zeek`:

```zeek
##! Check policy: the same packages and field naming as netsec.zeek, but JSON
##! log files in the working directory instead of Kafka. Used only by
##! deploy.sh zeek-check and deploy/tests/zeek-fixtures.sh. zeek-kafka formats
##! its messages with this same JSON formatter, so these files are exactly
##! what the sensor sends.

@load packages

redef Log::default_scope_sep = "_";
redef LogAscii::use_json = T;
redef LogAscii::json_timestamps = JSON::TS_EPOCH;
```

`deploy/zeek/run-zeek.sh`:

```bash
#!/usr/bin/env bash
# Entrypoint of the netsec-ml Zeek image. Writes the Kafka settings this
# container was given into a Zeek script, then runs Zeek with the sensor
# policy: live on ZEEK_INTERFACE, or over ZEEK_READ_FILE (a pcap; checks only).
set -euo pipefail

: "${NETSEC_KAFKA_BROKERS:?NETSEC_KAFKA_BROKERS must be host:port[,host:port]}"
modbus_topic="${MODBUS_RAW_TOPIC:-netsec.modbus.raw.v1}"
s7comm_topic="${S7COMM_RAW_TOPIC:-netsec.s7comm.raw.v1}"

# The values land inside Zeek string literals: allow only what a broker list
# and a Kafka topic name can contain, so none can break out of the quotes.
if ! [[ "$NETSEC_KAFKA_BROKERS" =~ ^[A-Za-z0-9.:,_-]+$ ]]; then
  echo "invalid NETSEC_KAFKA_BROKERS: ${NETSEC_KAFKA_BROKERS}" >&2
  exit 2
fi
for topic in "$modbus_topic" "$s7comm_topic"; do
  if ! [[ "$topic" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "invalid topic name: ${topic}" >&2
    exit 2
  fi
done

# The per-container settings, as Zeek redefs loaded after the policy.
env_script=/tmp/netsec-env.zeek
cat > "$env_script" <<EOF
redef Kafka::kafka_conf = table(["metadata.broker.list"] = "${NETSEC_KAFKA_BROKERS}", ["client.id"] = "netsec-zeek");
redef NetSec::modbus_topic = "${modbus_topic}";
redef NetSec::s7comm_topic = "${s7comm_topic}";
EOF

# Zeek writes no log files (the policy sends every stream to Kafka or
# nowhere); /tmp keeps any stray state out of the image's own directories.
cd /tmp
if [ -n "${ZEEK_READ_FILE:-}" ]; then
  exec zeek -C -r "$ZEEK_READ_FILE" /opt/netsec/netsec.zeek "$env_script"
fi
: "${ZEEK_INTERFACE:?ZEEK_INTERFACE must name the capture interface}"
exec zeek -C -i "$ZEEK_INTERFACE" /opt/netsec/netsec.zeek "$env_script"
```

- [ ] **Step 2: Write the Dockerfile**

`deploy/zeek/Dockerfile`:

```dockerfile
# netsec-ml Zeek sensor (design §4): Zeek 7.0 LTS plus exactly the three
# packages the platform's parsers were verified against. Every version is
# pinned, deliberately: icsnpp-modbus's current release (v2.0.0) writes
# modbus_detailed as ONE record per request/response pair, which neither the
# frozen Modbus model nor JsonZeekModbusParser can read; v1.0.0 writes one
# record per packet.
ARG ZEEK_BASE_IMAGE=zeek/zeek:7.0.9

# --- build stage: compile the two C++ plugins and install all three packages ---
FROM ${ZEEK_BASE_IMAGE} AS build
ARG ICSNPP_MODBUS_VERSION=v1.0.0
ARG ICSNPP_S7COMM_VERSION=7ebeb03a0f954541369361651d1c27d09a64b5a3
ARG ZEEK_KAFKA_VERSION=v1.2.0
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential cmake git librdkafka-dev libpcap-dev libssl-dev zlib1g-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN zkg autoconfig --force \
 && zkg install --force --skiptests https://github.com/cisagov/icsnpp-modbus --version "${ICSNPP_MODBUS_VERSION}" \
 && zkg install --force --skiptests https://github.com/cisagov/icsnpp-s7comm --version "${ICSNPP_S7COMM_VERSION}" \
 && zkg install --force --skiptests --user-var LIBRDKAFKA_ROOT=/usr \
      https://github.com/SeisoLLC/zeek-kafka --version "${ZEEK_KAFKA_VERSION}" \
 && zkg list

# --- runtime stage: the same Zeek, the installed packages, librdkafka only ---
FROM ${ZEEK_BASE_IMAGE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends librdkafka1 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/zeek /usr/local/zeek
COPY netsec.zeek offline-json.zeek /opt/netsec/
COPY --chmod=0755 run-zeek.sh /opt/netsec/run-zeek.sh
ENTRYPOINT ["/opt/netsec/run-zeek.sh"]
```

- [ ] **Step 3: Write the trace helpers (and the start of common.sh they need)**

`deploy/lib/common.sh` (first version; Task 4 appends the rest):

```bash
#!/usr/bin/env bash
# Shared helpers for deploy.sh: paths, logging, the deploy/.env loader and
# editor, the compose wrapper, polling, and the ClickHouse and Flink REST
# clients. Sourced by deploy.sh and the tests, never executed on its own.

# Absolute paths to deploy/ and the repository root, from this file's location,
# so every command works from any working directory.
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${DEPLOY_DIR}/.." && pwd)"
ENV_FILE="${ENV_FILE:-${DEPLOY_DIR}/.env}"
COMPOSE_PROJECT="netsec-ml"

# Logging: stdout for progress, stderr for warnings and errors.
log()  { printf '[netsec-ml] %s\n' "$*"; }
warn() { printf '[netsec-ml] WARNING: %s\n' "$*" >&2; }
die()  { printf '[netsec-ml] ERROR: %s\n' "$*" >&2; exit 1; }
```

`deploy/lib/traces.sh`:

```bash
#!/usr/bin/env bash
# ICSNPP's own published sample traces, pinned by URL and SHA-256, and the
# offline Zeek run over them. Input for the Zeek checks only
# (tests/zeek-fixtures.sh, deploy.sh zeek-check): the platform never reads a pcap.

# name  url  sha256
TRACE_PINS=(
  "modbus_example.pcap https://raw.githubusercontent.com/cisagov/icsnpp-modbus/v1.0.0/tests/traces/modbus_example.pcap a84656f9af62b2c948200ec288d51b81f03037c277a31a40efee0cfb244f1e30"
  "snap7.pcap https://raw.githubusercontent.com/cisagov/icsnpp-s7comm/7ebeb03a0f954541369361651d1c27d09a64b5a3/testing/traces/snap7.pcap 2b91f6a8a203ec83e4f2dbb69d5e61602845f23da0baa31d95d029fb24cbd427"
  "s7ident.pcap https://raw.githubusercontent.com/cisagov/icsnpp-s7comm/7ebeb03a0f954541369361651d1c27d09a64b5a3/testing/traces/s7ident.pcap 7a2ae7f2992669a3eb36a878f0e4f124c86ecdf3fc3a284f78e2e6e1deeea680"
)

# Download each pinned trace into DIR (once) and verify its SHA-256.
fetch_traces() {
  local dir="$1" pin name url sha
  mkdir -p "$dir"
  for pin in "${TRACE_PINS[@]}"; do
    read -r name url sha <<< "$pin"
    [ -f "${dir}/${name}" ] || curl -fsSL -o "${dir}/${name}" "$url"
    printf '%s  %s\n' "$sha" "${dir}/${name}" | sha256sum -c --quiet - \
      || die "${name} does not match its pinned SHA-256; delete ${dir} and retry"
  done
}

# Run IMAGE's Zeek offline (check policy, JSON files) over the traces in
# TRACES_DIR, then gather OUT_DIR/modbus_detailed.jsonl (from modbus_example)
# and OUT_DIR/s7comm.jsonl (snap7, then s7ident). Runs as the invoking user so
# the output stays theirs.
zeek_offline_records() {
  local image="$1" traces="$2" out="$3" trace
  for trace in modbus_example snap7 s7ident; do
    mkdir -p "${out}/runs/${trace}"
    docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint zeek \
      -v "${traces}:/traces:ro" -v "${out}/runs/${trace}:/work" -w /work \
      "$image" -C -r "/traces/${trace}.pcap" /opt/netsec/offline-json.zeek
  done
  cat "${out}/runs/modbus_example/modbus_detailed.log" > "${out}/modbus_detailed.jsonl"
  cat "${out}/runs/snap7/s7comm.log" "${out}/runs/s7ident/s7comm.log" > "${out}/s7comm.jsonl"
}
```

- [ ] **Step 4: Write the failing policy test**

`deploy/tests/test_zeek_policy.sh`:

```bash
#!/usr/bin/env bash
# The production sensor policy, run offline (seconds; no stack) against an
# unreachable broker: every Modbus/S7 record must reach the Kafka writer, and
# nothing may be written to local disk. Also pins run-zeek.sh's input checks.
# usage: deploy/tests/test_zeek_policy.sh [IMAGE]   (default netsec-ml/zeek:1)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/traces.sh"
IMAGE="${1:-netsec-ml/zeek:1}"
TRACES="${DEPLOY_DIR}/.cache/traces"
fetch_traces "$TRACES"

# trace, records the Kafka writer must receive, log path in Zeek's message.
for case in "modbus_example 48 modbus_detailed" "snap7 62 s7comm"; do
  read -r trace n path <<< "$case"
  out="$(mktemp -d)"
  # Port 9 on loopback with no network: the broker is unreachable, so the
  # writer reports exactly how many records it was handed.
  output="$(docker run --rm --network none --user "$(id -u):$(id -g)" \
    -e ZEEK_READ_FILE="/traces/${trace}.pcap" -e NETSEC_KAFKA_BROKERS=127.0.0.1:9 \
    -v "${TRACES}:/traces:ro" -v "${out}:/tmp" "$IMAGE" 2>&1)"
  assert_eq 1 "$(grep -cF "${path}/Log::WRITER_KAFKAWRITER: Unable to deliver ${n} message(s)" <<< "$output")" \
    "${trace}: all ${n} records reach the Kafka writer"
  assert_eq 0 "$(find "$out" -name '*.log' | wc -l)" "${trace}: no local log files"
  rm -rf "$out"
done

# A broker list that could break out of the Zeek string literal is refused.
bad="$(docker run --rm --network none -e 'NETSEC_KAFKA_BROKERS=x";evil' -e ZEEK_READ_FILE=/dev/null "$IMAGE" 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'invalid NETSEC_KAFKA_BROKERS' <<< "$bad")" "unsafe broker list refused"
assert_eq 1 "$(grep -c 'exit=2' <<< "$bad")" "unsafe broker list exits 2"

finish
```

- [ ] **Step 5: Run it to verify it fails**

Run: `bash deploy/tests/test_zeek_policy.sh`
Expected: FAIL — `Unable to find image 'netsec-ml/zeek:1'` in the output, every assertion failing, exit 1.

- [ ] **Step 6: Build the image**

Run: `docker build -t netsec-ml/zeek:1 deploy/zeek 2>&1 | tail -5 && docker run --rm --entrypoint zkg netsec-ml/zeek:1 list`
Expected: build succeeds (~7 min the first time); `zkg list` shows `icsnpp-modbus (installed: v1.0.0)`, `icsnpp-s7comm (installed: 7ebeb03a0f954541369361651d1c27d09a64b5a3)` and `zeek-kafka (installed: v1.2.0)`.

- [ ] **Step 7: Run the policy test**

Run: `bash deploy/tests/test_zeek_policy.sh`
Expected: `test_zeek_policy.sh: 6 checks, 0 failed`.

- [ ] **Step 8: Write the fixture generator and generate the fixtures**

`deploy/tests/zeek-fixtures.sh`:

```bash
#!/usr/bin/env bash
# Regenerates tests/fixtures/zeek/ -- what the netsec-ml Zeek image writes for
# ICSNPP's own sample traces -- so ZeekRecordCheckTest pins the parsers
# against real sensor output. Zeek uids are random per run; everything else is
# stable. usage: deploy/tests/zeek-fixtures.sh [IMAGE]  (default netsec-ml/zeek:1)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/traces.sh"
image="${1:-netsec-ml/zeek:1}"

# Run Zeek offline over the pinned traces into a scratch directory.
work="$(mktemp -d)"
fetch_traces "${DEPLOY_DIR}/.cache/traces"
zeek_offline_records "$image" "${DEPLOY_DIR}/.cache/traces" "$work"

# Copy the two logs to their fixture names and show the line counts.
out="${REPO_ROOT}/tests/fixtures/zeek"
mkdir -p "$out"
cp "${work}/modbus_detailed.jsonl" "${out}/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl"
cp "${work}/s7comm.jsonl" "${out}/icsnpp-s7comm-7ebeb03_s7comm.jsonl"
rm -rf "$work"
wc -l "${out}"/*.jsonl
```

`tests/fixtures/zeek/README.md`:

```markdown
# Real Zeek output (ICSNPP)

What the netsec-ml Zeek image (`deploy/zeek/Dockerfile`: Zeek 7.0.9,
icsnpp-modbus v1.0.0, icsnpp-s7comm 7ebeb03) writes for ICSNPP's own published
sample traces, with the check policy `deploy/zeek/offline-json.zeek` — the same
JSON the sensor sends to Kafka.

| File | Trace | Records |
|---|---|---|
| `icsnpp-modbus-v1.0.0_modbus_detailed.jsonl` | icsnpp-modbus v1.0.0 `tests/traces/modbus_example.pcap` | 48 |
| `icsnpp-s7comm-7ebeb03_s7comm.jsonl` | icsnpp-s7comm 7ebeb03 `testing/traces/snap7.pcap`, then `s7ident.pcap` | 62 + 22 |

Regenerate with `deploy/tests/zeek-fixtures.sh` (uids change, nothing else).
`ZeekRecordCheckTest` reads them; the traces themselves are never committed.
```

Run: `bash deploy/tests/zeek-fixtures.sh`
Expected: `48 …modbus_detailed.jsonl`, `84 …s7comm.jsonl`, `132 total`; `head -1 tests/fixtures/zeek/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl` shows `"id_orig_h"`, `"is_orig":true`, `"request_response":"REQUEST"`, `"func":"READ_COILS"`.

- [ ] **Step 9: Remove the plan-time spike image**

Run: `docker image rm netsec-zeek-spike:1`
Expected: `Untagged: netsec-zeek-spike:1` (it was built during planning; `netsec-ml/zeek:1` replaces it).

- [ ] **Step 10: Commit**

```bash
git add deploy/zeek/Dockerfile deploy/zeek/netsec.zeek deploy/zeek/offline-json.zeek deploy/zeek/run-zeek.sh deploy/lib/common.sh deploy/lib/traces.sh deploy/tests/test_zeek_policy.sh deploy/tests/zeek-fixtures.sh tests/fixtures/zeek/README.md tests/fixtures/zeek/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl tests/fixtures/zeek/icsnpp-s7comm-7ebeb03_s7comm.jsonl
git commit -m "feat(deploy): pinned Zeek sensor image and real ICSNPP output fixtures"
```

---

### Task 3: `ZeekRecordCheck` — real sensor output through the production parsers

**Files:**
- Create: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/ZeekRecordCheck.java`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/ZeekRecordCheckTest.java`
- Modify: `deploy/tests/check-jars.sh` (one assertion)

**Interfaces:**
- Consumes: Task 2's fixtures; `JsonZeekModbusParser.parse(byte[]) -> MappingResult<ZeekModbusRecord>`, `ModbusEventMapper.map(ZeekModbusRecord, SensorId) -> MappingResult<NetworkEvent>`, and the S7comm equivalents; `MappingResult.isValid()/value()/reason()/detail()`.
- Produces: CLI `java -cp online-feature-job.jar io.netsecml.platform.bootstrap.online.ZeekRecordCheck (--modbus|--s7comm) FILE …`; exit 0 pass, 1 unexpected rejection, 2 bad arguments/unreadable file, 3 no records; last line `RESULT: PASS|FAIL|NO RECORDS`. Java: `ZeekRecordCheck.check(Protocol, List<String>) -> Report`, `Report.accepted()/rejections()/records()/passes()`, `Rejection.knownUpstreamParity()`, package-private `run(String[], PrintStream) -> int`.

- [ ] **Step 1: Write the failing test**

`ZeekRecordCheckTest.java`:

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.bootstrap.online.ZeekRecordCheck.Protocol;
import io.netsecml.platform.bootstrap.online.ZeekRecordCheck.Rejection;
import io.netsecml.platform.bootstrap.online.ZeekRecordCheck.Report;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

// Pins the parsers against REAL Zeek output. tests/fixtures/zeek/ holds what the
// netsec-ml Zeek image (Zeek 7.0.9, icsnpp-modbus v1.0.0, icsnpp-s7comm 7ebeb03)
// wrote for ICSNPP's own sample traces; every other parser test builds its
// records by hand, these were written by the sensor itself.
class ZeekRecordCheckTest {

    private static final Path FIXTURES = Path.of("..", "..", "tests", "fixtures", "zeek");
    private static final String MODBUS = "icsnpp-modbus-v1.0.0_modbus_detailed.jsonl";
    private static final String S7COMM = "icsnpp-s7comm-7ebeb03_s7comm.jsonl";
    private static final Pattern FUNC = Pattern.compile("\"func\":\"([^\"]+)\"");

    @Test
    void everyRealIcsnppS7commRecordIsAccepted() throws IOException {
        Report report = ZeekRecordCheck.check(Protocol.S7COMM, fixture(S7COMM));
        assertEquals(84, report.accepted());
        assertEquals(List.of(), report.rejections());
        assertTrue(report.passes());
    }

    // 45 of 48 are accepted. The other three are rejections upstream's own engine
    // makes: Zeek names function 43 ENCAP_INTERFACE_TRANSPORT where upstream's
    // table says ENCAPSULATED_INTERFACE_TRANSPORT, and an exception PDU's name
    // resolves to no function at all (CLAUDE.md, Modbus limits, F4).
    @Test
    void realIcsnppModbusRecordsAreAcceptedExceptUpstreamsOwnUnresolvableFunctionNames() throws IOException {
        Report report = ZeekRecordCheck.check(Protocol.MODBUS, fixture(MODBUS));
        assertEquals(45, report.accepted());
        assertEquals(List.of("ENCAP_INTERFACE_TRANSPORT", "ENCAP_INTERFACE_TRANSPORT",
                "READ_HOLDING_REGISTERS_EXCEPTION"),
            report.rejections().stream().map(r -> funcOf(r.line())).toList());
        assertTrue(report.rejections().stream().allMatch(Rejection::knownUpstreamParity));
        assertTrue(report.passes());
    }

    // icsnpp-modbus v2.0.0's shape -- one record per request/response pair, no
    // is_orig, no request_response -- must be rejected loudly, never misread.
    @Test
    void aRecordInIcsnppModbusV2ShapeIsAnUnexpectedRejection() {
        String v2 = "{\"ts\":1595660608.237928,\"uid\":\"CfkkU2CEsCsYzgPNf\",\"id_orig_h\":\"127.0.0.1\","
            + "\"id_orig_p\":47785,\"id_resp_h\":\"127.0.0.1\",\"id_resp_p\":502,\"tid\":1,\"unit\":4,"
            + "\"func\":\"READ_COILS\",\"address\":1,\"quantity\":1,"
            + "\"response_values\":[1,0,0,0,0,0,0,0],\"matched\":true}";
        Report report = ZeekRecordCheck.check(Protocol.MODBUS, List.of(v2));
        assertEquals(0, report.accepted());
        assertEquals(1, report.rejections().size());
        assertFalse(report.rejections().get(0).knownUpstreamParity());
        assertFalse(report.passes());
    }

    @Test
    void blankLinesAreNotRecords() {
        assertEquals(0, ZeekRecordCheck.check(Protocol.S7COMM, List.of("", "   ")).records());
    }

    @Test
    void runPassesOnTheFixturesAndSaysSo() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int status = ZeekRecordCheck.run(new String[] {
            "--modbus", FIXTURES.resolve(MODBUS).toString(),
            "--s7comm", FIXTURES.resolve(S7COMM).toString()}, print(buffer));
        String out = buffer.toString(StandardCharsets.UTF_8);
        assertEquals(0, status, out);
        assertTrue(out.contains("48 records, 45 accepted, 3 rejected (3 known upstream parity, 0 unexpected)"), out);
        assertTrue(out.contains("84 records, 84 accepted, 0 rejected (0 known upstream parity, 0 unexpected)"), out);
        assertTrue(out.endsWith("RESULT: PASS" + System.lineSeparator()), out);
    }

    @Test
    void runFailsOnAnUnexpectedRejection(@TempDir Path dir) throws IOException {
        // An s7comm record with no uid: a real disagreement, not an upstream one.
        Path file = Files.writeString(dir.resolve("bad.jsonl"), "{\"ts\":1.0}\n");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        assertEquals(1, ZeekRecordCheck.run(new String[] {"--s7comm", file.toString()}, print(buffer)));
        assertTrue(buffer.toString(StandardCharsets.UTF_8).contains("UNEXPECTED"));
    }

    @Test
    void runReportsNoRecordsSeparatelyFromAPass(@TempDir Path dir) throws IOException {
        Path empty = Files.writeString(dir.resolve("empty.jsonl"), "");
        assertEquals(3, ZeekRecordCheck.run(new String[] {"--modbus", empty.toString()},
            print(new ByteArrayOutputStream())));
    }

    @Test
    void runRejectsBadArguments() {
        assertEquals(2, ZeekRecordCheck.run(new String[] {"--modbus"}, print(new ByteArrayOutputStream())));
        assertEquals(2, ZeekRecordCheck.run(new String[] {"--dnp3", "x"}, print(new ByteArrayOutputStream())));
        assertEquals(2, ZeekRecordCheck.run(new String[] {"--modbus", "/no/such/file"},
            print(new ByteArrayOutputStream())));
    }

    // The fixture's lines, in order.
    private static List<String> fixture(String name) throws IOException {
        return Files.readAllLines(FIXTURES.resolve(name), StandardCharsets.UTF_8);
    }

    // The Modbus function name a record carries.
    private static String funcOf(String line) {
        Matcher m = FUNC.matcher(line);
        return m.find() ? m.group(1) : "";
    }

    private static PrintStream print(ByteArrayOutputStream buffer) {
        return new PrintStream(buffer, true, StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw -o test -pl modules/bootstrap-online-job -Dtest=ZeekRecordCheckTest 2>&1 | grep -E "cannot find symbol" | head -3` (its siblings resolve from `~/.m2`, installed in Task 1 and unchanged since)
Expected: compilation failure — `cannot find symbol … class ZeekRecordCheck`.

- [ ] **Step 3: Write the implementation**

`ZeekRecordCheck.java`:

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.ModbusEventMapper;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

// Checks real sensor output against the production parsers: every JSON line of
// a Zeek modbus_detailed or s7comm log goes through the same parser and mapper
// the online job uses, and each rejection is reported. deploy.sh zeek-check runs
// it on the server -- over ICSNPP's sample traces put through our Zeek image,
// and over the newest records on the live raw topics -- and ZeekRecordCheckTest
// pins it here against the committed real-output fixtures.
//
// It never changes what the parsers accept. A sensor whose records they reject
// is a finding to report: the parsers mirror the frozen models' training input
// (docs/superpowers/specs/2026-09-24-server-deployment-design.md §10).
public final class ZeekRecordCheck {

    public enum Protocol { MODBUS, S7COMM }

    // One rejected record and the reason the parser or mapper gave.
    public record Rejection(Protocol protocol, ReasonCode reason, String detail, String line) {

        // A rejection upstream's own engine makes too: a Modbus function name its
        // FUNCTION_NAME_TO_CODE table cannot resolve -- Zeek's <NAME>_EXCEPTION
        // PDUs, and ENCAP_INTERFACE_TRANSPORT, which upstream spells
        // ENCAPSULATED_INTERFACE_TRANSPORT (CLAUDE.md, Modbus limits, F4). Any
        // other rejection means the sensor's output and the parsers disagree.
        public boolean knownUpstreamParity() {
            return protocol == Protocol.MODBUS
                && reason == ReasonCode.MISSING_REQUIRED_FIELD
                && detail.startsWith("func did not resolve to a known function code");
        }
    }

    // The outcome for one protocol's records.
    public record Report(Protocol protocol, int accepted, List<Rejection> rejections) {

        public Report {
            rejections = List.copyOf(rejections);
        }

        public int records() {
            return accepted + rejections.size();
        }

        public boolean passes() {
            return rejections.stream().allMatch(Rejection::knownUpstreamParity);
        }
    }

    // The sensor id only labels event ids, which this check never looks at.
    private static final SensorId SENSOR = new SensorId("zeek-record-check");

    private ZeekRecordCheck() {
    }

    // Parse and map every non-blank line as one record of the given protocol.
    public static Report check(Protocol protocol, List<String> lines) {
        int accepted = 0;
        List<Rejection> rejections = new ArrayList<>();
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            MappingResult<NetworkEvent> result = parseAndMap(protocol, line.getBytes(StandardCharsets.UTF_8));
            if (result.isValid()) {
                accepted++;
            } else {
                rejections.add(new Rejection(protocol, result.reason(), result.detail(), line));
            }
        }
        return new Report(protocol, accepted, rejections);
    }

    // The online job's own two stages for the protocol: parse, then map. A parse
    // failure is carried through as it is, with the parser's reason.
    private static MappingResult<NetworkEvent> parseAndMap(Protocol protocol, byte[] json) {
        return switch (protocol) {
            case MODBUS -> {
                MappingResult<ZeekModbusRecord> parsed = new JsonZeekModbusParser().parse(json);
                yield parsed.isValid()
                    ? new ModbusEventMapper().map(parsed.value(), SENSOR)
                    : MappingResult.invalid(parsed.reason(), parsed.detail());
            }
            case S7COMM -> {
                MappingResult<ZeekS7commRecord> parsed = new JsonZeekS7commParser().parse(json);
                yield parsed.isValid()
                    ? new S7commEventMapper().map(parsed.value(), SENSOR)
                    : MappingResult.invalid(parsed.reason(), parsed.detail());
            }
        };
    }

    // usage: ZeekRecordCheck (--modbus|--s7comm) FILE [(--modbus|--s7comm) FILE ...]
    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    // Exit status: 0 when every rejection is a known upstream-parity one; 1 when
    // one is not; 2 for bad arguments or an unreadable file; 3 when there were no
    // records at all (a live topic with no traffic yet), so nothing was proven.
    static int run(String[] args, PrintStream out) {
        if (args.length == 0 || args.length % 2 != 0) {
            out.println("usage: ZeekRecordCheck (--modbus|--s7comm) FILE [(--modbus|--s7comm) FILE ...]");
            return 2;
        }
        List<Report> reports = new ArrayList<>();
        for (int i = 0; i < args.length; i += 2) {
            // The option names the protocol whose parser reads the next file.
            Protocol protocol = switch (args[i]) {
                case "--modbus" -> Protocol.MODBUS;
                case "--s7comm" -> Protocol.S7COMM;
                default -> null;
            };
            if (protocol == null) {
                out.println("unknown option " + args[i] + "; expected --modbus or --s7comm");
                return 2;
            }
            List<String> lines;
            try {
                lines = Files.readAllLines(Path.of(args[i + 1]), StandardCharsets.UTF_8);
            } catch (IOException e) {
                out.println("cannot read " + args[i + 1] + ": " + e.getMessage());
                return 2;
            }
            Report report = check(protocol, lines);
            reports.add(report);
            print(report, args[i + 1], out);
        }

        // Nothing checked is its own outcome, never a pass.
        if (reports.stream().mapToInt(Report::records).sum() == 0) {
            out.println("RESULT: NO RECORDS -- nothing was checked");
            return 3;
        }
        boolean pass = reports.stream().allMatch(Report::passes);
        out.println(pass ? "RESULT: PASS" : "RESULT: FAIL -- the sensor's records and the parsers disagree");
        return pass ? 0 : 1;
    }

    // One summary line per protocol, then one line per rejection.
    private static void print(Report report, String file, PrintStream out) {
        long known = report.rejections().stream().filter(Rejection::knownUpstreamParity).count();
        out.printf("%s (%s): %d records, %d accepted, %d rejected (%d known upstream parity, %d unexpected)%n",
            report.protocol().name().toLowerCase(Locale.ROOT), file, report.records(), report.accepted(),
            report.rejections().size(), known, report.rejections().size() - known);
        for (Rejection r : report.rejections()) {
            out.printf("  %s %s %s :: %s%n", r.knownUpstreamParity() ? "known     " : "UNEXPECTED",
                r.reason(), r.detail(), abbreviate(r.line()));
        }
    }

    // Long records are cut so one bad line cannot flood the terminal.
    private static String abbreviate(String line) {
        return line.length() <= 240 ? line : line.substring(0, 240) + "...";
    }
}
```

- [ ] **Step 4: Run the test**

Run: `rm -rf modules/bootstrap-online-job/target/surefire-reports; ./mvnw -o test -pl modules/bootstrap-online-job -Dtest=ZeekRecordCheckTest 2>&1 | grep "Tests run:" | tail -1`
Expected: `Tests run: 8, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 5: Prove the known-rejection rule is load-bearing**

Temporarily change `knownUpstreamParity()` to `return false;`, re-run Step 4.
Expected: failures in `realIcsnppModbusRecords…` and `runPassesOnTheFixtures…`. Restore the method with an Edit (never `git checkout`), re-run Step 4: `Tests run: 8, Failures: 0`.

- [ ] **Step 6: Pin the class into the shaded JAR**

In `deploy/tests/check-jars.sh`, inside the `online)` branch after the Modbus-parser assertion, add:

```bash
      assert_eq yes "$(has "$entries" io/netsecml/platform/bootstrap/online/ZeekRecordCheck.class)" "online JAR contains ZeekRecordCheck"
```

Run: `./mvnw -o -q install -DskipTests && bash deploy/tests/check-jars.sh`
Expected: `check-jars.sh: 38 checks, 0 failed`.

- [ ] **Step 7: Commit**

```bash
git add modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/ZeekRecordCheck.java modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/ZeekRecordCheckTest.java deploy/tests/check-jars.sh
git commit -m "feat(deploy): ZeekRecordCheck pins the parsers against real ICSNPP output"
```

---

### Task 4: Resource tuning

**Files:**
- Modify: `deploy/lib/common.sh` (append the rest)
- Create: `deploy/lib/tune.sh`, `deploy/tests/test_common.sh`, `deploy/tests/test_tune.sh`

**Interfaces:**
- Consumes: `log/warn/die`, `DEPLOY_DIR`, `ENV_FILE`, `COMPOSE_PROJECT` (Task 2's common.sh).
- Produces (common.sh): `load_env`, `data_dir_abs PATH`, `env_value FILE KEY`, `env_set FILE KEY VALUE`, `setting KEY` (deploy/.env, else the template), `compose ARGS…`, `wait_for TIMEOUT WHAT CMD…`, `host_addr`, `ch_query SQL [URL_PARAMS]`, `flink_rest PATH`, `NETSEC_DATA_DIR_ABS`.
- Produces (tune.sh): `tune_compute CORES TOTAL AVAIL OWN BUDGET_OVERRIDE CPUS_OVERRIDE FORCE` → `KEY=VALUE` lines (keys below), return 2 when refused; `mem_to_mib STR`, `size_to_mib STR`, `own_usage_mib`, `write_tune_block FILE CONTENT`, `tune_run [--dry-run] [--memory-budget S] [--cpus N] [--force]`, `tune_check_drift`. Keys: `TUNE_DETECTED_CORES TUNE_DETECTED_MEM_TOTAL_MIB TUNE_BUDGET_MIB FLINK_TM_MEMORY_MIB FLINK_TM_PROCESS_MIB FLINK_JM_MEMORY_MIB FLINK_JM_PROCESS_MIB CLICKHOUSE_MEMORY_MIB KAFKA_MEMORY_MIB KAFKA_HEAP_MIB ZEEK_MEMORY_MIB FLINK_PARALLELISM FLINK_TASK_SLOTS FLINK_TM_CPUS CLICKHOUSE_CPUS KAFKA_CPUS ZEEK_CPUS FLINK_JM_CPUS`.

- [ ] **Step 1: Write the failing tests**

`deploy/tests/test_common.sh`:

```bash
#!/usr/bin/env bash
# Pins common.sh's env-file editing, path resolution and address helpers.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# env_set replaces an existing key in place and appends a new one.
printf 'A=1\nB=2\n' > "$tmp/e"
env_set "$tmp/e" A 9
env_set "$tmp/e" C 3
assert_eq "$(printf 'A=9\nB=2\nC=3')" "$(cat "$tmp/e")" "env_set replaces and appends"
assert_eq 2 "$(env_value "$tmp/e" B)" "env_value reads a key"
assert_eq "" "$(env_value "$tmp/e" MISSING)" "env_value of a missing key is empty"

# Relative data paths resolve against deploy/, like compose does.
assert_eq "${DEPLOY_DIR}/data" "$(data_dir_abs ./data)" "relative data dir"
assert_eq /srv/netsec "$(data_dir_abs /srv/netsec)" "absolute data dir"

# host_addr: loopback when bound everywhere, else the bind address itself.
assert_eq 127.0.0.1 "$(BIND_ADDRESS=0.0.0.0 host_addr)" "0.0.0.0 -> loopback"
assert_eq 10.1.2.3 "$(BIND_ADDRESS=10.1.2.3 host_addr)" "a specific bind address"

# load_env without a deploy/.env stops with the way forward.
out="$( (ENV_FILE="$tmp/none"; load_env) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c "run './deploy/deploy.sh install" <<< "$out")" "load_env names install"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "load_env exits 1"

finish
```

`deploy/tests/test_tune.sh`:

```bash
#!/usr/bin/env bash
# Pins tune_compute's arithmetic (design §7, rulings P4/P5/P10) on seven hosts,
# plus the size parsers, the .env block writer and own_usage_mib.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"

# The value of KEY in tune_compute output $1.
v() { sed -n "s/^$2=//p" <<< "$1"; }

# assert_alloc NAME OUTPUT KEY=VALUE... -- every listed key has that value.
assert_alloc() {
  local name="$1" out="$2" pair; shift 2
  for pair in "$@"; do
    assert_eq "${pair#*=}" "$(v "$out" "${pair%%=*}")" "${name}: ${pair%%=*}"
  done
}

# A: 8 cores, 32 GiB, 24 GiB free -> budget = 24576 - 3276 = 21300.
out="$(tune_compute 8 32768 24576 0 0 0 0)"
assert_alloc A "$out" TUNE_BUDGET_MIB=21300 FLINK_TM_MEMORY_MIB=8520 FLINK_TM_PROCESS_MIB=8456 \
  FLINK_JM_MEMORY_MIB=1024 FLINK_JM_PROCESS_MIB=960 CLICKHOUSE_MEMORY_MIB=5325 KAFKA_MEMORY_MIB=3195 \
  KAFKA_HEAP_MIB=1597 ZEEK_MEMORY_MIB=2130 FLINK_PARALLELISM=2 FLINK_TASK_SLOTS=4 \
  FLINK_TM_CPUS=2.70 CLICKHOUSE_CPUS=1.50 KAFKA_CPUS=0.90 ZEEK_CPUS=0.90 FLINK_JM_CPUS=1.00 \
  TUNE_DETECTED_CORES=8 TUNE_DETECTED_MEM_TOTAL_MIB=32768

# B: 4 cores, 8 GiB, 6 GiB free -> budget 5120; Kafka lands on its floor.
out="$(tune_compute 4 8192 6144 0 0 0 0)"
assert_alloc B "$out" TUNE_BUDGET_MIB=5120 FLINK_TM_MEMORY_MIB=2048 CLICKHOUSE_MEMORY_MIB=1280 \
  KAFKA_MEMORY_MIB=768 KAFKA_HEAP_MIB=384 ZEEK_MEMORY_MIB=512 FLINK_PARALLELISM=1 FLINK_TASK_SLOTS=2 \
  FLINK_TM_CPUS=1.35 CLICKHOUSE_CPUS=0.75 KAFKA_CPUS=0.50 ZEEK_CPUS=0.50

# C: the development machine (5.7 GiB, 3 GiB free) -> refused unless forced.
err="$(tune_compute 2 5836 3000 0 0 0 0 2>&1 >/dev/null)"; status=$?
assert_eq 2 "$status" "C: refused with status 2"
assert_eq 1 "$(grep -c 'below the 4352 MiB minimum' <<< "$err")" "C: says why"
out="$(tune_compute 2 5836 3000 0 0 0 1)"
assert_alloc "C forced" "$out" TUNE_BUDGET_MIB=1976 FLINK_TM_MEMORY_MIB=1280 CLICKHOUSE_MEMORY_MIB=768 \
  KAFKA_MEMORY_MIB=768 ZEEK_MEMORY_MIB=384 FLINK_TM_CPUS=0.50 CLICKHOUSE_CPUS=0.50

# D: a budget override replaces the computed budget.
out="$(tune_compute 8 32768 24576 0 10240 0 0)"
assert_alloc D "$out" TUNE_BUDGET_MIB=10240 FLINK_TM_MEMORY_MIB=4096 CLICKHOUSE_MEMORY_MIB=2560 \
  KAFKA_MEMORY_MIB=1536 KAFKA_HEAP_MIB=768 ZEEK_MEMORY_MIB=1024

# E: memory our own running stack holds counts as available.
out="$(tune_compute 4 8192 2000 4000 0 0 0)"
assert_alloc E "$out" TUNE_BUDGET_MIB=4976 FLINK_TM_MEMORY_MIB=1990 CLICKHOUSE_MEMORY_MIB=1244 \
  KAFKA_MEMORY_MIB=768 ZEEK_MEMORY_MIB=497

# F: a large host hits every ceiling; 80% of RAM caps the budget.
out="$(tune_compute 64 262144 250000 0 0 0 0)"
assert_alloc F "$out" TUNE_BUDGET_MIB=209715 FLINK_TM_MEMORY_MIB=16384 CLICKHOUSE_MEMORY_MIB=16384 \
  KAFKA_MEMORY_MIB=6144 KAFKA_HEAP_MIB=3072 ZEEK_MEMORY_MIB=4096 FLINK_PARALLELISM=4 FLINK_TASK_SLOTS=8 \
  FLINK_TM_CPUS=21.60 CLICKHOUSE_CPUS=12.00 KAFKA_CPUS=7.20 ZEEK_CPUS=7.20

# G: --cpus overrides the usable cores, but never beyond the host's.
out="$(tune_compute 8 32768 24576 0 0 4 0)"
assert_alloc G "$out" FLINK_TM_CPUS=1.80 CLICKHOUSE_CPUS=1.00 KAFKA_CPUS=0.60 ZEEK_CPUS=0.60
out="$(tune_compute 4 8192 6144 0 0 16 0)"
assert_alloc "G capped" "$out" FLINK_TM_CPUS=1.80 CLICKHOUSE_CPUS=1.00

# docker stats units -> MiB.
assert_eq 1536 "$(mem_to_mib 1.5GiB)" "mem_to_mib GiB"
assert_eq 512 "$(mem_to_mib 512MiB)" "mem_to_mib MiB"
assert_eq 2 "$(mem_to_mib 2048KiB)" "mem_to_mib KiB"
assert_eq 0 "$(mem_to_mib 0B)" "mem_to_mib bytes"
assert_eq 953 "$(mem_to_mib 1GB)" "mem_to_mib decimal GB"

# --memory-budget sizes -> MiB.
assert_eq 12288 "$(size_to_mib 12g)" "size_to_mib g"
assert_eq 4096 "$(size_to_mib 4096m)" "size_to_mib m"
assert_eq 5000 "$(size_to_mib 5000)" "size_to_mib bare MiB"
out="$( (size_to_mib 1.5g) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "size_to_mib refuses a fraction"

# write_tune_block replaces the old block, keeps every other line, once.
tmp="$(mktemp)"
printf 'A=1\n%s 2026-01-01 ---\nOLD=1\n%s\nB=2\n' "$TUNE_BLOCK_BEGIN" "$TUNE_BLOCK_END" > "$tmp"
write_tune_block "$tmp" "NEW=2"
assert_eq 1 "$(grep -c '^A=1$' "$tmp")" "block writer keeps A"
assert_eq 1 "$(grep -c '^B=2$' "$tmp")" "block writer keeps B"
assert_eq 0 "$(grep -c '^OLD=' "$tmp")" "block writer drops the old block"
assert_eq 1 "$(grep -c '^NEW=2$' "$tmp")" "block writer adds the new block"
assert_eq 1 "$(grep -cF "$TUNE_BLOCK_BEGIN" "$tmp")" "exactly one block"
rm -f "$tmp"

# Review Focus 5: with no netsec-ml containers, `docker stats` must not be asked
# (given no ids it lists EVERY container on the host).
docker() {
  case "$1" in
    ps) printf '' ;;
    stats) printf '9GiB / 10GiB\n' ;;
  esac
}
assert_eq 0 "$(own_usage_mib)" "own_usage_mib is 0 with no netsec-ml containers"
docker() {
  case "$1" in
    ps) printf 'aaa\nbbb\n' ;;
    stats) printf '1GiB / 2GiB\n512MiB / 1GiB\n' ;;
  esac
}
assert_eq 1536 "$(own_usage_mib)" "own_usage_mib sums our containers"
unset -f docker

finish
```

- [ ] **Step 2: Run them to verify they fail**

Run: `bash deploy/tests/test_common.sh; bash deploy/tests/test_tune.sh`
Expected: `test_common.sh`: `env_set: command not found` and failures; `test_tune.sh`: `…/lib/tune.sh: No such file or directory`, exit non-zero.

- [ ] **Step 3: Complete common.sh**

Append to `deploy/lib/common.sh`:

```bash

# Load deploy/.env, exporting every variable so docker compose and the helpers
# see the same values, then resolve the data directory to an absolute path.
load_env() {
  [ -f "$ENV_FILE" ] || die "deploy/.env not found: run './deploy/deploy.sh install --interface <if>' first"
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  NETSEC_DATA_DIR_ABS="$(data_dir_abs "${NETSEC_DATA_DIR:-./data}")"
  export NETSEC_DATA_DIR_ABS
}

# NETSEC_DATA_DIR is relative to deploy/ (as compose resolves it) unless absolute.
data_dir_abs() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *)  printf '%s\n' "${DEPLOY_DIR}/${1#./}" ;;
  esac
}

# Read KEY's value from an env-style file without sourcing it (last one wins).
env_value() {
  [ -f "$1" ] || return 0
  sed -n "s/^$2=//p" "$1" | tail -n 1
}

# Set KEY=VALUE in an env-style file: replace the line if present, else append.
# '|' is the sed delimiter; no value this tool writes ever contains one.
env_set() {
  if grep -q "^$2=" "$1"; then
    sed -i "s|^$2=.*|$2=$3|" "$1"
  else
    printf '%s=%s\n' "$2" "$3" >> "$1"
  fi
}

# A setting from deploy/.env if it has one, else the template's default -- for
# commands that must work before 'install' (doctor, build).
setting() {
  local value
  value="$(env_value "$ENV_FILE" "$1")"
  [ -n "$value" ] || value="$(env_value "${DEPLOY_DIR}/.env.template" "$1")"
  printf '%s\n' "$value"
}

# docker compose, always pinned to this project's name, file and env file, so
# no command here can act on another compose project on the host.
compose() {
  docker compose -p "$COMPOSE_PROJECT" -f "${DEPLOY_DIR}/docker-compose.yml" \
    --env-file "$ENV_FILE" "$@"
}

# Poll until a command succeeds, or fail after TIMEOUT seconds.
# usage: wait_for TIMEOUT DESCRIPTION COMMAND [ARGS...]
wait_for() {
  local timeout="$1" what="$2" waited=0
  shift 2
  until "$@" >/dev/null 2>&1; do
    [ "$waited" -ge "$timeout" ] && die "timed out after ${timeout}s waiting for ${what}"
    sleep 3
    waited=$((waited + 3))
  done
  log "${what}: ready"
}

# The address host-side tools use to reach our published ports: the bind
# address itself, or loopback when bound to every interface.
host_addr() {
  case "${BIND_ADDRESS:-127.0.0.1}" in
    0.0.0.0) printf '127.0.0.1\n' ;;
    *) printf '%s\n' "${BIND_ADDRESS:-127.0.0.1}" ;;
  esac
}

# ClickHouse over its published HTTP port with the deployment's credentials.
# usage: ch_query SQL [EXTRA_URL_PARAMS]   (prints TabSeparated rows)
ch_query() {
  curl --fail --silent --show-error \
    --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-binary "$1" \
    "http://$(host_addr):${CLICKHOUSE_HTTP_PORT}/?database=${CLICKHOUSE_DATABASE}${2:+&$2}"
}

# The Flink REST API over its published port. usage: flink_rest /jobs/overview
flink_rest() {
  curl --fail --silent --show-error "http://$(host_addr):${FLINK_UI_PORT}$1"
}
```

- [ ] **Step 4: Write tune.sh**

`deploy/lib/tune.sh`:

```bash
#!/usr/bin/env bash
# deploy.sh tune: size every service from this host's hardware (design §7,
# rulings P4, P5, P10). tune_compute is pure arithmetic so the tests can pin
# it; the rest detects the hardware and writes the result into deploy/.env.

TUNE_MIN_BUDGET_MIB=4352          # the floors below sum to 4224, plus slack
TUNE_BLOCK_BEGIN="# --- resources: written by deploy.sh tune"
TUNE_BLOCK_END="# --- end of resources ---"

# clamp VALUE FLOOR CEILING
clamp() {
  local value="$1"
  [ "$value" -lt "$2" ] && value="$2"
  [ "$value" -gt "$3" ] && value="$3"
  printf '%s\n' "$value"
}

# A CPU share in hundredths, printed as a Docker cpus value ("1.35"), never
# below 0.50: a limit is a ceiling, and half a core keeps a service responsive.
cpus_value() {
  local hundredths="$1"
  [ "$hundredths" -lt 50 ] && hundredths=50
  printf '%d.%02d\n' $((hundredths / 100)) $((hundredths % 100))
}

# tune_compute CORES MEM_TOTAL_MIB MEM_AVAILABLE_MIB OWN_USAGE_MIB BUDGET_OVERRIDE_MIB CPUS_OVERRIDE FORCE
# Prints KEY=VALUE lines. Returns 2, saying why on stderr, when the memory
# budget is below the minimum and FORCE is not 1. Overrides are 0 when unset.
tune_compute() {
  local cores="$1" total="$2" avail="$3" own="$4" budget_override="$5" cpus_override="$6" force="$7"

  # Memory budget: what is free now plus what our own stack already holds,
  # less a reserve for the OS and the host's other containers, and never more
  # than 80% of the machine.
  local reserve=$(( total * 10 / 100 ))
  [ "$reserve" -lt 1024 ] && reserve=1024
  local budget=$(( avail + own - reserve ))
  local cap=$(( total * 80 / 100 ))
  [ "$budget" -gt "$cap" ] && budget="$cap"
  [ "$budget_override" -gt 0 ] && budget="$budget_override"
  if [ "$budget" -lt "$TUNE_MIN_BUDGET_MIB" ] && [ "$force" != 1 ]; then
    printf 'memory budget %s MiB is below the %s MiB minimum the stack needs: free memory, pass --memory-budget, or --force to try anyway\n' \
      "$budget" "$TUNE_MIN_BUDGET_MIB" >&2
    return 2
  fi

  # Memory split (design §7), each clamped to its floor and ceiling. The
  # JobManager is fixed (P4); each Flink process is its container less 64 MiB (P5).
  local tm ch kafka zeek jm=1024
  tm="$(clamp $(( budget * 40 / 100 )) 1280 16384)"
  ch="$(clamp $(( budget * 25 / 100 )) 768 16384)"
  kafka="$(clamp $(( budget * 15 / 100 )) 768 6144)"
  zeek="$(clamp $(( budget * 10 / 100 )) 384 4096)"

  # CPU: keep a quarter of the cores (at least one) for the host, split the
  # rest; an override may not exceed the host.
  local keep=$(( cores * 25 / 100 ))
  [ "$keep" -lt 1 ] && keep=1
  local usable=$(( cores - keep ))
  [ "$usable" -lt 1 ] && usable=1
  [ "$cpus_override" -gt 0 ] && usable="$cpus_override"
  [ "$usable" -gt "$cores" ] && usable="$cores"

  # Parallelism by core count; two jobs share the TaskManager's slots.
  local parallelism=1
  [ "$cores" -ge 8 ] && parallelism=2
  [ "$cores" -ge 16 ] && parallelism=4

  cat <<EOF
TUNE_DETECTED_CORES=${cores}
TUNE_DETECTED_MEM_TOTAL_MIB=${total}
TUNE_BUDGET_MIB=${budget}
FLINK_TM_MEMORY_MIB=${tm}
FLINK_TM_PROCESS_MIB=$(( tm - 64 ))
FLINK_JM_MEMORY_MIB=${jm}
FLINK_JM_PROCESS_MIB=$(( jm - 64 ))
CLICKHOUSE_MEMORY_MIB=${ch}
KAFKA_MEMORY_MIB=${kafka}
KAFKA_HEAP_MIB=$(( kafka / 2 ))
ZEEK_MEMORY_MIB=${zeek}
FLINK_PARALLELISM=${parallelism}
FLINK_TASK_SLOTS=$(( parallelism * 2 ))
FLINK_TM_CPUS=$(cpus_value $(( usable * 45 )))
CLICKHOUSE_CPUS=$(cpus_value $(( usable * 25 )))
KAFKA_CPUS=$(cpus_value $(( usable * 15 )))
ZEEK_CPUS=$(cpus_value $(( usable * 15 )))
FLINK_JM_CPUS=1.00
EOF
}

# A docker stats memory figure ("1.5GiB", "512MiB", "1GB", "0B") in whole MiB.
mem_to_mib() {
  awk -v s="$1" 'BEGIN {
    n = s + 0; u = s; sub(/^[0-9.]+/, "", u)
    if (u ~ /^Ki/) f = 1 / 1024;          else if (u ~ /^Mi/) f = 1
    else if (u ~ /^Gi/) f = 1024;         else if (u ~ /^Ti/) f = 1048576
    else if (u ~ /^kB/) f = 1e3 / 1048576; else if (u ~ /^MB/) f = 1e6 / 1048576
    else if (u ~ /^GB/) f = 1e9 / 1048576; else f = 1 / 1048576
    printf "%d\n", n * f }'
}

# A --memory-budget size ("12g", "4096m", or bare MiB) in MiB.
size_to_mib() {
  [[ "$1" =~ ^[0-9]+[gGmM]?$ ]] || die "size '$1' must be whole MiB, or end in g or m (e.g. 12g)"
  case "$1" in
    *[gG]) printf '%s\n' $(( ${1%[gG]} * 1024 )) ;;
    *[mM]) printf '%s\n' "${1%[mM]}" ;;
    *) printf '%s\n' "$1" ;;
  esac
}

# The memory our own running containers hold now, in MiB, so re-running tune
# while the stack is up does not count its own usage as unavailable.
own_usage_mib() {
  local ids used total=0
  ids="$(docker ps -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}" 2>/dev/null)"
  # No containers of ours: 0. `docker stats` given no ids would list EVERY
  # container on the host and count theirs as ours (Review Focus 5).
  if [ -z "$ids" ]; then
    printf '0\n'
    return 0
  fi
  # shellcheck disable=SC2086  # one container id per word, split on purpose
  while read -r used; do
    total=$(( total + $(mem_to_mib "$used") ))
  done < <(docker stats --no-stream --format '{{.MemUsage}}' $ids | cut -d/ -f1 | tr -d ' ')
  printf '%s\n' "$total"
}

# Replace the tune block in FILE (or append one) with CONTENT, keeping every
# other line and the file's mode (600).
write_tune_block() {
  local file="$1" content="$2" tmp
  tmp="$(mktemp)"
  awk -v b="$TUNE_BLOCK_BEGIN" -v e="$TUNE_BLOCK_END" '
    index($0, b) == 1 { skip = 1; next }
    skip && index($0, e) == 1 { skip = 0; next }
    !skip { print }' "$file" > "$tmp"
  {
    printf '%s %s ---\n' "$TUNE_BLOCK_BEGIN" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '%s\n' "$content"
    printf '%s\n' "$TUNE_BLOCK_END"
  } >> "$tmp"
  cat "$tmp" > "$file"
  rm -f "$tmp"
}

# The allocation as a table, from tune_compute output $1.
tune_print_table() {
  local r="$1" avail="$2" own="$3"
  _tv() { sed -n "s/^$1=//p" <<< "$r"; }
  log "hardware: $(_tv TUNE_DETECTED_CORES) cores, $(_tv TUNE_DETECTED_MEM_TOTAL_MIB) MiB RAM, ${avail} MiB available now (other containers already excluded; +${own} MiB held by netsec-ml)"
  log "memory budget for netsec-ml: $(_tv TUNE_BUDGET_MIB) MiB"
  printf '  %-18s %10s %7s\n' service memory cpus
  printf '  %-18s %6s MiB %7s\n' flink-taskmanager "$(_tv FLINK_TM_MEMORY_MIB)" "$(_tv FLINK_TM_CPUS)"
  printf '  %-18s %6s MiB %7s\n' flink-jobmanager "$(_tv FLINK_JM_MEMORY_MIB)" "$(_tv FLINK_JM_CPUS)"
  printf '  %-18s %6s MiB %7s\n' clickhouse "$(_tv CLICKHOUSE_MEMORY_MIB)" "$(_tv CLICKHOUSE_CPUS)"
  printf '  %-18s %6s MiB %7s\n' kafka "$(_tv KAFKA_MEMORY_MIB)" "$(_tv KAFKA_CPUS)"
  printf '  %-18s %6s MiB %7s\n' zeek "$(_tv ZEEK_MEMORY_MIB)" "$(_tv ZEEK_CPUS)"
  printf '  flink parallelism %s, task slots %s\n' "$(_tv FLINK_PARALLELISM)" "$(_tv FLINK_TASK_SLOTS)"
}

# deploy.sh tune [--dry-run] [--memory-budget SIZE] [--cpus N] [--force]
tune_run() {
  local dry_run=0 budget_override=0 cpus_override=0 force=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --dry-run) dry_run=1; shift ;;
      --memory-budget) budget_override="$(size_to_mib "${2:?--memory-budget needs a size, e.g. 12g}")"; shift 2 ;;
      --cpus) cpus_override="${2:?--cpus needs a whole number}"; shift 2 ;;
      --force) force=1; shift ;;
      *) die "tune: unknown option $1" ;;
    esac
  done

  # Detect: cores, total RAM, RAM available now, and what our stack holds.
  local cores total avail own result
  cores="$(nproc)"
  total="$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)"
  avail="$(awk '/^MemAvailable:/ {print int($2 / 1024)}' /proc/meminfo)"
  own="$(own_usage_mib)"
  result="$(tune_compute "$cores" "$total" "$avail" "$own" "$budget_override" "$cpus_override" "$force")" \
    || die "tune refused (see the reason above); fix it, then run 'deploy.sh tune' (its options are in 'deploy.sh help')"

  tune_print_table "$result" "$avail" "$own"
  if [ "$dry_run" -eq 1 ]; then
    log "dry run: deploy/.env not changed"
  else
    write_tune_block "$ENV_FILE" "$result"
    log "resources written to deploy/.env; 'deploy.sh restart' applies them to a running stack"
  fi
}

# Warn when this host's cores or RAM differ from what the last tune recorded;
# fail when there is no resources block at all.
tune_check_drift() {
  local rec_cores rec_total total drift
  rec_cores="$(env_value "$ENV_FILE" TUNE_DETECTED_CORES)"
  rec_total="$(env_value "$ENV_FILE" TUNE_DETECTED_MEM_TOTAL_MIB)"
  [ -n "$rec_cores" ] && [ -n "$rec_total" ] || die "deploy/.env has no resources block: run 'deploy.sh tune'"
  total="$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)"
  drift=$(( (total - rec_total) * 100 / rec_total ))
  if [ "$rec_cores" != "$(nproc)" ] || [ "${drift#-}" -gt 5 ]; then
    warn "hardware changed since the last tune (${rec_cores} cores/${rec_total} MiB then, $(nproc)/${total} now): run 'deploy.sh tune' and 'deploy.sh restart'"
  fi
}
```

- [ ] **Step 5: Run the tests**

Run: `bash deploy/tests/test_tune.sh; bash deploy/tests/test_common.sh`
Expected: `test_tune.sh: 84 checks, 0 failed`; `test_common.sh: 9 checks, 0 failed`.

- [ ] **Step 6: Commit**

```bash
git add deploy/lib/common.sh deploy/lib/tune.sh deploy/tests/test_common.sh deploy/tests/test_tune.sh
git commit -m "feat(deploy): size every service from the host's CPU and RAM"
```

---

### Task 5: The Compose stack definition

**Files:**
- Create: `deploy/.env.template`, `deploy/docker-compose.yml`, `deploy/kafka/topics.conf`, `deploy/clickhouse/config.d/netsec.xml`, `deploy/tests/test_compose.sh`

**Interfaces:**
- Consumes: `tune_compute` (Task 4) to build a test env.
- Produces: services `kafka`, `clickhouse`, `flink-jobmanager`, `flink-taskmanager`, `job-submitter`, `zeek`; the supervisor mounts `./jars` → `/opt/netsec/jars`, `./flink/submit-jobs.sh` → `/opt/netsec/submit-jobs.sh`, `${NETSEC_DATA_DIR}/flink` → `/flink-data`. `topics.conf` lines: `ENV_VAR PARTITIONS RETENTION_HOURS`.

- [ ] **Step 1: Write the failing test**

`deploy/tests/test_compose.sh`:

```bash
#!/usr/bin/env bash
# Pins the Compose definition statically (`docker compose config`: nothing
# starts): services, restart policy, port bindings (P6), Zeek's capture
# rights, memory/CPU limits from the tune block, and the jobs' settings.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# A test env: the template, generated values, and host B's tune block.
make_env() {
  cp "${DEPLOY_DIR}/.env.template" "$tmp/env"
  env_set "$tmp/env" SENSOR_ID sensor-test
  env_set "$tmp/env" ZEEK_INTERFACE eth9
  env_set "$tmp/env" KAFKA_CLUSTER_ID bmV0c2VjLW1sLXRlc3QxMg
  env_set "$tmp/env" CLICKHOUSE_PASSWORD testpassword
  env_set "$tmp/env" BIND_ADDRESS "$1"
  write_tune_block "$tmp/env" "$(tune_compute 4 8192 6144 0 0 0 0)"
}

# The rendered project as JSON.
config() {
  docker compose -p netsec-ml -f "${DEPLOY_DIR}/docker-compose.yml" --env-file "$tmp/env" config --format json
}

make_env 127.0.0.1
json="$(config)"; status=$?
assert_eq 0 "$status" "compose config renders"
q() { jq -r "$1" <<< "$json"; }

assert_eq netsec-ml "$(q .name)" "project name"
assert_eq "clickhouse flink-jobmanager flink-taskmanager job-submitter kafka zeek" \
  "$(q '.services | keys | join(" ")')" "the six services"
assert_eq unless-stopped "$(q '[.services[].restart] | unique | join(",")')" "every service restarts unless stopped"
assert_eq 127.0.0.1 "$(q '[.services[].ports[]?.host_ip] | unique | join(",")')" "every port on loopback by default"
assert_eq "19092 18081 18123" \
  "$(q '[.services.kafka.ports[0].published, .services["flink-jobmanager"].ports[0].published, .services.clickhouse.ports[0].published] | map(tostring) | join(" ")')" \
  "published ports"
assert_eq host "$(q '.services.zeek.network_mode')" "zeek on the host network"
assert_eq "NET_ADMIN,NET_RAW" "$(q '.services.zeek.cap_add | sort | join(",")')" "zeek capture rights"
assert_eq eth9 "$(q '.services.zeek.environment.ZEEK_INTERFACE')" "zeek interface"
assert_eq 127.0.0.1:19092 "$(q '.services.zeek.environment.NETSEC_KAFKA_BROKERS')" "zeek reaches Kafka on loopback"
assert_eq "$((2048 * 1048576))" "$(q '.services["flink-taskmanager"].mem_limit')" "taskmanager memory from tune"
assert_eq 1 "$(q '.services["flink-taskmanager"].environment.FLINK_PROPERTIES' | grep -c '^taskmanager.memory.process.size: 1984m$')" "taskmanager process size (P5)"
assert_eq 0.5 "$(q '.services.kafka.cpus')" "kafka CPUs from tune"
assert_eq "-Xms384m -Xmx384m" "$(q '.services.kafka.environment.KAFKA_HEAP_OPTS')" "kafka heap"
assert_eq kafka:29092 "$(q '.services["job-submitter"].environment.KAFKA_BOOTSTRAP_SERVERS')" "jobs read Kafka inside the network"
assert_eq clickhouse "$(q '.services["job-submitter"].environment.CLICKHOUSE_HOST')" "archive job reaches ClickHouse by name"
assert_eq sensor-test "$(q '.services["job-submitter"].environment.SENSOR_ID')" "sensor id reaches the jobs"
assert_eq netsec.s7comm.raw.v1 "$(q '.services["job-submitter"].environment.S7COMM_RAW_TOPIC')" "topics reach the jobs"

# P6: widening BIND_ADDRESS moves the UI and ClickHouse, never Kafka.
make_env 0.0.0.0
json="$(config)"
assert_eq 127.0.0.1 "$(q '.services.kafka.ports[0].host_ip')" "kafka stays on loopback"
assert_eq 0.0.0.0 "$(q '.services["flink-jobmanager"].ports[0].host_ip')" "flink UI follows BIND_ADDRESS"

# setting() falls back to the template when deploy/.env lacks the key.
assert_eq 18081 "$(ENV_FILE="$tmp/none" setting FLINK_UI_PORT)" "setting falls back to .env.template"

finish
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash deploy/tests/test_compose.sh`
Expected: `cp: cannot stat '…/deploy/.env.template'`, failures, exit 1.

- [ ] **Step 3: Write the template, the compose file, the topic list and the ClickHouse config**

`deploy/.env.template`:

```bash
# netsec-ml deployment settings (design: docs/superpowers/specs/2026-09-24-server-deployment-design.md).
# 'deploy.sh install' copies this file to deploy/.env (mode 600) and fills in the
# values marked GENERATED; 'deploy.sh tune' appends the resources block. Edit
# deploy/.env, never this template. Values must be plain words: no spaces, no quotes.

# --- identity ---
# The sensor name stamped into every event id. GENERATED: this host's name.
SENSOR_ID=
# The network interface that sees the OT traffic (a mirror/SPAN port). Set it
# with 'deploy.sh install --interface <name>'; 'deploy.sh doctor' lists them.
ZEEK_INTERFACE=

# --- images (pinned; change only deliberately) ---
FLINK_IMAGE=flink:2.2.1-java21
KAFKA_IMAGE=confluentinc/cp-kafka:7.6.1
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:25.8
ZEEK_BASE_IMAGE=zeek/zeek:7.0.9
ZEEK_IMAGE=netsec-ml/zeek:1
MAVEN_IMAGE=maven:3.9.9-eclipse-temurin-21

# --- published ports ---
# The Flink UI and ClickHouse bind to BIND_ADDRESS (loopback by default; reach
# them remotely through an SSH tunnel). Kafka's port is always loopback-only.
BIND_ADDRESS=127.0.0.1
FLINK_UI_PORT=18081
KAFKA_HOST_PORT=19092
CLICKHOUSE_HTTP_PORT=18123

# --- data ---
# Relative paths are relative to deploy/.
NETSEC_DATA_DIR=./data

# --- Kafka ---
# GENERATED: the KRaft cluster id.
KAFKA_CLUSTER_ID=

# --- ClickHouse ---
CLICKHOUSE_DATABASE=netsec_ml
CLICKHOUSE_USER=netsec
# GENERATED: 32 random letters and digits.
CLICKHOUSE_PASSWORD=

# --- topics (read by both jobs' main(); the names match .env.example) ---
MODBUS_RAW_TOPIC=netsec.modbus.raw.v1
MODBUS_FEATURE_VECTOR_TOPIC=netsec.modbus.feature-vector.v1
MODBUS_DLQ_TOPIC=netsec.modbus.dlq.v1
S7COMM_RAW_TOPIC=netsec.s7comm.raw.v1
S7COMM_FEATURE_VECTOR_TOPIC=netsec.s7comm.feature-vector.v1
S7COMM_DLQ_TOPIC=netsec.s7comm.dlq.v1
# conn and dns are not ingested by this deployment, but both jobs subscribe to
# these topics unconditionally, so they must exist ('up' creates them empty).
CONN_INPUT_TOPIC=conn
FEATURE_VECTOR_TOPIC=netsec.conn.feature-vector.v1
DLQ_TOPIC=netsec.conn.dlq.v1
DNS_INPUT_TOPIC=dns
DNS_FEATURE_VECTOR_TOPIC=netsec.dns.feature-vector.v1
DNS_DLQ_TOPIC=netsec.dns.dlq.v1

# --- job settings ---
# Idle TTL of each S7 connection's state, in minutes. Set it above the longest
# outage the online job may have (CLAUDE.md, S7comm limits: the idle TTL).
S7COMM_STATE_TTL_MINUTES=60
```

`deploy/docker-compose.yml`:

```yaml
# netsec-ml: the Modbus + S7comm feature pipeline on one server (design:
# docs/superpowers/specs/2026-09-24-server-deployment-design.md). Driven by
# deploy/deploy.sh; every value comes from deploy/.env (see .env.template),
# including the memory/CPU block 'deploy.sh tune' writes. It shares no network,
# port or volume with any other compose project on the host.
name: netsec-ml

services:
  # Kafka, KRaft single node. Containers use kafka:29092. The HOST listener is
  # Zeek's (host network) and is always bound to loopback (ruling P6): it
  # advertises 127.0.0.1, so that is the only address it can be reached at.
  kafka:
    image: ${KAFKA_IMAGE}
    restart: unless-stopped
    environment:
      CLUSTER_ID: ${KAFKA_CLUSTER_ID}
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,HOST://0.0.0.0:19092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,HOST://127.0.0.1:${KAFKA_HOST_PORT}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      # A mistyped topic must fail loudly, never appear silently.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_HEAP_OPTS: -Xms${KAFKA_HEAP_MIB}m -Xmx${KAFKA_HEAP_MIB}m
    ports:
      - "127.0.0.1:${KAFKA_HOST_PORT}:19092"
    volumes:
      - ${NETSEC_DATA_DIR}/kafka:/var/lib/kafka/data
    mem_limit: ${KAFKA_MEMORY_MIB}m
    cpus: ${KAFKA_CPUS}
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:29092 --list >/dev/null"]
      interval: 15s
      timeout: 20s
      retries: 10
      start_period: 40s

  # ClickHouse: the archive's feature_vectors, invalid_events and predictions.
  # The image creates the user and database from these variables.
  clickhouse:
    image: ${CLICKHOUSE_IMAGE}
    restart: unless-stopped
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DATABASE}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
    ports:
      - "${BIND_ADDRESS}:${CLICKHOUSE_HTTP_PORT}:8123"
    volumes:
      - ${NETSEC_DATA_DIR}/clickhouse:/var/lib/clickhouse
      - ./clickhouse/config.d/netsec.xml:/etc/clickhouse-server/config.d/netsec.xml:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    mem_limit: ${CLICKHOUSE_MEMORY_MIB}m
    cpus: ${CLICKHOUSE_CPUS}
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://127.0.0.1:8123/ping"]
      interval: 15s
      timeout: 10s
      retries: 10

  # Flink session cluster: the JobManager (web UI, job coordination) ...
  flink-jobmanager:
    image: ${FLINK_IMAGE}
    restart: unless-stopped
    command: jobmanager
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager
      FLINK_PROPERTIES: |
        jobmanager.memory.process.size: ${FLINK_JM_PROCESS_MIB}m
        parallelism.default: ${FLINK_PARALLELISM}
    ports:
      - "${BIND_ADDRESS}:${FLINK_UI_PORT}:8081"
    volumes:
      - ${NETSEC_DATA_DIR}/flink:/flink-data
    mem_limit: ${FLINK_JM_MEMORY_MIB}m
    cpus: ${FLINK_JM_CPUS}
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8081/overview >/dev/null"]
      interval: 15s
      timeout: 10s
      retries: 10

  # ... and the TaskManager, which runs both jobs. Managed memory is kept small:
  # the jobs use the heap state backend, which does not use it.
  flink-taskmanager:
    image: ${FLINK_IMAGE}
    restart: unless-stopped
    command: taskmanager
    depends_on:
      - flink-jobmanager
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager
      FLINK_PROPERTIES: |
        taskmanager.memory.process.size: ${FLINK_TM_PROCESS_MIB}m
        taskmanager.numberOfTaskSlots: ${FLINK_TASK_SLOTS}
        taskmanager.memory.managed.fraction: 0.05
    volumes:
      - ${NETSEC_DATA_DIR}/flink:/flink-data
    mem_limit: ${FLINK_TM_MEMORY_MIB}m
    cpus: ${FLINK_TM_CPUS}

  # The job supervisor (design §6, ruling P9): every 60 s it submits each job
  # that is not running, resuming from its newest savepoint or retained
  # checkpoint. Both jobs' main() runs here, so this is where their settings live.
  job-submitter:
    image: ${FLINK_IMAGE}
    restart: unless-stopped
    command: ["bash", "/opt/netsec/submit-jobs.sh"]
    depends_on:
      - flink-jobmanager
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager
      FLINK_PROPERTIES: |
        rest.address: flink-jobmanager
        rest.port: 8081
        parallelism.default: ${FLINK_PARALLELISM}
      JVM_ARGS: -Xmx256m
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SENSOR_ID: ${SENSOR_ID}
      CONN_INPUT_TOPIC: ${CONN_INPUT_TOPIC}
      FEATURE_VECTOR_TOPIC: ${FEATURE_VECTOR_TOPIC}
      DLQ_TOPIC: ${DLQ_TOPIC}
      DNS_INPUT_TOPIC: ${DNS_INPUT_TOPIC}
      DNS_FEATURE_VECTOR_TOPIC: ${DNS_FEATURE_VECTOR_TOPIC}
      DNS_DLQ_TOPIC: ${DNS_DLQ_TOPIC}
      MODBUS_RAW_TOPIC: ${MODBUS_RAW_TOPIC}
      MODBUS_FEATURE_VECTOR_TOPIC: ${MODBUS_FEATURE_VECTOR_TOPIC}
      MODBUS_DLQ_TOPIC: ${MODBUS_DLQ_TOPIC}
      S7COMM_RAW_TOPIC: ${S7COMM_RAW_TOPIC}
      S7COMM_FEATURE_VECTOR_TOPIC: ${S7COMM_FEATURE_VECTOR_TOPIC}
      S7COMM_DLQ_TOPIC: ${S7COMM_DLQ_TOPIC}
      S7COMM_STATE_TTL_MINUTES: ${S7COMM_STATE_TTL_MINUTES}
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_PORT: "8123"
      CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
    volumes:
      - ./jars:/opt/netsec/jars:ro
      - ./flink/submit-jobs.sh:/opt/netsec/submit-jobs.sh:ro
      - ${NETSEC_DATA_DIR}/flink:/flink-data
    mem_limit: 640m

  # The Zeek sensor (design §4): sniffs ZEEK_INTERFACE on the host network and
  # sends the two OT logs to Kafka's loopback listener.
  zeek:
    image: ${ZEEK_IMAGE}
    restart: unless-stopped
    network_mode: host
    cap_add:
      - NET_RAW
      - NET_ADMIN
    environment:
      ZEEK_INTERFACE: ${ZEEK_INTERFACE}
      NETSEC_KAFKA_BROKERS: 127.0.0.1:${KAFKA_HOST_PORT}
      MODBUS_RAW_TOPIC: ${MODBUS_RAW_TOPIC}
      S7COMM_RAW_TOPIC: ${S7COMM_RAW_TOPIC}
    mem_limit: ${ZEEK_MEMORY_MIB}m
    cpus: ${ZEEK_CPUS}
```

`deploy/kafka/topics.conf`:

```
# One line per topic 'deploy.sh up' creates (idempotently): the deploy/.env
# variable that holds its name, its partition count, its retention in hours.
# The raw topics get ONE partition on purpose: total order per topic is what
# satisfies the per-key arrival order both OT protocols need (design §5).
MODBUS_RAW_TOPIC              1  168
S7COMM_RAW_TOPIC              1  168
MODBUS_FEATURE_VECTOR_TOPIC   1  168
S7COMM_FEATURE_VECTOR_TOPIC   1  168
MODBUS_DLQ_TOPIC              1  336
S7COMM_DLQ_TOPIC              1  336
# conn and dns are not ingested here; these exist only because both jobs
# subscribe to them unconditionally, and a missing topic crash-loops a job.
CONN_INPUT_TOPIC              1  24
FEATURE_VECTOR_TOPIC          1  24
DLQ_TOPIC                     1  24
DNS_INPUT_TOPIC               1  24
DNS_FEATURE_VECTOR_TOPIC      1  24
DNS_DLQ_TOPIC                 1  24
```

`deploy/clickhouse/config.d/netsec.xml`:

```xml
<!-- netsec-ml: keep ClickHouse inside its container limit (design §7). It
     reads the cgroup limit as its RAM, so 0.8 is 80% of what tune gave it. -->
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio>
</clickhouse>
```

- [ ] **Step 4: Run the tests**

Run: `bash deploy/tests/test_compose.sh; bash deploy/tests/test_common.sh`
Expected: `test_compose.sh: 21 checks, 0 failed`. If `mem_limit` renders as a string other than bytes in this compose version, read `docker compose … config --format json | jq .services.kafka.mem_limit`, and rule on the assertion's form (not on the value) in the ledger.

- [ ] **Step 5: Commit**

```bash
git add deploy/.env.template deploy/docker-compose.yml deploy/kafka/topics.conf deploy/clickhouse/config.d/netsec.xml deploy/tests/test_compose.sh
git commit -m "feat(deploy): the netsec-ml compose stack, sized from the tune block"
```

---

### Task 6: The job supervisor

**Files:**
- Create: `deploy/flink/submit-jobs.sh`, `deploy/tests/test_submit_jobs.sh`

**Interfaces:**
- Consumes: the `job-submitter` service's mounts and environment (Task 5).
- Produces: `submit-jobs.sh [supervise|once]`; functions `newest_restore_point JOB`, `active_job_names` (stdin: `/jobs/overview` JSON), `submit JOB JAR CLASS`, `prune_restore_points JOB`, `supervise_once`. Checkpoints go to `/flink-data/checkpoints/<job>/<job-id>/chk-N`, savepoints to `/flink-data/savepoints/<job>/savepoint-…`.

- [ ] **Step 1: Write the failing test**

`deploy/tests/test_submit_jobs.sh`:

```bash
#!/usr/bin/env bash
# Pins the supervisor's decisions with stubbed curl and flink: which restore
# point it picks, which jobs it thinks are running, what it submits, what it
# prunes, and what it says when a submission fails (Review Focus 1).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export NETSEC_FLINK_DATA="$tmp/data"
. "$HERE/../flink/submit-jobs.sh"
# The script sets -e for itself; a test must survive a failing check.
set +e

# Make a restore point of a given age: the _metadata file (what
# newest_restore_point reads) and its folder (what prune_restore_points sorts).
restore_point() { mkdir -p "$1"; touch -d "$2" "$1/_metadata" "$1"; }

# None yet: nothing printed.
assert_eq "" "$(newest_restore_point online-feature-job)" "no restore point"

# A savepoint alone is chosen.
restore_point "$DATA/savepoints/online-feature-job/savepoint-aaa" "2026-09-20 10:00"
assert_eq "$DATA/savepoints/online-feature-job/savepoint-aaa" "$(newest_restore_point online-feature-job)" "savepoint chosen"

# A newer retained checkpoint wins over it; another job's points never count.
restore_point "$DATA/checkpoints/online-feature-job/0123abcd/chk-7" "2026-09-21 10:00"
restore_point "$DATA/checkpoints/archive-job/ffff/chk-9" "2026-09-22 10:00"
assert_eq "$DATA/checkpoints/online-feature-job/0123abcd/chk-7" "$(newest_restore_point online-feature-job)" "newer checkpoint wins"

# A directory without _metadata (an incomplete checkpoint) is ignored.
mkdir -p "$DATA/checkpoints/online-feature-job/0123abcd/chk-8"
assert_eq "$DATA/checkpoints/online-feature-job/0123abcd/chk-7" "$(newest_restore_point online-feature-job)" "incomplete ignored"

# Running and about-to-run jobs count as active; finished and failed ones do not.
overview='{"jobs":[{"jid":"a1","name":"online-feature-job","start-time":1,"state":"RUNNING"},
{"jid":"b2","name":"archive-job","start-time":2,"state":"FAILED"},
{"jid":"c3","name":"archive-job","start-time":3,"state":"FINISHED"}]}'
assert_eq online-feature-job "$(active_job_names <<< "$overview")" "active job names"
assert_eq "" "$(active_job_names <<< '{"jobs":[]}')" "no jobs"

# submit: per-job checkpoint dir always; -s only when a restore point exists.
flink() { printf '%s\n' "$*" > "$tmp/flink-args"; return 0; }
submit online-feature-job online-feature-job.jar io.netsecml.platform.bootstrap.online.OnlineFeatureJob >/dev/null
args="$(cat "$tmp/flink-args")"
assert_eq 1 "$(grep -c -- "-Dexecution.checkpointing.dir=file://$DATA/checkpoints/online-feature-job" <<< "$args")" "per-job checkpoint dir"
assert_eq 1 "$(grep -c -- "-s file://$DATA/checkpoints/online-feature-job/0123abcd/chk-7" <<< "$args")" "resumes from the newest point"
assert_eq 1 "$(grep -c -- '-c io.netsecml.platform.bootstrap.online.OnlineFeatureJob' <<< "$args")" "names the main class"
assert_eq 1 "$(grep -c -- "/opt/netsec/jars/online-feature-job.jar\$" <<< "$args")" "submits the job's JAR"
rm -rf "$DATA/checkpoints/archive-job"
submit archive-job archive-job.jar io.netsecml.platform.bootstrap.archive.ArchiveJob >/dev/null
assert_eq 0 "$(grep -c -- ' -s ' "$tmp/flink-args")" "first start has no -s"

# prune keeps the three newest savepoints.
for i in 1 2 3 4 5; do restore_point "$DATA/savepoints/archive-job/savepoint-$i" "2026-09-0$i 10:00"; done
prune_restore_points archive-job
assert_eq "savepoint-3 savepoint-4 savepoint-5" "$(ls "$DATA/savepoints/archive-job" | sort | tr '\n' ' ' | sed 's/ $//')" "prune keeps the 3 newest"

# Review Focus 1: a failed submission logs how to start that job fresh, and the
# supervisor carries on to the next job.
curl() { printf '{"jobs":[]}'; }
flink() { printf '%s\n' "$*" >> "$tmp/calls"; return 1; }
out="$(supervise_once 2>&1)"
assert_eq 1 "$(grep -c "submitting online-feature-job failed" <<< "$out")" "failure is logged"
assert_eq 1 "$(grep -c "move $DATA/savepoints/online-feature-job and $DATA/checkpoints/online-feature-job aside" <<< "$out")" "the way out is named"
assert_eq 2 "$(grep -c 'run -d' "$tmp/calls")" "both jobs were still attempted"
unset -f curl flink

finish
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash deploy/tests/test_submit_jobs.sh`
Expected: `…/flink/submit-jobs.sh: No such file or directory`, failures, exit 1.

- [ ] **Step 3: Write the supervisor**

`deploy/flink/submit-jobs.sh`:

```bash
#!/usr/bin/env bash
# netsec-ml job supervisor, run by the job-submitter container (design §6,
# ruling P9). Every 60 s: each job the session cluster is not running is
# submitted again from its newest restore point. So 'deploy.sh up', a server
# reboot and a JobManager restart all end with both jobs running on their
# saved state. Both jobs' main() runs here, reading this container's env.
set -euo pipefail

DATA="${NETSEC_FLINK_DATA:-/flink-data}"
JARS="${NETSEC_JARS:-/opt/netsec/jars}"
REST="${NETSEC_FLINK_REST:-http://flink-jobmanager:8081}"
# name  jar  main class
JOBS=(
  "online-feature-job online-feature-job.jar io.netsecml.platform.bootstrap.online.OnlineFeatureJob"
  "archive-job archive-job.jar io.netsecml.platform.bootstrap.archive.ArchiveJob"
)

log() { printf '%s [job-submitter] %s\n' "$(date -u +%FT%TZ)" "$*"; }

# The newest restore point of a job: a savepoint written by 'deploy.sh down' or
# a checkpoint retained after a crash, whichever completed last. A restore
# point is a directory holding a _metadata file. Prints nothing if none exists.
newest_restore_point() {
  # '|| true': before a job's first savepoint or checkpoint its folders do not
  # exist, and under 'set -e -o pipefail' a failing find would end the supervisor.
  { find "${DATA}/savepoints/$1" "${DATA}/checkpoints/$1" -name _metadata -type f -printf '%T@ %h\n' 2>/dev/null || true; } \
    | sort -n | tail -n 1 | cut -d' ' -f2-
}

# Names of the jobs the cluster runs or is about to run, from a /jobs/overview
# document on stdin. Terminal states (FINISHED, FAILED, CANCELED) are not active.
active_job_names() {
  tr '{' '\n' \
    | sed -n 's/.*"name":"\([^"]*\)".*"state":"\(RUNNING\|RESTARTING\|CREATED\|INITIALIZING\|RECONCILING\|FAILING\|CANCELLING\)".*/\1/p' \
    | sort -u
}

# Submit one job, detached, with its own checkpoint directory (which is what
# makes "the newest checkpoint of this job" answerable) and retained
# checkpoints; resume from its newest restore point when there is one.
submit() {
  local name="$1" jar="$2" class="$3" restore
  restore="$(newest_restore_point "$name")"
  local args=(run -d -c "$class"
    "-Dexecution.checkpointing.dir=file://${DATA}/checkpoints/${name}"
    "-Dexecution.checkpointing.savepoint-dir=file://${DATA}/savepoints/${name}"
    -Dexecution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION
    -Dexecution.checkpointing.num-retained=3)
  if [ -n "$restore" ]; then
    log "submitting ${name}, resuming from ${restore}"
    args+=(-s "file://${restore}")
  else
    log "submitting ${name} with no saved state (first start)"
  fi
  flink "${args[@]}" "${JARS}/${jar}"
}

# Keep the three newest savepoints and checkpoint runs of a job; the one just
# restored from and the new run's own directory are always among them.
prune_restore_points() {
  local dir
  for dir in "${DATA}/savepoints/$1" "${DATA}/checkpoints/$1"; do
    # '|| true' for the same reason as in newest_restore_point.
    { find "$dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null || true; } \
      | sort -rn | tail -n +4 | cut -d' ' -f2- | xargs -r rm -rf
  done
}

# One pass: submit every job that is not active. A failed submission is
# logged with the way out, and the next job is still tried (Review Focus 1).
supervise_once() {
  local active entry name jar class
  active="$(curl -fsS "${REST}/jobs/overview" | active_job_names)" || return 0
  for entry in "${JOBS[@]}"; do
    read -r name jar class <<< "$entry"
    grep -qxF "$name" <<< "$active" && continue
    if submit "$name" "$jar" "$class"; then
      prune_restore_points "$name"
    else
      log "submitting ${name} failed; retrying in 60 s. If it keeps failing while restoring, its saved state no longer fits the job: move ${DATA}/savepoints/${name} and ${DATA}/checkpoints/${name} aside to start it fresh"
    fi
  done
}

main() {
  # Wait for the JobManager, then supervise forever (or once, for a check).
  until curl -fsS "${REST}/overview" >/dev/null 2>&1; do
    log "waiting for the JobManager at ${REST}"
    sleep 5
  done
  while true; do
    supervise_once
    [ "${1:-supervise}" = once ] && return 0
    sleep 60
  done
}

# Run only when executed, so the tests can source the functions.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
```

- [ ] **Step 4: Run the test**

Run: `bash deploy/tests/test_submit_jobs.sh`
Expected: `test_submit_jobs.sh: 15 checks, 0 failed`.

- [ ] **Step 5: Commit**

```bash
git add deploy/flink/submit-jobs.sh deploy/tests/test_submit_jobs.sh
git commit -m "feat(deploy): job supervisor that resumes each job from its newest restore point"
```

---

### Task 7: `deploy.sh`, doctor, install/uninstall and build

**Files:**
- Create: `deploy/deploy.sh`, `deploy/lib/doctor.sh`, `deploy/lib/install.sh`, `deploy/lib/build.sh`, `deploy/tests/test_install_doctor.sh`, `deploy/lib/stack.sh` (stub: `stack_up`/`stack_down`/`stack_status` that `die "not yet implemented"` — replaced in Task 8), `deploy/lib/selftest.sh` (stub: `selftest_run`/`zeek_check_run` likewise — replaced in Task 9)

**Interfaces:**
- Consumes: common.sh, tune.sh, traces.sh.
- Produces: `deploy.sh <command>`; doctor.sh: `port_in_use PORT`, `own_stack_running`, `list_interfaces`, `doctor_run`; install.sh: `install_run [--interface IF]`, `create_env_file IF`, `random_token N`, `kafka_cluster_id`, `prepare_data_dirs`, `uninstall_run [--purge]`; build.sh: `build_run [--with-tests] [--jars-only]`, `maven ARGS…`.

- [ ] **Step 1: Write the failing test**

`deploy/tests/test_install_doctor.sh`:

```bash
#!/usr/bin/env bash
# Pins install's env-file creation and doctor's port and interface checks,
# with docker, ss and ip stubbed (nothing is installed, nothing starts).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"
. "$HERE/../lib/doctor.sh"
. "$HERE/../lib/install.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Generated secrets: 32 letters/digits; a 22-character base64url KRaft id.
assert_eq 1 "$(random_token 32 | grep -cE '^[A-Za-z0-9]{32}$')" "random_token shape"
assert_eq 1 "$(kafka_cluster_id | grep -cE '^[A-Za-z0-9_-]{22}$')" "kafka_cluster_id shape"

# create_env_file: mode 600, generated values, the interface; a second run keeps them.
ENV_FILE="$tmp/.env"
create_env_file eth7 >/dev/null 2>&1
assert_eq 600 "$(stat -c %a "$ENV_FILE")" ".env is mode 600"
assert_eq "$(hostname -s)" "$(env_value "$ENV_FILE" SENSOR_ID)" "SENSOR_ID is the host name"
assert_eq eth7 "$(env_value "$ENV_FILE" ZEEK_INTERFACE)" "interface recorded"
password="$(env_value "$ENV_FILE" CLICKHOUSE_PASSWORD)"
assert_eq 32 "${#password}" "ClickHouse password generated"
create_env_file "" >/dev/null 2>&1
assert_eq "$password" "$(env_value "$ENV_FILE" CLICKHOUSE_PASSWORD)" "a second install keeps the password"
assert_eq eth7 "$(env_value "$ENV_FILE" ZEEK_INTERFACE)" "and the interface"

# Review Focus 4: a port held by another process is reported; ours is not.
ss() { [ "$*" = "-Hltn sport = :18081" ] && printf 'LISTEN 0 4096 127.0.0.1:18081 0.0.0.0:*\n'; return 0; }
assert_eq yes "$(port_in_use 18081 && echo yes || echo no)" "port_in_use sees a listener"
assert_eq no "$(port_in_use 18123 && echo yes || echo no)" "port_in_use sees a free port"
docker() {
  case "$1" in
    ps) printf '' ;;
    version) printf '27.0.0\n' ;;
  esac
  return 0
}
curl() { printf '401'; }       # "Docker Hub reachable", without the network
ip() { printf '1: lo: <LOOPBACK>\n2: eth0: <UP>\n3: ens5@if9: <UP>\n'; }
out="$(doctor_run 2>&1)"
assert_eq 1 "$(grep -c 'FAIL  port 18081 (FLINK_UI_PORT) is taken by another process' <<< "$out")" "doctor names the taken port"
assert_eq "lo eth0 ens5" "$(list_interfaces | tr '\n' ' ' | sed 's/ $//')" "interfaces listed without @peer"
assert_eq 1 "$(grep -c 'FAIL  capture interface eth7 does not exist' <<< "$out")" "doctor flags a missing interface"
unset -f ss docker curl ip

finish
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash deploy/tests/test_install_doctor.sh`
Expected: `…/lib/doctor.sh: No such file or directory`, failures, exit 1.

- [ ] **Step 3: Write doctor.sh**

`deploy/lib/doctor.sh`:

```bash
#!/usr/bin/env bash
# deploy.sh doctor: read-only checks of this host (design §8). One line per
# check -- PASS, WARN, FAIL or INFO -- and exit status 1 if anything FAILed.

# True when something already listens on TCP port $1.
port_in_use() {
  ss -Hltn "sport = :$1" 2>/dev/null | grep -q .
}

# True when our own stack is running (then it is the one holding our ports).
own_stack_running() {
  [ -n "$(docker ps -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}" 2>/dev/null)" ]
}

# This host's network interface names, one per line ("veth1@if5" -> "veth1").
list_interfaces() {
  ip -o link show | awk -F': ' '{print $2}' | cut -d@ -f1
}

doctor_run() {
  local fails=0
  # Print one check; count the failures.
  _check() {
    printf '  %-5s %s\n' "$1" "$2"
    if [ "$1" = FAIL ]; then fails=$((fails + 1)); fi
  }
  log "doctor: checking this host (read-only)"

  # Operating system.
  local os=unknown
  if [ -r /etc/os-release ]; then os="$(. /etc/os-release && printf '%s' "${PRETTY_NAME:-unknown}")"; fi
  _check INFO "OS: ${os} ($(uname -m))"

  # Docker engine and compose plugin, and the two host tools the scripts use.
  if ! command -v docker >/dev/null 2>&1; then
    _check FAIL "Docker is not installed ('deploy.sh install' installs it)"
  elif ! docker info >/dev/null 2>&1; then
    _check FAIL "Docker is installed but this user cannot reach it: add the user to the 'docker' group, or run as root"
  else
    local version; version="$(docker version --format '{{.Server.Version}}')"
    if [ "${version%%.*}" -ge 24 ]; then _check PASS "Docker ${version}"; else _check FAIL "Docker ${version}: 24 or newer is needed"; fi
    if docker compose version >/dev/null 2>&1; then _check PASS "docker compose $(docker compose version --short)"
    else _check FAIL "the docker compose plugin is missing ('deploy.sh install' installs it)"; fi
  fi
  local tool
  for tool in curl jq; do
    if command -v "$tool" >/dev/null 2>&1; then _check PASS "$tool"; else _check FAIL "${tool} is missing ('deploy.sh install' installs it)"; fi
  done

  # Hardware, against the design's recommended minimums.
  local cores mem disk
  cores="$(nproc)"
  mem="$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)"
  disk="$(df -Pm "$DEPLOY_DIR" | awk 'NR == 2 {print $4}')"
  if [ "$cores" -ge 4 ]; then _check PASS "${cores} CPU cores"; else _check WARN "${cores} CPU cores (4 or more recommended)"; fi
  if [ "$mem" -ge 7800 ]; then _check PASS "${mem} MiB RAM"; else _check WARN "${mem} MiB RAM (8 GB or more recommended)"; fi
  if [ "$disk" -ge 51200 ]; then _check PASS "$((disk / 1024)) GiB free disk"; else _check WARN "$((disk / 1024)) GiB free disk (50 GB or more recommended)"; fi

  # Our published ports must be free, unless our own stack is what holds them.
  local var port
  for var in FLINK_UI_PORT KAFKA_HOST_PORT CLICKHOUSE_HTTP_PORT; do
    port="$(setting "$var")"
    if ! command -v ss >/dev/null 2>&1; then _check WARN "cannot check port ${port} (no 'ss' command)"
    elif port_in_use "$port" && ! own_stack_running; then _check FAIL "port ${port} (${var}) is taken by another process: change ${var} in deploy/.env"
    else _check PASS "port ${port} (${var})"; fi
  done

  # The capture interface Zeek will sniff.
  local iface interfaces
  iface="$(setting ZEEK_INTERFACE)"
  interfaces="$(list_interfaces | tr '\n' ' ')"
  if [ -z "$iface" ]; then _check WARN "ZEEK_INTERFACE is not set; this host has: ${interfaces}"
  elif list_interfaces | grep -qxF "$iface"; then _check PASS "capture interface ${iface}"
  else _check FAIL "capture interface ${iface} does not exist; this host has: ${interfaces}"; fi

  # Internet, for pulling images and building (401 = reachable, auth needed).
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 https://registry-1.docker.io/v2/ || true)"
  if [ "$code" = 401 ] || [ "$code" = 200 ]; then _check PASS "Docker Hub reachable"; else _check FAIL "Docker Hub unreachable (HTTP '${code}')"; fi

  if [ "$fails" -gt 0 ]; then
    log "doctor: ${fails} check(s) failed"
    return 1
  fi
  log "doctor: no failures"
}
```

- [ ] **Step 4: Write install.sh**

`deploy/lib/install.sh`:

```bash
#!/usr/bin/env bash
# deploy.sh install / uninstall (design §8).

# Letters and digits only: safe unquoted in .env, YAML and a URL. Reads a fixed
# 512 bytes first, so no pipe is cut short under 'set -o pipefail'.
random_token() {
  local pool
  pool="$(head -c 512 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9')"
  printf '%s\n' "${pool:0:$1}"
}

# A KRaft cluster id: 16 random bytes, base64url without padding (22 chars).
kafka_cluster_id() {
  head -c 16 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n'
  printf '\n'
}

# Install OS packages with whichever package manager this host has.
install_os_packages() {
  local sudo=""
  [ "$(id -u)" -ne 0 ] && sudo=sudo
  if command -v apt-get >/dev/null 2>&1; then $sudo apt-get update -q && $sudo apt-get install -y -q "$@"
  elif command -v dnf >/dev/null 2>&1; then $sudo dnf install -y -q "$@"
  elif command -v yum >/dev/null 2>&1; then $sudo yum install -y -q "$@"
  else die "no apt-get, dnf or yum here: install $* by hand"; fi
}

# curl and jq, then Docker and its compose plugin -- each only if missing.
install_packages() {
  local missing=() tool
  for tool in curl jq; do command -v "$tool" >/dev/null 2>&1 || missing+=("$tool"); done
  if [ "${#missing[@]}" -gt 0 ]; then
    log "installing ${missing[*]}"
    install_os_packages "${missing[@]}"
  fi
  if ! command -v docker >/dev/null 2>&1; then
    log "installing Docker Engine and the compose plugin (get.docker.com)"
    local sudo=""
    [ "$(id -u)" -ne 0 ] && sudo=sudo
    curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
    $sudo sh /tmp/get-docker.sh
    rm -f /tmp/get-docker.sh
  elif ! docker compose version >/dev/null 2>&1; then
    log "installing the docker compose plugin"
    install_os_packages docker-compose-plugin
  else
    log "Docker $(docker version --format '{{.Server.Version}}' 2>/dev/null) and compose already installed"
  fi
}

# deploy/.env from the template (mode 600) with its generated values; an
# existing file keeps every value. A non-empty interface is always recorded.
create_env_file() {
  local interface="$1"
  if [ -f "$ENV_FILE" ]; then
    log "deploy/.env exists: keeping its values"
  else
    log "creating deploy/.env from .env.template"
    (umask 077 && cp "${DEPLOY_DIR}/.env.template" "$ENV_FILE")
    chmod 600 "$ENV_FILE"
    env_set "$ENV_FILE" SENSOR_ID "$(hostname -s)"
    env_set "$ENV_FILE" CLICKHOUSE_PASSWORD "$(random_token 32)"
    env_set "$ENV_FILE" KAFKA_CLUSTER_ID "$(kafka_cluster_id)"
  fi
  [ -n "$interface" ] && env_set "$ENV_FILE" ZEEK_INTERFACE "$interface"
  if [ -z "$(env_value "$ENV_FILE" ZEEK_INTERFACE)" ]; then
    warn "ZEEK_INTERFACE is empty: set it with 'install --interface <name>' before 'up' ('doctor' lists the interfaces)"
  fi
}

# The data folders, owned by the uid each image runs as: Kafka 1000 (appuser),
# ClickHouse 101, Flink 9999. The chown runs in a container, so no sudo.
prepare_data_dirs() {
  local d="$NETSEC_DATA_DIR_ABS"
  mkdir -p "${d}/kafka" "${d}/clickhouse" "${d}/flink/checkpoints" "${d}/flink/savepoints"
  docker run --rm --user 0 --entrypoint sh -v "${d}:/data" "$FLINK_IMAGE" -c \
    'chown -R 1000:1000 /data/kafka && chown -R 101:101 /data/clickhouse && chown -R 9999:9999 /data/flink'
}

# deploy.sh install [--interface IF]
install_run() {
  local interface=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --interface) interface="${2:?--interface needs a name}"; shift 2 ;;
      *) die "install: unknown option $1" ;;
    esac
  done
  install_packages
  create_env_file "$interface"
  load_env
  prepare_data_dirs
  tune_run
  log "install complete. Next: ./deploy/deploy.sh build, then ./deploy/deploy.sh up"
}

# deploy.sh uninstall [--purge]: stop (with savepoints), remove our containers,
# network and the built Zeek image; --purge also deletes every byte of data.
uninstall_run() {
  local purge=0 answer
  [ "${1:-}" = --purge ] && purge=1
  load_env
  if [ -n "$(docker ps -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}")" ]; then
    stack_down
  fi
  log "removing the netsec-ml containers and network, and ${ZEEK_IMAGE}"
  compose down --remove-orphans
  docker image rm -f "$ZEEK_IMAGE" >/dev/null 2>&1 || true
  if [ "$purge" -eq 1 ]; then
    printf 'This deletes ALL netsec-ml data in %s (Kafka topics, ClickHouse tables, Flink state).\nType "delete netsec-ml data" to confirm: ' "$NETSEC_DATA_DIR_ABS"
    read -r answer
    [ "$answer" = "delete netsec-ml data" ] || die "not confirmed; data kept"
    docker run --rm --user 0 --entrypoint sh -v "${NETSEC_DATA_DIR_ABS}:/data" "$FLINK_IMAGE" -c \
      'rm -rf /data/kafka /data/clickhouse /data/flink'
    log "data deleted; deploy/.env kept (delete it by hand to start completely fresh)"
  fi
}
```

- [ ] **Step 5: Write build.sh, the stubs and deploy.sh**

`deploy/lib/build.sh`:

```bash
#!/usr/bin/env bash
# deploy.sh build [--with-tests] [--jars-only]: the two job JARs, compiled in
# the pinned Maven/JDK 21 image (no Java on the host), and the Zeek image.

# Maven in a container, as the invoking user so target/ stays theirs; the local
# repository is cached in deploy/.m2 between builds.
maven() {
  docker run --rm --user "$(id -u):$(id -g)" \
    -v "${REPO_ROOT}:/src" -w /src \
    -v "${DEPLOY_DIR}/.m2:/var/maven/.m2" -e MAVEN_CONFIG=/var/maven/.m2 \
    "$(setting MAVEN_IMAGE)" mvn -B -q -Duser.home=/var/maven "$@"
}

build_run() {
  local with_tests=0 jars_only=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --with-tests) with_tests=1; shift ;;
      --jars-only) jars_only=1; shift ;;
      *) die "build: unknown option $1" ;;
    esac
  done
  mkdir -p "${DEPLOY_DIR}/.m2" "${DEPLOY_DIR}/jars"

  # Unit suites only: the modules whose tests need no containers (CLAUDE.md,
  # Verification state). The container-backed suites never run here.
  if [ "$with_tests" -eq 1 ]; then
    log "running the unit test suites (domain, application, adapter-kafka, adapter-flink)"
    maven -pl modules/domain,modules/application,modules/adapter-kafka,modules/adapter-flink -am verify
  fi

  # The shaded JARs, copied to the names the supervisor submits.
  log "building the job JARs"
  maven -pl modules/bootstrap-online-job,modules/bootstrap-archive-job -am -DskipTests package
  cp "${REPO_ROOT}"/modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar "${DEPLOY_DIR}/jars/online-feature-job.jar"
  cp "${REPO_ROOT}"/modules/bootstrap-archive-job/target/bootstrap-archive-job-*-all.jar "${DEPLOY_DIR}/jars/archive-job.jar"
  log "jars: $(cd "${DEPLOY_DIR}/jars" && ls -1 | tr '\n' ' ')"

  # The sensor image (compiles two plugins: ~7 minutes the first time).
  if [ "$jars_only" -eq 0 ]; then
    log "building the Zeek sensor image $(setting ZEEK_IMAGE)"
    docker build --build-arg "ZEEK_BASE_IMAGE=$(setting ZEEK_BASE_IMAGE)" -t "$(setting ZEEK_IMAGE)" "${DEPLOY_DIR}/zeek"
  fi
  log "build complete"
}
```

`deploy/lib/stack.sh` (stub, replaced in Task 8):

```bash
#!/usr/bin/env bash
# deploy.sh up / down / status -- implemented in the next task.
stack_up() { die "up: not implemented yet"; }
stack_down() { die "down: not implemented yet"; }
stack_status() { die "status: not implemented yet"; }
```

`deploy/lib/selftest.sh` (stub, replaced in Task 9):

```bash
#!/usr/bin/env bash
# deploy.sh selftest / zeek-check -- implemented in a later task.
selftest_run() { die "selftest: not implemented yet"; }
zeek_check_run() { die "zeek-check: not implemented yet"; }
```

`deploy/deploy.sh`:

```bash
#!/usr/bin/env bash
# netsec-ml deployment tool: installs, sizes, builds, runs and checks the
# Modbus + S7comm pipeline on one server
# (docs/superpowers/specs/2026-09-24-server-deployment-design.md).
# Run ./deploy/deploy.sh help for the commands.
set -euo pipefail

# Every lib module, in dependency order.
LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib"
for module in common tune traces doctor install build stack selftest; do
  # shellcheck disable=SC1090
  . "${LIB}/${module}.sh"
done

usage() {
  cat <<'EOF'
Usage: ./deploy/deploy.sh <command> [options]

  doctor                        check this host (read-only)
  install [--interface IF]      install Docker/curl/jq if missing, create deploy/.env,
                                prepare the data folders, size the services
  tune [--dry-run] [--memory-budget SIZE] [--cpus N] [--force]
                                size every service from this host's CPU and RAM
  build [--with-tests] [--jars-only]
                                build the job JARs (in a container) and the Zeek image
  up                            start everything; the jobs resume their saved state
  down                          stop the jobs with a savepoint, then stop everything
  restart                       down + up (after 'build', this is an upgrade)
  status                        health, jobs, traffic, recent ClickHouse rows
  logs [SERVICE]                follow logs: kafka clickhouse flink-jobmanager
                                flink-taskmanager job-submitter zeek
  sql "QUERY"                   run a ClickHouse query
  selftest                      send one Modbus and one S7 pair through the pipeline
  zeek-check [--live N]         check Zeek's records against the platform's parsers
  uninstall [--purge]           remove containers and the Zeek image (--purge: all data)
EOF
}

main() {
  local command="${1:-help}"
  [ $# -gt 0 ] && shift
  case "$command" in
    doctor) doctor_run "$@" ;;
    install) install_run "$@" ;;
    tune) load_env; tune_run "$@" ;;
    build) build_run "$@" ;;
    up) stack_up ;;
    down) stack_down ;;
    restart) stack_down; stack_up ;;
    status) stack_status ;;
    logs) load_env; compose logs -f --tail 200 "$@" ;;
    sql) load_env; ch_query "${1:?sql needs a query}" ;;
    selftest) selftest_run ;;
    zeek-check) zeek_check_run "$@" ;;
    uninstall) uninstall_run "$@" ;;
    help|-h|--help) usage ;;
    *) usage; die "unknown command: ${command}" ;;
  esac
}

main "$@"
```

Run: `chmod +x deploy/deploy.sh` (the tests run under `bash`, the supervisor under `bash` in its container, and the Dockerfile sets run-zeek.sh's mode)

- [ ] **Step 6: Run the tests and the harmless commands**

Run: `bash deploy/tests/test_install_doctor.sh && ./deploy/deploy.sh help | head -3 && (./deploy/deploy.sh bogus; echo "exit=$?") 2>&1 | tail -2`
Expected: `test_install_doctor.sh: 13 checks, 0 failed`; the usage header; `ERROR: unknown command: bogus` and `exit=1`.

- [ ] **Step 7: Build the JARs through the script (a build, not a run)**

Run: `./deploy/deploy.sh build --jars-only 2>&1 | tail -3 && ls -la deploy/jars/`
Expected: `jars: archive-job.jar online-feature-job.jar`, `build complete`; both files present. (This runs Maven in the pinned container and starts nothing.)

- [ ] **Step 8: Commit**

```bash
git add deploy/deploy.sh deploy/lib/doctor.sh deploy/lib/install.sh deploy/lib/build.sh deploy/lib/stack.sh deploy/lib/selftest.sh deploy/tests/test_install_doctor.sh
git commit -m "feat(deploy): deploy.sh with doctor, install, uninstall and build"
```

---

### Task 8: `up`, `down`, `restart`, `status`

**Files:**
- Modify: `deploy/lib/stack.sh` (replace the stub)
- Create: `deploy/tests/test_stack.sh`

**Interfaces:**
- Consumes: `compose`, `wait_for`, `flink_rest`, `ch_query`, `host_addr`, `load_env`, `setting` (common.sh); `tune_check_drift` (tune.sh); `list_interfaces` (doctor.sh); `prepare_data_dirs` (install.sh); `scripts/database/apply-ddl.sh`.
- Produces: `stack_preflight`, `create_topics`, `apply_ddl`, `running_job_names` / `running_jobs` / `sum_offsets` / `checkpoint_age NOW_MS` (stdin filters), `jobs_running`, `stack_up`, `stack_down`, `stack_status`.

- [ ] **Step 1: Write the failing test**

`deploy/tests/test_stack.sh`:

```bash
#!/usr/bin/env bash
# Pins stack.sh's decisions without starting anything: the JSON/offset
# filters, the topic loop (Review Focus 3) and the preflight (Review Focus 2).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"
. "$HERE/../lib/doctor.sh"
. "$HERE/../lib/install.sh"
. "$HERE/../lib/stack.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# /jobs/overview filters.
overview='{"jobs":[{"jid":"a1","name":"online-feature-job","state":"RUNNING"},
{"jid":"b2","name":"archive-job","state":"RESTARTING"},{"jid":"c3","name":"archive-job","state":"FINISHED"}]}'
assert_eq online-feature-job "$(running_job_names <<< "$overview")" "running job names"
assert_eq "a1 online-feature-job" "$(running_jobs <<< "$overview")" "running jobs with ids"

# kafka-get-offsets output -> total records.
assert_eq 22 "$(printf 't:0:15\nt:1:7\n' | sum_offsets)" "sum_offsets adds partitions"
assert_eq 0 "$(printf '' | sum_offsets)" "sum_offsets of nothing"

# Seconds since the newest completed checkpoint.
assert_eq 42 "$(checkpoint_age 1000042000 <<< '{"latest":{"completed":{"latest_ack_timestamp":1000000000}}}')" "checkpoint age"
assert_eq none "$(checkpoint_age 1 <<< '{"latest":{"completed":null}}')" "no checkpoint yet"

# Review Focus 3: 'compose exec' reads stdin; inside the topic loop it must not
# swallow topics.conf, or only the first topic is ever created.
set -a; . "${DEPLOY_DIR}/.env.template"; set +a
compose() {
  if [ "$1" = exec ]; then
    cat > /dev/null            # what the real 'docker compose exec -T' does to stdin
    printf '%s\n' "$*" >> "$tmp/calls"
  fi
}
create_topics >/dev/null
assert_eq 12 "$(grep -c -- '--create' "$tmp/calls")" "create_topics creates every topic"
assert_eq 1 "$(grep -c -- '--topic netsec.s7comm.raw.v1 --partitions 1 --replication-factor 1 --config retention.ms=604800000' "$tmp/calls")" "raw topic: 1 partition, 7 days"
unset -f compose

# Review Focus 2: preflight refuses an unknown interface and lists the real ones.
DEPLOY_DIR_SAVED="$DEPLOY_DIR"; DEPLOY_DIR="$tmp/deploy"; mkdir -p "$DEPLOY_DIR/jars"
touch "$DEPLOY_DIR/jars/online-feature-job.jar" "$DEPLOY_DIR/jars/archive-job.jar"
docker() { return 0; }                     # 'docker image inspect' succeeds
list_interfaces() { printf 'lo\neth0\n'; }
out="$( (ZEEK_INTERFACE=eth9; stack_preflight) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'capture interface eth9 does not exist (this host has: lo eth0 )' <<< "$out")" "preflight refuses an unknown interface"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "and stops"
out="$( (ZEEK_INTERFACE=""; stack_preflight) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'ZEEK_INTERFACE is not set' <<< "$out")" "preflight refuses an empty interface"
rm "$DEPLOY_DIR/jars/archive-job.jar"
out="$( (ZEEK_INTERFACE=eth0; stack_preflight) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c "job JARs missing: run 'deploy.sh build' first" <<< "$out")" "preflight wants the JARs"
DEPLOY_DIR="$DEPLOY_DIR_SAVED"
unset -f docker list_interfaces

finish
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash deploy/tests/test_stack.sh`
Expected: `running_job_names: command not found` (the stub has no such functions), failures, exit 1.

- [ ] **Step 3: Write stack.sh**

Replace `deploy/lib/stack.sh` with:

```bash
#!/usr/bin/env bash
# deploy.sh up / down / restart / status (design §8).

# --- small filters, pinned by tests/test_stack.sh ---

# Names of RUNNING jobs, from a /jobs/overview document on stdin.
running_job_names() { jq -r '.jobs[] | select(.state == "RUNNING") | .name' | sort -u; }

# "jid name" for each RUNNING job, from a /jobs/overview document on stdin.
running_jobs() { jq -r '.jobs[] | select(.state == "RUNNING") | "\(.jid) \(.name)"'; }

# Total records ever written to a topic, from kafka-get-offsets output
# (topic:partition:offset lines) on stdin.
sum_offsets() { awk -F: 'NF >= 3 { s += $NF } END { print s + 0 }'; }

# Seconds since the newest completed checkpoint, from a /jobs/<id>/checkpoints
# document on stdin; "none" before the first one.
checkpoint_age() {
  jq -r --argjson now "$1" \
    'if .latest.completed == null then "none"
     else (($now - .latest.completed.latest_ack_timestamp) / 1000 | floor | tostring) end'
}

# True when both jobs are RUNNING on the cluster.
jobs_running() {
  [ "$(flink_rest /jobs/overview | running_job_names | grep -cxE 'online-feature-job|archive-job')" -eq 2 ]
}

# True when at least one TaskManager has registered.
taskmanager_registered() { [ "$(flink_rest /overview | jq '.taskmanagers')" -ge 1 ]; }

# --- up ---

# Refuse to start anything that cannot work: missing JARs or Zeek image, an
# unset or unknown capture interface (Review Focus 2), no resources block.
stack_preflight() {
  [ -f "${DEPLOY_DIR}/jars/online-feature-job.jar" ] && [ -f "${DEPLOY_DIR}/jars/archive-job.jar" ] \
    || die "job JARs missing: run 'deploy.sh build' first"
  docker image inspect "$ZEEK_IMAGE" >/dev/null 2>&1 || die "Zeek image ${ZEEK_IMAGE} missing: run 'deploy.sh build' first"
  local interfaces
  interfaces="$(list_interfaces | tr '\n' ' ')"
  [ -n "${ZEEK_INTERFACE:-}" ] || die "ZEEK_INTERFACE is not set: run 'deploy.sh install --interface <name>' (this host has: ${interfaces})"
  list_interfaces | grep -qxF "$ZEEK_INTERFACE" \
    || die "capture interface ${ZEEK_INTERFACE} does not exist (this host has: ${interfaces})"
  tune_check_drift
}

# Every topic both jobs subscribe to, created before they start: a missing
# topic crash-loops a whole job. '</dev/null' keeps 'compose exec' from
# swallowing the rest of topics.conf (Review Focus 3).
create_topics() {
  local var partitions hours topic
  while read -r var partitions hours; do
    case "$var" in ''|'#'*) continue ;; esac
    topic="${!var:?topics.conf names ${var}, which deploy/.env does not set}"
    compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
      --topic "$topic" --partitions "$partitions" --replication-factor 1 \
      --config "retention.ms=$(( hours * 3600000 ))" </dev/null >/dev/null
  done < "${DEPLOY_DIR}/kafka/topics.conf"
  log "topics: $(compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list </dev/null | tr '\n' ' ')"
}

# The ClickHouse tables, through the repository's own idempotent DDL script.
apply_ddl() {
  CLICKHOUSE_HOST="$(host_addr)" CLICKHOUSE_PORT="$CLICKHOUSE_HTTP_PORT" \
    bash "${REPO_ROOT}/scripts/database/apply-ddl.sh"
}

stack_up() {
  load_env
  stack_preflight
  prepare_data_dirs

  # Storage first, then its schema.
  log "starting Kafka and ClickHouse"
  compose up -d kafka clickhouse
  wait_for 180 "Kafka" compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
  wait_for 180 "ClickHouse" curl -fsS "http://$(host_addr):${CLICKHOUSE_HTTP_PORT}/ping"
  create_topics
  apply_ddl

  # Then Flink, and the supervisor that submits (or resumes) both jobs.
  log "starting Flink"
  compose up -d flink-jobmanager flink-taskmanager
  wait_for 180 "Flink JobManager" flink_rest /overview
  wait_for 180 "Flink TaskManager" taskmanager_registered
  log "starting the job supervisor (it submits both jobs, resuming any saved state)"
  compose up -d job-submitter
  wait_for 300 "both jobs RUNNING" jobs_running

  # The sensor last, so its first records find the jobs running.
  log "starting Zeek on ${ZEEK_INTERFACE}"
  compose up -d zeek
  stack_status
}

# --- down ---

stack_down() {
  load_env
  # The sensor and the supervisor first: no new input, and nobody to resubmit
  # the jobs being stopped.
  compose stop zeek job-submitter >/dev/null 2>&1 || true
  if flink_rest /overview >/dev/null 2>&1; then
    local jid name
    while read -r jid name; do
      [ -n "$jid" ] || continue
      log "stopping ${name} with a savepoint"
      compose exec -T flink-jobmanager flink stop \
        --savepointPath "file:///flink-data/savepoints/${name}" "$jid" </dev/null \
        || warn "the savepoint for ${name} failed: it will resume from its newest retained checkpoint"
    done < <(flink_rest /jobs/overview | running_jobs)
  fi
  compose down
  log "stopped; data kept in ${NETSEC_DATA_DIR_ABS}"
}

# --- status ---

stack_status() {
  load_env
  log "containers:"
  compose ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}' || true

  # Jobs, and how old each running job's newest checkpoint is.
  if flink_rest /overview >/dev/null 2>&1; then
    log "Flink jobs:"
    local now jid name
    now="$(date +%s%3N)"
    while read -r jid name; do
      [ -n "$jid" ] || continue
      printf '  %-20s RUNNING   last checkpoint %ss ago\n' "$name" "$(flink_rest "/jobs/${jid}/checkpoints" | checkpoint_age "$now")"
    done < <(flink_rest /jobs/overview | running_jobs)
    flink_rest /jobs/overview | jq -r '.jobs[] | select(.state != "RUNNING") | "  \(.name)  \(.state)"' | sort -u || true
  else
    warn "the Flink JobManager is not reachable"
  fi

  # Traffic: records ever written to each raw topic.
  local var
  log "raw topics (records written since the topic was created):"
  for var in MODBUS_RAW_TOPIC S7COMM_RAW_TOPIC; do
    printf '  %-26s %s\n' "${!var}" \
      "$(compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic "${!var}" </dev/null 2>/dev/null | sum_offsets)"
  done

  # What reached ClickHouse lately, and what was rejected.
  log "feature vectors archived in the last 5 minutes:"
  ch_query "SELECT log_type, count() AS rows FROM feature_vectors WHERE archived_at > now64(3) - INTERVAL 5 MINUTE GROUP BY log_type ORDER BY log_type FORMAT PrettyCompactMonoBlock" \
    || warn "ClickHouse is not reachable"
  log "DLQ rows in the last hour, by reason:"
  ch_query "SELECT log_type, reason_code, count() AS rows FROM invalid_events WHERE received_at > now64(3) - INTERVAL 1 HOUR GROUP BY log_type, reason_code ORDER BY rows DESC LIMIT 10 FORMAT PrettyCompactMonoBlock" \
    || true
}
```

- [ ] **Step 4: Run the tests**

Run: `bash deploy/tests/test_stack.sh`
Expected: `test_stack.sh: 12 checks, 0 failed`.

- [ ] **Step 5: Mutation check of Review Focus 3**

Temporarily delete `</dev/null` from the `--create` call in `create_topics`; run Step 4.
Expected: `create_topics creates every topic` fails (expected 12, actual 1). Restore it with an Edit; re-run: `0 failed`.

- [ ] **Step 6: Commit**

```bash
git add deploy/lib/stack.sh deploy/tests/test_stack.sh
git commit -m "feat(deploy): up, down, restart and status"
```

---

### Task 9: `selftest` and `zeek-check`

**Files:**
- Modify: `deploy/lib/selftest.sh` (replace the stub)
- Create: `deploy/selftest/modbus.jsonl.template`, `deploy/selftest/s7comm.jsonl.template`, `deploy/tests/test_selftest.sh`

**Interfaces:**
- Consumes: `ch_query`, `compose`, `load_env` (common.sh); `sum_offsets` (stack.sh); `fetch_traces`, `zeek_offline_records` (traces.sh); the online JAR's `ZeekRecordCheck` (Task 3).
- Produces: `render_selftest TEMPLATE UID TS_REQ TS_RESP`, `now_epoch`, `plus_ms TS MS`, `selftest_run`, `selftest_cleanup RUN UID_M UID_S`, `zeek_check_run [--live N]`, `live_records TOPIC N`, `run_record_check DIR`.

- [ ] **Step 1: Write the templates**

Both are real ICSNPP records (Task 2's fixtures) with the endpoints moved to documentation-only `192.0.2.0/24` and the uid/timestamps as placeholders.

`deploy/selftest/modbus.jsonl.template`:

```
{"ts":__TS_REQ__,"uid":"__UID__","id_orig_h":"192.0.2.10","id_orig_p":50200,"id_resp_h":"192.0.2.20","id_resp_p":502,"is_orig":true,"source_h":"192.0.2.10","source_p":50200,"destination_h":"192.0.2.20","destination_p":502,"tid":7,"unit":1,"func":"READ_HOLDING_REGISTERS","request_response":"REQUEST","address":1,"quantity":1}
{"ts":__TS_RESP__,"uid":"__UID__","id_orig_h":"192.0.2.10","id_orig_p":50200,"id_resp_h":"192.0.2.20","id_resp_p":502,"is_orig":false,"source_h":"192.0.2.20","source_p":502,"destination_h":"192.0.2.10","destination_p":50200,"tid":7,"unit":1,"func":"READ_HOLDING_REGISTERS","request_response":"RESPONSE","quantity":1,"values":"170"}
```

`deploy/selftest/s7comm.jsonl.template`:

```
{"ts":__TS_REQ__,"uid":"__UID__","id_orig_h":"192.0.2.30","id_orig_p":50300,"id_resp_h":"192.0.2.40","id_resp_p":102,"is_orig":true,"source_h":"192.0.2.30","source_p":50300,"destination_h":"192.0.2.40","destination_p":102,"rosctr_code":1,"rosctr_name":"Job-Request","pdu_reference":25,"function_code":"0x04","function_name":"Read Variable"}
{"ts":__TS_RESP__,"uid":"__UID__","id_orig_h":"192.0.2.30","id_orig_p":50300,"id_resp_h":"192.0.2.40","id_resp_p":102,"is_orig":false,"source_h":"192.0.2.40","source_p":102,"destination_h":"192.0.2.30","destination_p":50300,"rosctr_code":3,"rosctr_name":"ACK-Data","pdu_reference":25,"function_code":"0x04","function_name":"Read Variable","error_class":"No error","error_code":"0x00"}
```

- [ ] **Step 2: Write the failing test**

`deploy/tests/test_selftest.sh`:

```bash
#!/usr/bin/env bash
# Pins the selftest's records: rendered, they are valid JSON with the run's
# uid and ordered timestamps, and the PRODUCTION parsers accept all four
# (ZeekRecordCheck from the built online JAR, on the host's Java 21) -- so a
# selftest failure on the server is never the test records' own fault.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/stack.sh"
. "$HERE/../lib/traces.sh"
. "$HERE/../lib/selftest.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Timestamps: microseconds, and the response 4 ms after the request.
assert_eq 1 "$(now_epoch | grep -cE '^[0-9]{10}\.[0-9]{6}$')" "now_epoch has microseconds"
assert_eq 1790000000.127456 "$(plus_ms 1790000000.123456 4)" "plus_ms"

# Rendering fills every placeholder.
render_selftest "${DEPLOY_DIR}/selftest/modbus.jsonl.template" SELFTEST-1-M 1790000000.123456 1790000000.127456 > "$tmp/modbus_detailed.jsonl"
render_selftest "${DEPLOY_DIR}/selftest/s7comm.jsonl.template" SELFTEST-1-S 1790000000.123456 1790000000.127456 > "$tmp/s7comm.jsonl"
assert_eq 0 "$(grep -c '__' "$tmp/modbus_detailed.jsonl" "$tmp/s7comm.jsonl" | awk -F: '{s += $2} END {print s}')" "no placeholder left"
assert_eq "SELFTEST-1-M SELFTEST-1-M" "$(jq -r .uid "$tmp/modbus_detailed.jsonl" | tr '\n' ' ' | sed 's/ $//')" "modbus uid"
assert_eq true "$(jq -s '.[1].ts > .[0].ts' "$tmp/s7comm.jsonl")" "response after request"

# The production parsers accept all four records.
jar="$(ls "${REPO_ROOT}"/modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar | head -n 1)"
out="$(java -cp "$jar" io.netsecml.platform.bootstrap.online.ZeekRecordCheck \
  --modbus "$tmp/modbus_detailed.jsonl" --s7comm "$tmp/s7comm.jsonl")"; status=$?
assert_eq 0 "$status" "ZeekRecordCheck passes the selftest records"
assert_eq 1 "$(grep -c '2 records, 2 accepted, 0 rejected' <<< "$(grep '^modbus' <<< "$out")")" "both modbus accepted"
assert_eq 1 "$(grep -c '2 records, 2 accepted, 0 rejected' <<< "$(grep '^s7comm' <<< "$out")")" "both s7comm accepted"

# A trace that does not match its pin is refused.
mkdir -p "$tmp/traces"; printf 'not a pcap' > "$tmp/traces/modbus_example.pcap"
out="$( (fetch_traces "$tmp/traces") 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'does not match its pinned SHA-256' <<< "$out")" "tampered trace refused"

finish
```

- [ ] **Step 3: Run it to verify it fails**

Run: `bash deploy/tests/test_selftest.sh`
Expected: `now_epoch: command not found` (the stub), failures, exit 1.

- [ ] **Step 4: Write selftest.sh**

Replace `deploy/lib/selftest.sh` with:

```bash
#!/usr/bin/env bash
# deploy.sh selftest and zeek-check (design §8, ruling P8).

# --- selftest ---

# Fill a selftest template: __UID__, __TS_REQ__, __TS_RESP__ (epoch seconds).
render_selftest() {
  sed -e "s/__UID__/$2/g" -e "s/__TS_REQ__/$3/g" -e "s/__TS_RESP__/$4/g" "$1"
}

# Epoch seconds with microseconds, like Zeek's own timestamps.
now_epoch() { date +%s.%6N; }

# TS plus MS milliseconds, to the microsecond.
plus_ms() { awk -v t="$1" -v ms="$2" 'BEGIN { printf "%.6f\n", t + ms / 1000 }'; }

# Remove every row a selftest run left in ClickHouse.
selftest_cleanup() {
  ch_query "ALTER TABLE feature_vectors DELETE WHERE connection_uid IN ('$2', '$3')" mutations_sync=1 >/dev/null \
    || warn "could not delete the selftest's feature rows"
  ch_query "ALTER TABLE invalid_events DELETE WHERE event_id LIKE '%$1%'" mutations_sync=1 >/dev/null \
    || warn "could not delete the selftest's DLQ rows"
}

# One Modbus and one S7comm request/response pair, in exactly the JSON Zeek
# writes, through Kafka, both jobs and ClickHouse. Documentation-only
# addresses and SELFTEST- uids keep it apart from real devices; its rows are
# deleted afterwards.
selftest_run() {
  load_env
  local run uid_m uid_s ts_req ts_resp work got dlq deadline
  run="SELFTEST-$(date -u +%Y%m%d%H%M%S)"
  uid_m="${run}-M"
  uid_s="${run}-S"
  ts_req="$(now_epoch)"
  ts_resp="$(plus_ms "$ts_req" 4)"

  # Render and publish the two pairs to the raw topics.
  work="$(mktemp -d)"
  render_selftest "${DEPLOY_DIR}/selftest/modbus.jsonl.template" "$uid_m" "$ts_req" "$ts_resp" > "${work}/modbus.jsonl"
  render_selftest "${DEPLOY_DIR}/selftest/s7comm.jsonl.template" "$uid_s" "$ts_req" "$ts_resp" > "${work}/s7comm.jsonl"
  log "selftest ${run}: publishing one Modbus and one S7comm request/response pair"
  compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic "$MODBUS_RAW_TOPIC" < "${work}/modbus.jsonl"
  compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic "$S7COMM_RAW_TOPIC" < "${work}/s7comm.jsonl"
  rm -rf "$work"

  # The archive job writes on each 30 s checkpoint: allow a few of them.
  deadline=$(( $(date +%s) + 180 ))
  while :; do
    got="$(ch_query "SELECT countIf(log_type = 'modbus'), countIf(log_type = 's7comm') FROM feature_vectors WHERE connection_uid IN ('${uid_m}', '${uid_s}')" || true)"
    dlq="$(ch_query "SELECT count() FROM invalid_events WHERE event_id LIKE '%${run}%'" || true)"
    if [ "$got" = "$(printf '2\t2')" ] && [ "$dlq" = 0 ]; then
      break
    fi
    if [ "${dlq:-0}" != 0 ] || [ "$(date +%s)" -ge "$deadline" ]; then
      warn "selftest FAILED: feature vectors (modbus, s7comm) = '${got}', want 2 and 2; DLQ rows = '${dlq}', want 0"
      ch_query "SELECT log_type, reason_code, detail FROM invalid_events WHERE event_id LIKE '%${run}%' FORMAT PrettyCompactMonoBlock" >&2 || true
      selftest_cleanup "$run" "$uid_m" "$uid_s"
      return 1
    fi
    sleep 5
  done
  selftest_cleanup "$run" "$uid_m" "$uid_s"
  log "selftest PASSED: 2 Modbus and 2 S7comm feature vectors reached ClickHouse, no DLQ rows (test rows removed)"
}

# --- zeek-check ---

# The newest N records of a single-partition topic, one JSON object per line.
live_records() {
  local topic="$1" n="$2" end start
  end="$(compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic "$topic" </dev/null | sum_offsets)"
  [ "$end" -gt 0 ] || return 0
  start=$(( end > n ? end - n : 0 ))
  compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic "$topic" \
    --partition 0 --offset "$start" --max-messages $(( end - start )) --timeout-ms 20000 </dev/null 2>/dev/null
}

# ZeekRecordCheck from the online JAR, on the Flink image's Java 21, over
# DIR/modbus_detailed.jsonl and DIR/s7comm.jsonl. Its exit status is ours.
run_record_check() {
  docker run --rm --entrypoint java -v "${DEPLOY_DIR}/jars:/jars:ro" -v "$1:/in:ro" "$FLINK_IMAGE" \
    -cp /jars/online-feature-job.jar io.netsecml.platform.bootstrap.online.ZeekRecordCheck \
    --modbus /in/modbus_detailed.jsonl --s7comm /in/s7comm.jsonl
}

# deploy.sh zeek-check [--live N]: do the sensor's records fit our parsers?
# Offline: our Zeek image over ICSNPP's own sample traces. --live N: the
# newest N records on each raw topic, i.e. what this sensor really wrote.
zeek_check_run() {
  load_env
  [ -f "${DEPLOY_DIR}/jars/online-feature-job.jar" ] || die "job JARs missing: run 'deploy.sh build' first"
  local work="${DEPLOY_DIR}/.cache/zeek-check"
  rm -rf "$work"
  mkdir -p "$work"
  if [ "${1:-}" = --live ]; then
    local n="${2:-500}"
    log "zeek-check: the newest ${n} records on each raw topic"
    live_records "$MODBUS_RAW_TOPIC" "$n" > "${work}/modbus_detailed.jsonl"
    live_records "$S7COMM_RAW_TOPIC" "$n" > "${work}/s7comm.jsonl"
  else
    docker image inspect "$ZEEK_IMAGE" >/dev/null 2>&1 || die "Zeek image ${ZEEK_IMAGE} missing: run 'deploy.sh build' first"
    log "zeek-check: ${ZEEK_IMAGE} over ICSNPP's own sample traces"
    fetch_traces "${DEPLOY_DIR}/.cache/traces"
    zeek_offline_records "$ZEEK_IMAGE" "${DEPLOY_DIR}/.cache/traces" "$work"
  fi
  run_record_check "$work"
}
```

- [ ] **Step 5: Run the test**

Run: `bash deploy/tests/test_selftest.sh`
Expected: `test_selftest.sh: 9 checks, 0 failed`.

- [ ] **Step 6: Run the offline half of zeek-check for real (seconds; no stack)**

Run: `ENV_FILE="$(mktemp)" bash -c 'cp deploy/.env.template "$ENV_FILE"; ./deploy/deploy.sh zeek-check' 2>&1 | tail -6`
Expected: `modbus (/in/modbus_detailed.jsonl): 48 records, 45 accepted, 3 rejected (3 known upstream parity, 0 unexpected)`, `s7comm (/in/s7comm.jsonl): 84 records, 84 accepted, 0 rejected …`, `RESULT: PASS`. (It uses a throwaway env file, runs Zeek offline and `java`, and starts no service.)

- [ ] **Step 7: Commit**

```bash
git add deploy/lib/selftest.sh deploy/selftest/modbus.jsonl.template deploy/selftest/s7comm.jsonl.template deploy/tests/test_selftest.sh
git commit -m "feat(deploy): selftest and zeek-check"
```

---

### Task 10: Operator guide, CLAUDE.md, the whole check suite

**Files:**
- Create: `deploy/README.md`, `deploy/.shellcheckrc`, `deploy/tests/run-all.sh`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: every earlier task.
- Produces: `bash deploy/tests/run-all.sh` — every check that needs no running stack; exit 0 only if all pass.

- [ ] **Step 1: Write the suite runner and the shellcheck config**

`deploy/.shellcheckrc`:

```
# Variables such as CLICKHOUSE_USER come from deploy/.env at run time
# (load_env), and lib/*.sh are sourced by computed paths shellcheck cannot follow.
disable=SC1090,SC1091,SC2154
```

`deploy/tests/run-all.sh`:

```bash
#!/usr/bin/env bash
# Every deploy/ check that needs no running stack (the development machine
# never runs one): the bash unit tests, the compose definition, the shaded JARs,
# the offline Zeek policy, and shellcheck (in a container).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
status=0

# The unit tests, one file at a time; a failing file fails the run.
for test in test_common.sh test_tune.sh test_compose.sh test_submit_jobs.sh \
            test_install_doctor.sh test_stack.sh test_selftest.sh check-jars.sh test_zeek_policy.sh; do
  bash "${HERE}/${test}" || status=1
done

# shellcheck, pinned, over every script; the tool is not assumed on the host.
docker run --rm -v "${HERE}/..:/mnt:ro" -w /mnt koalaman/shellcheck:v0.10.0 \
  deploy.sh lib/*.sh flink/submit-jobs.sh zeek/run-zeek.sh tests/*.sh || status=1

[ "$status" -eq 0 ] && echo "run-all: every check passed" || echo "run-all: FAILURES above"
exit "$status"
```

- [ ] **Step 2: Run the whole suite; fix what shellcheck reports**

Run: `bash deploy/tests/run-all.sh`
Expected: every test file `0 failed`; shellcheck exit 0; `run-all: every check passed`. For each shellcheck finding, fix the script (not the rc file), re-running the file's own test after each fix.

- [ ] **Step 3: Write the operator guide**

`deploy/README.md`:

````markdown
# netsec-ml on a server

One script runs the whole Modbus + S7comm pipeline on one Linux server:
Zeek sniffs the OT interface → Kafka → the online Flink job (42-value Modbus and
16-value S7comm feature vectors, bad records to a DLQ) → the archive job →
ClickHouse. It is its own Docker Compose project (`netsec-ml`) and never touches
any other container on the host. Design:
`docs/superpowers/specs/2026-09-24-server-deployment-design.md`.

No attack scoring yet: the Modbus and S7 models are not delivered. The
`predictions` table exists and stays empty until they are.

## First install

```sh
git clone <this repository> && cd ml-platform
./deploy/deploy.sh doctor                     # read-only host check; lists interfaces
./deploy/deploy.sh install --interface eth1   # Docker/curl/jq if missing, deploy/.env, sizing
./deploy/deploy.sh build                      # job JARs + Zeek image (~10 min the first time)
./deploy/deploy.sh up                         # everything, in order; ends with 'status'
./deploy/deploy.sh selftest                   # one Modbus + one S7 pair end to end
./deploy/deploy.sh zeek-check                 # our Zeek vs the parsers, on ICSNPP samples
```

Once real traffic flows: `./deploy/deploy.sh zeek-check --live 500` checks the
sensor's own newest records against the parsers, and `status` shows rows per
protocol. `RESULT: PASS` may list "known upstream parity" Modbus rejections —
function names the frozen model's own code cannot resolve (exception PDUs and
Zeek's `ENCAP_INTERFACE_TRANSPORT`); anything `UNEXPECTED` is a finding to report.

## Day to day

| Task | Command |
|---|---|
| Health, jobs, traffic, recent rows | `./deploy/deploy.sh status` |
| Logs | `./deploy/deploy.sh logs zeek` (or kafka, clickhouse, flink-jobmanager, flink-taskmanager, job-submitter) |
| Query | `./deploy/deploy.sh sql "SELECT log_type, count() FROM feature_vectors GROUP BY log_type"` |
| Stop (state saved) / start | `./deploy/deploy.sh down` / `./deploy/deploy.sh up` |
| Upgrade after `git pull` | `./deploy/deploy.sh build && ./deploy/deploy.sh restart` |
| Re-size after a hardware change | `./deploy/deploy.sh tune && ./deploy/deploy.sh restart` |
| Remove (data kept) / everything | `./deploy/deploy.sh uninstall` / `uninstall --purge` |

The Flink UI and ClickHouse listen on `127.0.0.1` only. From your desk:
`ssh -L 18081:127.0.0.1:18081 -L 18123:127.0.0.1:18123 you@server`, then open
http://localhost:18081.

## What to know

- **Sizing.** `tune` reads the CPU count and the RAM free right now (so what
  other containers use is already excluded), keeps a reserve for the host, and
  splits the rest: Flink TaskManager 40%, ClickHouse 25%, Kafka 15%, Zeek 10%,
  JobManager a fixed 1 GiB. It refuses below 4.25 GiB (`--force` overrides);
  `--memory-budget 12g` and `--cpus 4` set the budget yourself.
- **State survives restarts.** `down` stops each job with a savepoint; `up`, a
  reboot or a JobManager restart resumes each job from its newest savepoint or
  checkpoint (the `job-submitter` container does this every minute).
- **If a job keeps failing to start after an upgrade**, its saved state no longer
  fits the new code. `logs job-submitter` says so; move that job's folders under
  `deploy/data/flink/savepoints/` and `deploy/data/flink/checkpoints/` aside and
  it starts fresh.
- **Zeek packages are pinned.** icsnpp-modbus stays at v1.0.0: v2.0.0 changed
  `modbus_detailed` to one record per request/response pair, which the frozen
  Modbus model cannot read.
- **Every topic must exist**; `up` creates all twelve, including the empty conn
  and dns ones both jobs subscribe to.
- **Single node, no HA.** One Kafka broker, one ClickHouse, one TaskManager.
- Data lives in `deploy/data/`; settings and the generated ClickHouse password
  in `deploy/.env` (mode 600, never committed).
````

- [ ] **Step 4: Update CLAUDE.md**

1. Under `## Commands`, after the Python block, add:

```markdown
### Deployment (`deploy/`)
```sh
./deploy/deploy.sh help              # every command
bash deploy/tests/run-all.sh         # every deploy check that needs no running stack
```

The stack (`deploy.sh up`, `selftest`, `zeek-check --live`) runs on the server
only: the development machine does not have the hardware, so never start it
here. `deploy/README.md` is the operator guide.
```

2. In `## Implementation state`, after the S7comm deliverables list, add a paragraph:

```markdown
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
```

3. In `### Verification state`, add a row to the no-container table:

```markdown
| `bootstrap-online-job`, `ZeekRecordCheckTest` only (filtered) | 8/8, 0 skipped (real ICSNPP output: 84/84 s7comm accepted; modbus 45/48, the 3 rejections upstream's own engine makes) |
| `deploy/tests/run-all.sh` | every file 0 failed, shellcheck clean (no stack started: builds, stubs, `compose config`, offline `zeek -r`) |
```

and after the tables:

```markdown
**Not yet verified: the deployed stack itself.** `deploy.sh up`, `selftest`,
`zeek-check --live` and a `down`/`up` restore have not run anywhere: the
development machine cannot hold the stack. The first run on the server is that
verification.
```

4. In `**Modbus limits and decisions**`, extend the F4 bullet's first sentence with: `Zeek itself names function 43 ENCAP_INTERFACE_TRANSPORT, while upstream's FUNCTION_NAME_TO_CODE spells it ENCAPSULATED_INTERFACE_TRANSPORT, so real FC-43 records are DLQ'd too -- by upstream's engine as well (measured on ICSNPP's own sample trace).` and add a bullet:

```markdown
- **The sensor must run icsnpp-modbus v1.0.0.** v2.0.0 (2025-09-03) writes
  `modbus_detailed` as one record per request/response pair (`matched`,
  `request_values`, `response_values`; no `is_orig`, no `request_response`, one
  `ts`). Such a record is rejected (`direction is required`), never misread
  (`ZeekRecordCheckTest`). The deployment's Zeek image pins v1.0.0.
```

- [ ] **Step 5: Final verification**

Run, one at a time, reading each `Tests run:` line:

```bash
./mvnw -q install -DskipTests
rm -rf modules/bootstrap-online-job/target/surefire-reports
./mvnw -o test -pl modules/bootstrap-online-job -Dtest='ZeekRecordCheckTest,OnlineFeatureJobTopologyTest' 2>&1 | grep "Tests run:" | tail -1
rm -rf modules/bootstrap-archive-job/target/surefire-reports
./mvnw -o test -pl modules/bootstrap-archive-job -Dtest=ArchiveJobTopologyTest 2>&1 | grep "Tests run:" | tail -1
bash deploy/tests/run-all.sh
git status --short | grep -v '^??'
```

Expected: `Tests run: 16, Failures: 0, Errors: 0, Skipped: 0` (8 + 8); `Tests run: 10, …`; `run-all: every check passed`; only this task's files modified. No `docker ps --filter label=com.docker.compose.project=netsec-ml` container exists (`docker ps -q --filter label=com.docker.compose.project=netsec-ml | wc -l` → `0`).

- [ ] **Step 6: Commit**

```bash
git add deploy/README.md deploy/.shellcheckrc deploy/tests/run-all.sh CLAUDE.md
git commit -m "docs(deploy): operator guide, CLAUDE.md, and the whole deploy check suite"
```

---

## Self-Review

**Spec coverage.** §1 goal → Tasks 7–9 (`install`, `build`, `up`, `selftest`, `status`, `down`/`up`). §2 D1–D7 → Global Constraints, Tasks 2, 5, 9. §3 services/ports/networking/data → Task 5 (P3, P6). §4 Zeek pins and config → Task 2 (P7). §5 topics → Task 5 `topics.conf`, Task 8 `create_topics`. §6 packaging → Task 1; cluster config, submitter, resume → Tasks 5–6 (P9); `down` savepoints → Task 8. §7 tuning → Task 4 (P4, P5, P10). §8 commands → Tasks 7–9, plus `zeek-check` (P8). §9 security → Task 5 (loopback, generated password in Task 7, mode 600), Zeek's two capabilities. §10 verification → Task 1 (JAR contents), Tasks 2/3/9 (real-Zeek parity, P2), Task 10 (shellcheck, compose config); §10.2 moved to the server (P1). §11/§12 → README and CLAUDE.md.

**Placeholders.** None: every script, config and class is written out in full; test counts are stated where fixed.

**Type consistency.** `tune_compute` keys (Task 4) = the names docker-compose.yml interpolates (Task 5) = test_compose.sh's expectations. JAR names `online-feature-job.jar`/`archive-job.jar` agree across build.sh (Task 7), the compose mounts (Task 5), submit-jobs.sh (Task 6) and run_record_check (Task 9). `ZeekRecordCheck` CLI flags and exit codes (Task 3) = run_record_check and test_selftest.sh (Task 9). Fixture file names (Task 2) = ZeekRecordCheckTest (Task 3). `list_interfaces` (Task 7) is what `stack_preflight` (Task 8) calls.

**Review Focus.** All five lines have a test in their owning task: 1 → Task 6, 2 → Task 8, 3 → Task 8 (+ mutation check), 4 → Task 7, 5 → Task 4.
