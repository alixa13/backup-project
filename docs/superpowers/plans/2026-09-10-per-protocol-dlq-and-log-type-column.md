# Per-Protocol DLQ and the `log_type` Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the failure path multi-protocol aware, so "is the S7comm parser rejecting everything?" becomes answerable instead of every rejection landing in one undifferentiated pile.

**Architecture:** The `dlq-v1` contract stays frozen — it carries no protocol field and gains none. Instead each protocol gets its own DLQ topic, and the archive job learns the log type from the **topic binding at wiring time**, the same compile-time binding the input side already uses. `invalid_events` gains a `log_type` column through a new `002_` migration rather than an edit.

**Tech Stack:** Java 21, Flink 2.2.1, ClickHouse client-v2 0.9.0, Jackson 2.17, JUnit 5, Testcontainers 1.21.4.

**Spec:** `docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md` (this plan implements Unit 2 of §12; the requirement is §9)

**Base branch:** `feat/common-feature-tier` — Task 3 modifies `ArchiveJob.LogTypeChain`, which exists only there. Not `main`.

## Global Constraints

- **`contracts/stream/dlq-v1.json` is FROZEN and must not gain a `logType` field.** Its six fields are `eventId, stage, reasonCode, detail, rawPayloadHash, receivedAt`. The whole design of this unit exists so that contract need not change.
- **Log type comes from the topic binding at wiring time — never a runtime string parse of a payload or topic name.** This mirrors the input side's rule and is what keeps `LogType` a compile-time fact.
- **`infrastructure/clickhouse/ddl/001_mvp_tables.sql` is immutable.** The new column arrives in `002_add_invalid_events_log_type.sql`. Never edit `001_`.
- **`contracts/` is immutable** — new files allowed, edits are not.
- **Existing Flink operator uids are checkpoint state identity.** `dlq-source`, `invalid-event-row` and `invalid-events-clickhouse-sink` belong to the conn DLQ chain and **must keep those exact names**. A changed uid makes Flink silently discard state on restore rather than fail.
- **Java package root:** `io.netsecml.platform`. Dependency chain is one-way: `domain → ports → application → adapters → bootstrap`. `domain` imports no framework code.
- **Inline comments describing each block are mandatory** — a standing user requirement. A comment that misdescribes the code is worse than no comment.
- **Build in stages.** `./mvnw clean verify` is OOM-killed on a 5.7 GiB machine. Run `./mvnw install -DskipTests -q -o` once, then one module at a time.
- **Never combine `-am` with `-Dtest=`.** Surefire fails hard on the first upstream module that has tests but none matching the pattern, and the suppression flag is forbidden because it reports BUILD SUCCESS on zero tests. Either run a module's whole suite with `-am`, or run a single module without `-am` after an install.
- **`ClickHouseOutageTest` is OOM-killed on this machine and cannot run.** Report it unverified; never as passing. Clear containers between runs: `docker ps -aq | xargs -r docker rm -f`.
- **Commit with explicit paths only** — never `git add -A`, `git add .`, or a bare directory.
- **Commit trailer:**
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  ```

---

## Two spec corrections this plan carries

Both were found by reading the code before writing tasks, and both change what Unit 2 has to do.

**1. §9 says "`apply-ddl.sh` already iterates the directory". That is only half true.** The shell script does — `for ddl in "${DDL_DIR}"/*.sql` expands in lexical order, so `002_` follows `001_` correctly. But the **Java** loader does not: `ClickHouseTestSupport.applyDdl` hardcodes `001_mvp_tables.sql`. Every container test would therefore run against a table with no `log_type` column, and any insert naming it would fail. The Java loader must be taught to walk the directory too.

**2. `SchemaDriftTest.ddlColumns` also hardcodes `001_mvp_tables.sql`.** It is the Docker-free guard that every `@JsonProperty` on a row record matches a real column. The moment `InvalidEventRow` gains `log_type`, that test fails against `001_` alone — it cannot see a column added by a migration. It must resolve columns across the whole directory, applying `ALTER TABLE ... ADD COLUMN` as well as `CREATE TABLE`.

Task 1 fixes both before anything depends on them.

---

## File Structure

| File | Responsibility |
|---|---|
| `infrastructure/clickhouse/ddl/002_add_invalid_events_log_type.sql` | Adds the `log_type` column. New file; `001_` is never edited. |
| `modules/adapter-clickhouse/src/test/.../ClickHouseTestSupport.java` | `applyDdl` walks the DDL directory in lexical order instead of one hardcoded file. |
| `modules/adapter-clickhouse/src/test/.../SchemaDriftTest.java` | `ddlColumns` resolves columns across `CREATE TABLE` plus `ALTER TABLE ... ADD COLUMN`. |
| `modules/adapter-clickhouse/src/main/.../row/InvalidEventRow.java` | Gains a `log_type` component. |
| `modules/adapter-clickhouse/src/main/.../mapper/InvalidEventRowMapper.java` | Carries the `LogType` supplied at construction into the row. |
| `modules/bootstrap-archive-job/src/main/.../InvalidEventRowMapFunction.java` | Takes the `LogType` bound to its topic. |
| `modules/bootstrap-archive-job/src/main/.../ArchiveJob.java` | Registers one DLQ chain per log type. |

---

## Task 1: Teach both DDL readers to see migrations

Nothing else in this unit can work until the `002_` file is actually read. `SchemaDriftTest` needs no Docker, so this task is verifiable here even though `DdlMigrationTest` is not.

**Files:**
- Create: `infrastructure/clickhouse/ddl/002_add_invalid_events_log_type.sql`
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/ClickHouseTestSupport.java`
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/SchemaDriftTest.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlDirectoryTest.java`

**Interfaces:**
- Consumes: `ClickHouseTestSupport.repoPath(String...)`.
- Produces: `ClickHouseTestSupport.ddlFiles()` returning `List<Path>` in lexical order; `applyDdl(Client)` applying all of them.

- [ ] **Step 1: Write the migration**

`infrastructure/clickhouse/ddl/002_add_invalid_events_log_type.sql`:

```sql
-- Adds the log type to invalid_events.
--
-- The success path has always been protocol-aware: feature_vectors carries
-- log_type and the feature-vector-v1 contract carries logType. The failure path
-- was not. With one protocol that was invisible; with six it means "is the
-- S7comm parser rejecting everything?" cannot be answered, because every
-- rejection lands in one undifferentiated pile.
--
-- The value does NOT come from the message. dlq-v1 is frozen and carries no
-- protocol field, deliberately: the archive job knows the log type from the
-- topic it is reading, bound at wiring time. See the design's section 9.
--
-- LowCardinality(String) to match log_type on feature_vectors, and DEFAULT ''
-- so rows written before this migration remain readable rather than erroring.
ALTER TABLE invalid_events
    ADD COLUMN IF NOT EXISTS log_type LowCardinality(String) DEFAULT '' AFTER received_at;
```

- [ ] **Step 2: Write the failing test**

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlDirectoryTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The DDL directory is applied as an ordered sequence of migrations, not as one
// file. These tests need no Docker: they check how the directory is read, which
// is what every container test then depends on.
class DdlDirectoryTest {

    // A migration that runs before the CREATE TABLE it alters would fail against a
    // real server, so the order is load-bearing rather than cosmetic.
    @Test
    void ddlFilesAreReturnedInLexicalOrder() throws Exception {
        List<Path> files = ClickHouseTestSupport.ddlFiles();

        assertFalse(files.isEmpty(), "the DDL directory must not be empty");

        List<String> names = files.stream().map(p -> p.getFileName().toString()).toList();
        assertEquals(names.stream().sorted().toList(), names,
            "DDL files must be applied in lexical order so 002_ follows 001_");
    }

    // The specific pairing this unit introduces. Stated explicitly because the
    // whole unit is inert if the migration is never read.
    @Test
    void theDirectoryContainsBothTheBaseSchemaAndTheLogTypeMigration() throws Exception {
        List<String> names = ClickHouseTestSupport.ddlFiles().stream()
            .map(p -> p.getFileName().toString()).toList();

        assertTrue(names.contains("001_mvp_tables.sql"), "found: " + names);
        assertTrue(names.contains("002_add_invalid_events_log_type.sql"), "found: " + names);
        assertTrue(names.indexOf("001_mvp_tables.sql") < names.indexOf("002_add_invalid_events_log_type.sql"),
            "the base schema must be applied before the migration that alters it");
    }

    // The migration must never edit the frozen base file. This catches the
    // mistake of "fixing" 001_ instead of adding 002_.
    @Test
    void theBaseSchemaDoesNotDeclareTheMigratedColumn() throws Exception {
        String base = Files.readString(
            ClickHouseTestSupport.repoPath("infrastructure", "clickhouse", "ddl", "001_mvp_tables.sql"));

        assertFalse(base.contains("log_type LowCardinality(String) DEFAULT"),
            "001_mvp_tables.sql is immutable; the log_type column belongs in 002_");
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/adapter-clickhouse -o -Dtest=DdlDirectoryTest`
Expected: FAIL — compilation error, `ClickHouseTestSupport.ddlFiles` does not exist.

- [ ] **Step 4: Add `ddlFiles()` and make `applyDdl` use it**

In `ClickHouseTestSupport`, add:

```java
    // Every .sql file in the DDL directory, in lexical order.
    //
    // The directory is a migration sequence, not a single schema file:
    // 001_ creates the tables and 002_ alters one of them, so applying them out of
    // order would fail against a real server. Lexical order is the ordering
    // contract, which is also what scripts/database/apply-ddl.sh relies on.
    public static List<Path> ddlFiles() throws IOException {
        try (Stream<Path> entries = Files.list(repoPath("infrastructure", "clickhouse", "ddl"))) {
            return entries
                .filter(path -> path.getFileName().toString().endsWith(".sql"))
                .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                .toList();
        }
    }
```

and replace `applyDdl`'s body so it walks that list rather than one hardcoded file:

```java
    // Applies the whole DDL directory in order -- the same files, in the same
    // sequence, that scripts/database/apply-ddl.sh applies in production. Reading
    // only 001_ here would leave every container test running against a schema
    // that is missing whatever later migrations add.
    public static void applyDdl(Client client) throws Exception {
        for (Path file : ddlFiles()) {
            String sql = Files.readString(file).replaceAll("(?m)--.*$", "");
            for (String statement : sql.split(";")) {
                if (!statement.isBlank()) {
                    client.execute(statement).get();
                }
            }
        }
    }
```

Add the imports it needs: `java.io.IOException`, `java.util.Comparator`, `java.util.List`, `java.util.stream.Stream`.

- [ ] **Step 5: Teach `SchemaDriftTest` to resolve columns across migrations**

`SchemaDriftTest.ddlColumns(String table)` currently reads only `001_mvp_tables.sql`, so it cannot see a column a migration adds. Keep its existing `CREATE TABLE` parsing exactly as it is — it handles nested parens in types like `DateTime64(3, 'UTC')` and that logic is correct — but drive it from the base file and then apply any `ALTER TABLE <table> ADD COLUMN` found in later files:

```java
    // Columns are resolved across the whole migration sequence, not from the base
    // file alone. A column added by 002_ is just as real as one declared in 001_,
    // and a drift check that could not see it would fail the moment a row record
    // caught up with the schema.
    private Set<String> ddlColumns(String table) throws Exception {
        Set<String> columns = new LinkedHashSet<>();
        for (Path file : ClickHouseTestSupport.ddlFiles()) {
            String ddl = Files.readString(file).replaceAll("(?m)--.*$", "");
            columns.addAll(createTableColumns(ddl, table));
            columns.addAll(addedColumns(ddl, table));
        }
        assertFalse(columns.isEmpty(), "no columns found for DDL table " + table);
        return columns;
    }

    // ALTER TABLE <table> ADD COLUMN [IF NOT EXISTS] <name> ... -- the column name
    // is the first identifier after the optional IF NOT EXISTS.
    private Set<String> addedColumns(String ddl, String table) {
        Set<String> added = new LinkedHashSet<>();
        Matcher matcher = Pattern.compile(
            "ALTER\\s+TABLE\\s+" + Pattern.quote(table)
                + "\\s+ADD\\s+COLUMN\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([A-Za-z_][A-Za-z0-9_]*)",
            Pattern.CASE_INSENSITIVE).matcher(ddl);
        while (matcher.find()) {
            added.add(matcher.group(1));
        }
        return added;
    }
```

Rename the existing parsing method to `createTableColumns(String ddl, String table)`, taking the DDL text as a parameter instead of reading the file itself, and returning an empty set when the marker is absent rather than calling `fail` — a migration file legitimately contains no `CREATE TABLE` for the table being asked about.

Add imports: `java.nio.file.Path`, `java.util.LinkedHashSet`, `java.util.regex.Matcher`, `java.util.regex.Pattern`.

- [ ] **Step 6: Run both tests**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/adapter-clickhouse -o -Dtest='DdlDirectoryTest'`
Expected: PASS, 3 tests run.

Run: `./mvnw test -pl modules/adapter-clickhouse -o -Dtest='SchemaDriftTest'`
Expected: PASS, 2 tests run — still green, because no row record has gained a column yet.

- [ ] **Step 7: Commit**

```bash
git add infrastructure/clickhouse/ddl/002_add_invalid_events_log_type.sql \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/ClickHouseTestSupport.java \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/SchemaDriftTest.java \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlDirectoryTest.java
git commit -m "feat(infrastructure): add the invalid_events log_type migration and read the DDL directory"
```

---

## Task 2: Carry the log type into the row

**Files:**
- Modify: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/InvalidEventRow.java`
- Modify: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapper.java`
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/writer/ClientV2InserterTest.java:88` — also constructs the mapper
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapperTest.java`

**Every caller of `new InvalidEventRowMapper()` must be updated** — there are three: `InvalidEventRowMapperTest:16`, `ClientV2InserterTest:88`, and `InvalidEventRowMapFunction:30` (that last one belongs to Task 3). Pass `LogType.CONN` in the two tests; conn is the only log type that exists.

**Interfaces:**
- Consumes: `io.netsecml.platform.domain.event.LogType` and its `wireName()`; `RejectedEvent`.
- Produces: `InvalidEventRow` with a ninth component `@JsonProperty("log_type") String logType`; `new InvalidEventRowMapper(LogType)` and its existing `toRow(RejectedEvent)`.

- [ ] **Step 1: Write the failing test**

Add to `InvalidEventRowMapperTest`:

```java
    // The log type is supplied when the mapper is constructed, not read from the
    // event. dlq-v1 is frozen and carries no protocol field: the archive job knows
    // the log type from the topic it is reading, bound at wiring time.
    @Test
    void carriesTheConstructedLogTypeOntoTheRow() {
        RejectedEvent event = new RejectedEvent("sensor-eu-1:abc", "f".repeat(64),
            ReasonCode.MALFORMED_JSON, "unparseable", Instant.parse("2026-09-10T10:00:00Z"));

        InvalidEventRow row = new InvalidEventRowMapper(LogType.CONN).toRow(event);

        assertEquals("conn", row.logType(),
            "the row must carry the wire form, matching feature_vectors.log_type");
    }

    // The wire form is lowercase, the same value the success path writes. A row
    // reading "CONN" would be silently accepted by LowCardinality(String) and
    // would not join against feature_vectors.
    @Test
    void usesTheWireFormNotTheEnumName() {
        RejectedEvent event = new RejectedEvent("sensor-eu-1:abc", "f".repeat(64),
            ReasonCode.MALFORMED_JSON, "unparseable", Instant.parse("2026-09-10T10:00:00Z"));

        InvalidEventRow row = new InvalidEventRowMapper(LogType.CONN).toRow(event);

        assertNotEquals(LogType.CONN.name(), row.logType());
        assertEquals(LogType.CONN.wireName(), row.logType());
    }

    // A mapper without a log type cannot produce a diagnosable row, which is this
    // unit's whole purpose, so it is a construction error rather than a default.
    @Test
    void rejectsANullLogType() {
        assertThrows(IllegalArgumentException.class, () -> new InvalidEventRowMapper(null));
    }
```

Add the imports the test needs: `io.netsecml.platform.domain.event.LogType`.

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/adapter-clickhouse -o -Dtest=InvalidEventRowMapperTest`
Expected: FAIL — no constructor `InvalidEventRowMapper(LogType)`, no accessor `logType()`.

- [ ] **Step 3: Add the component and the constructor**

In `InvalidEventRow`, add as the **last** component so no existing position shifts:

```java
    @JsonProperty("log_type") String logType) {
```

In `InvalidEventRowMapper`, hold the log type and write it:

```java
    // Supplied at construction from the topic binding, never read from the event:
    // dlq-v1 is frozen and carries no protocol field. See the design's section 9.
    private final LogType logType;

    public InvalidEventRowMapper(LogType logType) {
        if (logType == null) {
            throw new IllegalArgumentException("logType must not be null");
        }
        this.logType = logType;
    }
```

and pass `logType.wireName()` as the row's final argument. Use `wireName()`, not `name()`: the column is `LowCardinality(String)` and would silently accept `"CONN"`, which then fails to join against `feature_vectors.log_type`.

**Also address `SOURCE_VERSION`.** It is currently `"zeek-conn-source-v1"` — a conn-specific constant that will be wrong for the other five protocols. Derive it from the log type instead (`"zeek-" + logType.wireName() + "-source-v1"`), and add a test pinning the conn value to `"zeek-conn-source-v1"` so the existing behaviour is unchanged for the only log type that exists today.

- [ ] **Step 4: Run the mapper and drift tests**

Run: `./mvnw test -pl modules/adapter-clickhouse -o -Dtest='InvalidEventRowMapperTest'`
Expected: PASS, 8 tests run — the 4 that already existed plus the 3 above and the `SOURCE_VERSION` one.

Run: `./mvnw test -pl modules/adapter-clickhouse -o -Dtest='SchemaDriftTest'`
Expected: PASS — this is the payoff from Task 1. The new `@JsonProperty("log_type")` resolves against the column added by `002_`, which the drift test can now see. **If this fails, Task 1's migration-aware column resolution is wrong — fix that, not this.**

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/InvalidEventRow.java \
        modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapper.java \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapperTest.java
git commit -m "feat(adapter-clickhouse): carry the bound log type onto invalid_events rows"
```

---

## Task 3: Wire one DLQ chain per log type

**Files:**
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/InvalidEventRowMapFunction.java`
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Test: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java`

**Two existing call sites in that test already construct `InvalidEventRowMapFunction`** (lines 107 and 110) and will not compile once the constructor changes. One of them wires a chain labelled `netsec.http.dlq.v1` with uids `http-dlq-source`, `http-invalid-event-row`, `http-invalid-events-clickhouse-sink`.

**Good news:** those are exactly the strings `dlqChain` will generate for a non-CONN log type, so the factory and that test already agree — do not change the uids.

**The catch:** there is no `LogType.HTTP`. `LogType` has only `CONN`, and its own comment states a constant is added per log type as each one lands, so Java never advertises support with nothing behind it. **Do not add `LogType.HTTP`.** Pass `LogType.CONN` at both call sites and add a comment to the http-named chain saying its names are placeholder labels exercising uid uniqueness, not real protocol support — that test's subject is topology wiring, not log-type semantics. Unit 3 should switch it to a real second log type when DNS lands.

**Interfaces:**
- Consumes: `ArchiveJob.LogTypeChain<T>(String topic, RichMapFunction<byte[],T> rowMapper, String table, String sourceUid, String mapUid, String sinkUid)`; `ArchiveJob.build(env, bootstrapServers, List<LogTypeChain<?>>, clickHouse)`.
- Produces: `new InvalidEventRowMapFunction(String topic, LogType logType)`; `ArchiveJob.dlqChain(LogType, String topic)` returning a `LogTypeChain<InvalidEventRow>`.

- [ ] **Step 1: Write the failing test**

Add to `ArchiveJobTopologyTest`:

```java
    // The conn DLQ chain keeps its historical uids. They are checkpoint state
    // identity: a job restoring from an existing checkpoint looks them up by
    // exactly these strings, so renaming them to match a new per-protocol pattern
    // would silently discard that operator's state instead of failing.
    @Test
    void theConnDlqChainKeepsItsHistoricalUids() {
        Set<String> uids = operatorUids();

        assertTrue(uids.containsAll(Set.of(
            "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            "the conn DLQ chain's original uids must survive, found: " + uids);
    }

    // dlqChain is the single place that knows the conn uids, so this pins that it
    // reproduces them rather than generating the per-protocol pattern for CONN.
    //
    // NOTE the limit of this test: LogType has only CONN today, so dlqChain's
    // non-CONN branch -- the per-protocol uid prefix -- is UNEXERCISED until Unit 3
    // adds DNS. Do not rename this to suggest it covers a second log type; it
    // cannot, and a test whose name overstates its reach is worse than none.
    @Test
    void dlqChainReproducesTheHistoricalConnUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", List.of(
            ArchiveJob.dlqChain(LogType.CONN, "netsec.conn.dlq.v1")),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));

        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });

        assertTrue(uids.contains("dlq-source"),
            "dlqChain(CONN, ...) must reproduce the historical conn uids, found: " + uids);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobTopologyTest`
Expected: FAIL — `ArchiveJob.dlqChain` does not exist.

- [ ] **Step 3: Take the log type in the map function**

`InvalidEventRowMapFunction` currently constructs `new InvalidEventRowMapper()` in `open()`. Give it a `LogType` alongside the topic, keep the field `final` so it serializes with the function, and build the mapper from it:

```java
    // The log type bound to this function's topic at wiring time. An enum is
    // Serializable, so it travels with the function to the TaskManagers; the
    // mapper it configures is still built in open().
    private final LogType logType;

    public InvalidEventRowMapFunction(String topic, LogType logType) {
        if (logType == null) {
            throw new IllegalArgumentException("logType must not be null");
        }
        this.topic = topic;
        this.logType = logType;
    }
```

and in `open()`: `mapper = new InvalidEventRowMapper(logType);`

Leave the existing `KNOWN LIMITATION` comment about poison records exactly as it is — it is still true and is referenced from `docs/clickhouse.md`.

- [ ] **Step 4: Add the chain factory**

In `ArchiveJob`, add a factory so registering a protocol's DLQ is one call rather than six hand-written strings:

```java
    // One DLQ chain for a log type. A factory rather than six literal strings at
    // each call site, because the uids are checkpoint state identity and hand-
    // writing them per protocol is how a typo silently orphans state.
    //
    // CONN keeps the uids it has always had. They predate any per-protocol naming
    // pattern and cannot be regularised: a running job restores state by looking
    // them up verbatim. Every other log type gets the pattern.
    public static LogTypeChain<InvalidEventRow> dlqChain(LogType logType, String topic) {
        String prefix = logType == LogType.CONN ? "" : logType.wireName() + "-";
        String sourceUid = logType == LogType.CONN ? "dlq-source" : prefix + "dlq-source";
        String mapUid = logType == LogType.CONN ? "invalid-event-row" : prefix + "invalid-event-row";
        String sinkUid = logType == LogType.CONN
            ? "invalid-events-clickhouse-sink" : prefix + "invalid-events-clickhouse-sink";

        return new LogTypeChain<>(topic, new InvalidEventRowMapFunction(topic, logType),
            "invalid_events", sourceUid, mapUid, sinkUid);
    }
```

Update the existing 5-argument `build` overload to construct its DLQ chain through `dlqChain(LogType.CONN, dlqTopic)` instead of building the `LogTypeChain` inline, so there is exactly one place that knows the conn uids.

- [ ] **Step 5: Run the topology tests**

Run: `./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobTopologyTest`
Expected: PASS. Confirm the real count and that every pre-existing test in the class still passes — especially `everyOperatorCarriesAnExplicitUid` and `everyStreamNodeCarriesANonNullUid`.

- [ ] **Step 6: Run the archive E2E**

Run: `docker ps -aq | xargs -r docker rm -f; ./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobE2ETest`
Expected: PASS, 1 test. This is the only test that proves a row with the new column actually reaches ClickHouse. If it fails on an unknown column, Task 1's `applyDdl` is not applying `002_`.

`ClickHouseOutageTest` is **not** run here and must not be described as passing.

- [ ] **Step 7: Commit**

```bash
git add modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/InvalidEventRowMapFunction.java \
        modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java \
        modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java
git commit -m "feat(bootstrap-archive-job): bind a log type to each DLQ chain at wiring time"
```

---

## Task 4: Document the failure path

**Files:**
- Modify: `docs/clickhouse.md`

- [ ] **Step 1: Add a failure-path section**

Insert after the common-tier section:

```markdown
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
```

- [ ] **Step 2: Verify every claim against the tree**

Check the file name, the `DEFAULT ''`, the six `dlq-v1` fields, and that `001_` is genuinely unedited. If the prose and the code disagree, the code wins and you say so in your report.

- [ ] **Step 3: Commit**

```bash
git add docs/clickhouse.md
git commit -m "docs: describe the protocol-aware failure path"
```

---

## Self-Review

**Spec coverage.** §9's three requirements map to tasks: the `log_type` column → Task 1; the topic-bound log type reaching the row → Tasks 2 and 3; `dlq-v1` staying frozen → enforced as a Global Constraint and never touched by any task. §12's "small, independent, makes every later unit's failures diagnosable" holds: nothing here depends on a protocol existing.

**Two spec corrections carried.** §9's claim that `apply-ddl.sh` already handles this is only half true — the shell script iterates the directory but `ClickHouseTestSupport.applyDdl` and `SchemaDriftTest.ddlColumns` both hardcode `001_mvp_tables.sql`. Task 1 fixes both before anything depends on them. Without that, Task 2's drift test and Task 3's E2E would both fail for reasons unrelated to their own work.

**Type consistency.** `LogType.wireName()` is used in Tasks 2 and 3 and matches the existing enum. `InvalidEventRow`'s new component is appended last so no existing position shifts. `LogTypeChain`'s six-field shape matches what Unit 1 produced. `ArchiveJob.build(env, bootstrapServers, List<LogTypeChain<?>>, clickHouse)` is the Unit 1 signature, unchanged.

**Known risk carried into Task 3.** `dlqChain` special-cases `CONN` to reproduce three historical uids that do not follow the per-protocol pattern. That asymmetry is deliberate and load-bearing — those strings are checkpoint state identity. A reviewer may reasonably propose a cleaner expression of the special case; a reviewer must not propose removing it.

**A coverage limit stated rather than hidden.** `LogType` has only `CONN` today, so `dlqChain`'s non-`CONN` branch — the per-protocol uid prefix — cannot be exercised by any test in this unit. Unit 3 (DNS) is the first that can. The test is named for what it actually checks, and the limit is written into its comment, so nobody later reads the suite as proving more than it does.

**Scope check.** Four tasks, one module chain, no protocol dependency — this is one plan's worth. The five per-protocol units (§12 units 3-7) each remain their own plan.
