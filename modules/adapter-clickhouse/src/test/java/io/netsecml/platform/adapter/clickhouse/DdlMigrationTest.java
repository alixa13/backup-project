package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// Applying the DDL against a real server is the only thing that verifies it.
// These tests use the SAME file scripts/database/apply-ddl.sh applies — there is
// no test-only copy of the schema to drift.
//
// The container, database and DDL plumbing itself lives in ClickHouseTestSupport
// now that ClientV2InserterTest needs the same pieces — this class only supplies
// the container instance and the three DDL-specific test bodies.
@Testcontainers(disabledWithoutDocker = true)
class DdlMigrationTest {
    // Exactly what 001_mvp_tables.sql must create.
    private static final Set<String> EXPECTED_TABLES = Set.of(
        "feature_vectors", "invalid_events", "predictions", "network_events", "model_releases");

    // A bare ClickHouse container, built by the shared helper so every container
    // test in this module configures it identically. One container for the whole
    // class; each test isolates itself in its own database instead.
    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // Fresh database, single application: every table the DDL defines must exist
    // afterward, and nothing else.
    @Test
    void createsAllFiveMvpTables() throws Exception {
        ClickHouseTestSupport.createDatabase(CLICKHOUSE, "ddl_fresh");
        try (Client client = ClickHouseTestSupport.clientFor(CLICKHOUSE, "ddl_fresh")) {
            ClickHouseTestSupport.applyDdl(client);
            assertEquals(EXPECTED_TABLES, ClickHouseTestSupport.tableNames(client));
        }
    }

    // A migration runner that cannot be run twice is a migration runner nobody
    // dares run once.
    @Test
    void reapplyingTheDdlIsANoOp() throws Exception {
        ClickHouseTestSupport.createDatabase(CLICKHOUSE, "ddl_twice");
        try (Client client = ClickHouseTestSupport.clientFor(CLICKHOUSE, "ddl_twice")) {
            ClickHouseTestSupport.applyDdl(client);
            ClickHouseTestSupport.applyDdl(client);
            assertEquals(EXPECTED_TABLES, ClickHouseTestSupport.tableNames(client));
        }
    }

    // Exercises the actual shipped script, not a Java reimplementation of it.
    // Skipped off Linux, where bash and curl may be absent.
    @Test
    @EnabledOnOs(OS.LINUX)
    void applyDdlScriptCreatesTheSameTables() throws Exception {
        String database = "ddl_via_script";

        ProcessBuilder builder = new ProcessBuilder(
            "bash", ClickHouseTestSupport.repoPath("scripts", "database", "apply-ddl.sh").toAbsolutePath().toString());
        builder.environment().put("CLICKHOUSE_HOST", CLICKHOUSE.getHost());
        builder.environment().put("CLICKHOUSE_PORT", String.valueOf(CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT)));
        builder.environment().put("CLICKHOUSE_DATABASE", database);
        builder.environment().put("CLICKHOUSE_USER", "default");
        builder.environment().put("CLICKHOUSE_PASSWORD", "");
        builder.redirectErrorStream(true);

        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), "apply-ddl.sh exited non-zero:\n" + output);

        try (Client client = ClickHouseTestSupport.clientFor(CLICKHOUSE, database)) {
            assertEquals(EXPECTED_TABLES, ClickHouseTestSupport.tableNames(client), "script output:\n" + output);
        }
    }
}
