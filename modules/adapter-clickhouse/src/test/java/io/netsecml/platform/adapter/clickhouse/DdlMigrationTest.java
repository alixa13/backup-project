package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

// Applying the DDL against a real server is the only thing that verifies it.
// These tests use the SAME file scripts/database/apply-ddl.sh applies — there is
// no test-only copy of the schema to drift.
@Testcontainers(disabledWithoutDocker = true)
class DdlMigrationTest {
    // Exactly what 001_mvp_tables.sql must create.
    private static final Set<String> EXPECTED_TABLES = Set.of(
        "feature_vectors", "invalid_events", "predictions", "network_events", "model_releases");

    // A bare ClickHouse container, deliberately GenericContainer rather than
    // testcontainers' ClickHouseContainer: that one extends JdbcDatabaseContainer
    // and waits for readiness through a JDBC driver this project does not ship.
    // One container for the whole class; each test isolates itself in its own
    // database instead.
    @Container
    private static final GenericContainer<?> CLICKHOUSE =
        new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:25.8"))
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));

    // This module lives at modules/adapter-clickhouse, so the repo root is two up.
    private static Path repoPath(String... parts) {
        return Paths.get("..", "..", String.join("/", parts));
    }

    // Builds a client bound to one database on the container's mapped port. Each
    // test gets its own database, so clients are not shared across tests.
    private static Client clientFor(String database) {
        return new Client.Builder()
            .addEndpoint("http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123))
            .setUsername("default")
            .setPassword("")
            .setDefaultDatabase(database)
            .build();
    }

    // Creates the given database via a bootstrap client connected to ClickHouse's
    // own built-in "default" database, mirroring what apply-ddl.sh does before
    // it applies any table DDL.
    private static void createDatabase(String database) throws Exception {
        try (Client bootstrap = clientFor("default")) {
            bootstrap.execute("CREATE DATABASE IF NOT EXISTS " + database).get();
        }
    }

    // Applies the DDL file statement by statement. The HTTP interface takes one
    // statement per request, so line comments are stripped and the remainder is
    // split on ';' — the same treatment apply-ddl.sh gives it.
    private static void applyDdl(Client client) throws Exception {
        String sql = Files.readString(repoPath("infrastructure", "clickhouse", "ddl", "001_mvp_tables.sql"))
            .replaceAll("(?m)--.*$", "");
        for (String statement : sql.split(";")) {
            if (!statement.isBlank()) {
                client.execute(statement).get();
            }
        }
    }

    // Lists every table that exists in the client's default database.
    private static Set<String> tableNames(Client client) {
        return client.queryAll("SHOW TABLES").stream()
            .map(record -> record.getString("name"))
            .collect(Collectors.toSet());
    }

    // Fresh database, single application: every table the DDL defines must exist
    // afterward, and nothing else.
    @Test
    void createsAllFiveMvpTables() throws Exception {
        createDatabase("ddl_fresh");
        try (Client client = clientFor("ddl_fresh")) {
            applyDdl(client);
            assertEquals(EXPECTED_TABLES, tableNames(client));
        }
    }

    // A migration runner that cannot be run twice is a migration runner nobody
    // dares run once.
    @Test
    void reapplyingTheDdlIsANoOp() throws Exception {
        createDatabase("ddl_twice");
        try (Client client = clientFor("ddl_twice")) {
            applyDdl(client);
            applyDdl(client);
            assertEquals(EXPECTED_TABLES, tableNames(client));
        }
    }

    // Exercises the actual shipped script, not a Java reimplementation of it.
    // Skipped off Linux, where bash and curl may be absent.
    @Test
    @EnabledOnOs(OS.LINUX)
    void applyDdlScriptCreatesTheSameTables() throws Exception {
        String database = "ddl_via_script";

        ProcessBuilder builder = new ProcessBuilder(
            "bash", repoPath("scripts", "database", "apply-ddl.sh").toAbsolutePath().toString());
        builder.environment().put("CLICKHOUSE_HOST", CLICKHOUSE.getHost());
        builder.environment().put("CLICKHOUSE_PORT", String.valueOf(CLICKHOUSE.getMappedPort(8123)));
        builder.environment().put("CLICKHOUSE_DATABASE", database);
        builder.environment().put("CLICKHOUSE_USER", "default");
        builder.environment().put("CLICKHOUSE_PASSWORD", "");
        builder.redirectErrorStream(true);

        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), "apply-ddl.sh exited non-zero:\n" + output);

        try (Client client = clientFor(database)) {
            assertEquals(EXPECTED_TABLES, tableNames(client), "script output:\n" + output);
        }
    }
}
