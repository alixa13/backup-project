package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Shared plumbing for every ClickHouse container test in this module.
//
// Container tests here deliberately use GenericContainer rather than
// testcontainers' ClickHouseContainer: that one extends JdbcDatabaseContainer and
// waits for readiness through a JDBC driver this project does not ship.
//
// Public: ClientV2InserterTest lives in the .writer subpackage, and Java
// package-private access does not extend to subpackages, so every member a
// second-package test needs to call has to be public. Only the constructor
// stays private, since this is a non-instantiable static utility class.
//
// This class is also published as this module's test-jar (see pom.xml) so
// bootstrap-archive-job's container tests reuse it instead of keeping a second
// copy of the container, database and DDL plumbing. repoPath()'s "../.."
// resolves correctly from there too: Maven runs a module's tests with that
// module's own directory as the working directory, and every module lives at
// the same modules/<name> depth, so the repo root is always two levels up
// regardless of which module's tests are running.
public final class ClickHouseTestSupport {
    public static final int HTTP_PORT = 8123;

    // clickhouse-server:25.8 refuses an empty password for the default user
    // (error 194, REQUIRED_PASSWORD) unless one is configured, so every container
    // and every client in this module has to agree on one. Tests that build a
    // ClickHouseConfig must pass this rather than "".
    public static final String PASSWORD = "test-password";

    private ClickHouseTestSupport() {
    }

    // Builds one ClickHouse container, configured identically for every test in
    // this module. Callers own its lifecycle — typically a static @Container
    // field, so one container serves the whole test class.
    public static GenericContainer<?> newContainer() {
        return new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:25.8"))
            .withExposedPorts(HTTP_PORT)
            // The image's entrypoint provisions the default user with this password.
            .withEnv("CLICKHOUSE_PASSWORD", PASSWORD)
            .waitingFor(Wait.forHttp("/ping").forPort(HTTP_PORT).forStatusCode(200));
    }

    // This module lives at modules/adapter-clickhouse, so the repo root is two up.
    public static Path repoPath(String... parts) {
        return Paths.get("..", "..", String.join("/", parts));
    }

    // Builds a client bound to one database on the container's mapped port. Each
    // test gets its own database, so clients are not shared across tests.
    public static Client clientFor(GenericContainer<?> container, String database) {
        return new Client.Builder()
            .addEndpoint("http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT))
            .setUsername("default")
            .setPassword(PASSWORD)
            .setDefaultDatabase(database)
            .build();
    }

    // Each test isolates itself in its own database rather than paying for a
    // fresh container.
    public static void createDatabase(GenericContainer<?> container, String database) throws Exception {
        try (Client bootstrap = clientFor(container, "default")) {
            bootstrap.execute("CREATE DATABASE IF NOT EXISTS " + database).get();
        }
    }

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

    // Applies the whole DDL directory in order -- the same files, in the same
    // sequence, that scripts/database/apply-ddl.sh applies in production. Reading
    // only 001_ here would leave every container test running against a schema
    // that is missing whatever later migrations add.
    //
    // The HTTP interface takes one statement per request, so line comments are
    // stripped (a ';' inside a comment would split a statement) and the remainder
    // is executed statement by statement, exactly as apply-ddl.sh does it.
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

    // Convenience: create the database and apply the DDL, returning a bound client.
    public static Client freshDatabase(GenericContainer<?> container, String database) throws Exception {
        createDatabase(container, database);
        Client client = clientFor(container, database);
        applyDdl(client);
        return client;
    }

    // Lists every table that exists in the client's default database.
    public static Set<String> tableNames(Client client) {
        return client.queryAll("SHOW TABLES").stream()
            .map(record -> record.getString("name"))
            .collect(Collectors.toSet());
    }
}
