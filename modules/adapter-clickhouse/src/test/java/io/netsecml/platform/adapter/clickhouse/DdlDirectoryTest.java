package io.netsecml.platform.adapter.clickhouse;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The DDL directory is applied as an ordered sequence of migrations, not as one
// file. These tests need no Docker: they check how the directory is read, which
// is what every container test then depends on.
class DdlDirectoryTest {

    // The known-good SHA-256 of 001_mvp_tables.sql, computed with
    // `sha256sum infrastructure/clickhouse/ddl/001_mvp_tables.sql`. The file is
    // immutable -- pinning its content hash, mirroring
    // ConnFeatureSchemaV1Test's technique for contracts, means ANY edit to it
    // (not just the specific "log_type LowCardinality(String) DEFAULT" spelling a
    // literal string match would miss) fails this test. If this assertion fails
    // because you intentionally changed the file, that is the mistake being
    // caught: the fix belongs in a new 00N_ migration, not in 001_.
    private static final String BASE_SCHEMA_SHA256 =
        "e695fed6024f577df03068c53dabcef89cb3509f4c5a8e572a5b0235d0107fa2";

    // A migration that runs before the CREATE TABLE it alters would fail against a
    // real server, so the order is load-bearing rather than cosmetic.
    @Test
    void ddlFilesAreReturnedInLexicalOrder() throws Exception {
        List<Path> files = ClickHouseTestSupport.ddlFiles();

        assertFalse(files.isEmpty(), "the DDL directory must not be empty");

        List<String> names = files.stream().map(p -> p.getFileName().toString()).toList();
        // Docker-free ordering guard: the expected sequence is built independently
        // of ddlFiles()'s own sort, by listing today's two files literally, so this
        // cannot pass by re-sorting the implementation's own output and comparing
        // it to itself. DdlMigrationTest covers the runtime consequence of a wrong
        // order (a migration failing against a real server).
        assertEquals(List.of("001_mvp_tables.sql", "002_add_invalid_events_log_type.sql"), names,
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

    // The migration must never edit the frozen base file. A content-hash pin
    // catches every edit, not just one specific column spelling: 001_mvp_tables.sql
    // pads column names to a 16-character column, so a hand-written edit adding
    // log_type there would likely be spelled "log_type         LowCardinality(String)"
    // -- which a literal substring check for "log_type LowCardinality(String) DEFAULT"
    // (single space) would silently miss. The hash cannot miss any edit.
    @Test
    void theBaseSchemaDoesNotDeclareTheMigratedColumn() throws Exception {
        byte[] bytes = Files.readAllBytes(
            ClickHouseTestSupport.repoPath("infrastructure", "clickhouse", "ddl", "001_mvp_tables.sql"));
        String hex = sha256Hex(bytes);

        assertEquals(BASE_SCHEMA_SHA256, hex,
            "001_mvp_tables.sql is immutable; the log_type column (or any other change) "
            + "belongs in a new 00N_ migration, not in this file. If this file was "
            + "intentionally edited, that edit is the mistake this test exists to catch.");
    }

    // SHA-256 of the given bytes as lowercase hex, the same technique
    // ConnFeatureSchemaV1Test uses to pin a contract file's content hash.
    private static String sha256Hex(byte[] bytes) throws NoSuchAlgorithmException {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
