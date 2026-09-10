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
