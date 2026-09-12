package io.netsecml.platform.adapter.clickhouse;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import org.junit.jupiter.api.Test;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// This is the executable guard for a seam that otherwise only Docker-backed
// tests check: DdlMigrationTest merely asserts the tables exist, and
// ClientV2InserterTest's "an unknown column must not be silently discarded"
// case only proves anything once input_format_skip_unknown_fields is forced to
// 0 (see ClientV2Inserter) -- and both need a real ClickHouse container, which
// is unavailable in this environment. This test parses the committed DDL
// directory -- every migration file, not just the base schema -- directly with
// plain JUnit, no Testcontainers and no Docker, so it actually runs here and
// catches the exact drift those tests exist to catch: a row
// record's @JsonProperty name that does not match any DDL column, e.g. from a
// rename on one side without the other.
//
// Direction matters: this only asserts row-record -> DDL column, never the
// reverse. A DDL column with no matching @JsonProperty is legitimate --
// archived_at/created_at carry a server-side DEFAULT now64(3) and are
// deliberately never sent by the client (see FeatureVectorRow/InvalidEventRow's
// own comments) -- so requiring every DDL column to have a matching property
// would fail on working, intentional code.
class SchemaDriftTest {

    @Test
    void featureVectorRowPropertiesMatchDdlColumns() throws Exception {
        assertJsonPropertiesAreDdlColumns(FeatureVectorRow.class, "feature_vectors");
    }

    @Test
    void invalidEventRowPropertiesMatchDdlColumns() throws Exception {
        assertJsonPropertiesAreDdlColumns(InvalidEventRow.class, "invalid_events");
    }

    // Reflects over the record's components (cleaner than string-scraping the
    // source) to pull each @JsonProperty name, then checks it against the set of
    // real DDL columns for the given table.
    private void assertJsonPropertiesAreDdlColumns(Class<?> rowType, String table) throws Exception {
        Set<String> columns = ddlColumns(table);

        for (RecordComponent component : rowType.getRecordComponents()) {
            // JsonProperty's own @Target is {ANNOTATION_TYPE, FIELD, METHOD,
            // PARAMETER} -- it does NOT include RECORD_COMPONENT, so javac does not
            // copy an annotation written on a record component onto the
            // RecordComponent reflective object itself. It is copied onto the
            // backing field, which -- unlike the accessor -- a record can never
            // redeclare by hand, so the field is the one place guaranteed to still
            // carry it even for a component like FeatureVectorRow.values(), whose
            // accessor is hand-written (for the defensive copy) and therefore does
            // NOT inherit the annotation.
            JsonProperty jsonProperty = rowType.getDeclaredField(component.getName())
                .getAnnotation(JsonProperty.class);
            assertNotNull(jsonProperty, "backing field for record component " + component.getName()
                + " on " + rowType.getSimpleName() + " has no @JsonProperty");

            String column = jsonProperty.value();
            Supplier<String> message = () -> "@JsonProperty(\"" + column + "\") on "
                + rowType.getSimpleName() + "." + component.getName()
                + " has no matching column in DDL table " + table
                + "; columns present there: " + columns;
            assertTrue(columns.contains(column), message);
        }
    }

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

    // Extracts the column names of one CREATE TABLE statement from a single DDL
    // file's text. Returns an empty set when the marker is absent rather than
    // failing -- a migration file legitimately contains no CREATE TABLE for the
    // table being asked about.
    private Set<String> createTableColumns(String ddl, String table) {
        String marker = "CREATE TABLE IF NOT EXISTS " + table + " (";
        int markerStart = ddl.indexOf(marker);
        if (markerStart < 0) {
            return Set.of();
        }

        // bodyStart sits just past the column list's opening '(', which is
        // already counted below as depth 1.
        int bodyStart = markerStart + marker.length();

        // Column type definitions themselves contain parens and commas --
        // DateTime64(3, 'UTC'), Nullable(DateTime64(3, 'UTC')),
        // LowCardinality(String) -- so the column list's closing ')' has to be
        // found by tracking paren depth, not by taking the first ')' seen.
        int depth = 1;
        int i = bodyStart;
        while (depth > 0) {
            char c = ddl.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            i++;
        }
        String body = ddl.substring(bodyStart, i - 1);

        // Split the column list on top-level commas only, for the same reason:
        // a comma inside a type's own parens must not split a column definition.
        List<String> definitions = new ArrayList<>();
        int splitDepth = 0;
        int last = 0;
        for (int j = 0; j < body.length(); j++) {
            char c = body.charAt(j);
            if (c == '(') {
                splitDepth++;
            } else if (c == ')') {
                splitDepth--;
            } else if (c == ',' && splitDepth == 0) {
                definitions.add(body.substring(last, j));
                last = j + 1;
            }
        }
        definitions.add(body.substring(last));

        // Each definition's column name is its first whitespace-delimited token.
        // Strip backticks for identifiers quoted because they collide with SQL
        // syntax, e.g. `values` (VALUES is INSERT syntax).
        Set<String> columns = new HashSet<>();
        for (String definition : definitions) {
            String trimmed = definition.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String name = trimmed.split("\\s+", 2)[0];
            if (name.startsWith("`") && name.endsWith("`") && name.length() > 1) {
                name = name.substring(1, name.length() - 1);
            }
            columns.add(name);
        }
        return columns;
    }
}
