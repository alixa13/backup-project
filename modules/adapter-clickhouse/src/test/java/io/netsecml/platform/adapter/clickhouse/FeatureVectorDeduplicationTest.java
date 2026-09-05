package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

// Roadmap Day 6, test 2: a retried archive write produces an allowed duplicate,
// and the snapshot query still returns one deterministic latest row.
@Testcontainers(disabledWithoutDocker = true)
class FeatureVectorDeduplicationTest {
    private static final String SCHEMA_HASH = "f".repeat(64);
    private static final String OTHER_HASH = "e".repeat(64);

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // The committed query, read from disk. The test must exercise the shipped
    // file verbatim — a copy pasted into the test would prove nothing about what
    // training actually runs.
    private static String dedupQuery() throws Exception {
        return Files.readString(
            ClickHouseTestSupport.repoPath("infrastructure", "clickhouse", "queries", "feature-vector-dedup.sql"));
    }

    // Reads the first feature value. getFloatArray() cannot be used here: the
    // client throws "Array is not of primitive type" for a value produced by an
    // aggregate function, because argMax returns a boxed list rather than the
    // primitive array a plain column read would give. getList() is the accessor
    // that works for it. This is a client-library detail, not a query defect --
    // the same query returns the column fine, as filtersToTheRequestedSchemaHash
    // demonstrates.
    private static float firstValue(GenericRecord row) {
        List<Number> values = row.getList("values");
        return values.get(0).floatValue();
    }

    // Inserts one row directly, so the test controls row_version precisely.
    // logType and connectionUid are the multi-protocol envelope columns added to
    // feature_vectors by the schema redesign; neither has a DEFAULT clause, so an
    // INSERT that omitted them would silently get '' instead of failing loudly,
    // and a dedup query that dropped both columns would still pass a test that
    // never gave them a real value to lose. Both are set here so that mistake is
    // observable.
    private void insert(Client client, String eventId, String schemaHash, float first, String rowVersion,
            String logType, String connectionUid) throws Exception {
        client.execute("INSERT INTO feature_vectors "
            + "(event_id, event_time, sensor, log_type, connection_uid, schema_id, schema_hash, `values`, quality_flags, row_version) VALUES ("
            + "'" + eventId + "', '2026-08-27 10:03:11.250', 'sensor-eu-1', '" + logType + "', '" + connectionUid + "', 'conn-feature-v1', "
            + "'" + schemaHash + "', [" + first + ", 2, 3], 0, '" + rowVersion + "')").get();
    }

    // Two emissions of the same event with DIFFERENT values — what a replay after
    // a checkpoint restore actually produces, because indices 17-19 come from the
    // restored window state. log_type and connection_uid are identical across the
    // two rows: they describe which Zeek log and which connection the event
    // belongs to, and a checkpoint restore replays the same source event, so
    // neither changes on replay -- only the window-derived values and the
    // producedAt stamp do.
    @Test
    void returnsOneDeterministicLatestRowPerEventAndSchema() throws Exception {
        try (Client client = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, "dedup_latest")) {
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 1.0f, "2026-08-27 10:03:11.402", "conn", "Cabc123XYZ");
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 9.0f, "2026-08-27 11:00:00.000", "conn", "Cabc123XYZ");

            // No OPTIMIZE ... FINAL anywhere: the point is that the query is
            // correct without physical compaction having happened.
            assertEquals(2, client.queryAll("SELECT count() AS c FROM feature_vectors").get(0).getLong("c"),
                "both rows are physically present; ReplacingMergeTree has not compacted");

            List<GenericRecord> deduplicated =
                client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH));

            assertEquals(1, deduplicated.size(), "the query must collapse the duplicate");
            assertEquals("sensor-eu-1:a", deduplicated.get(0).getString("event_id"));
            assertEquals(9.0f, firstValue(deduplicated.get(0)), 0.0001f,
                "the later row_version wins, so the replayed emission's values survive");
            // The multi-protocol envelope columns. log_type is how training tells
            // protocols apart -- the entire point of the redesign -- and
            // connection_uid is the Zeek cross-protocol correlation key. Without
            // this assertion, a query that silently dropped both projections (or
            // that let ClickHouse's implicit '' default leak through) would still
            // pass every other assertion in this test.
            assertEquals("conn", deduplicated.get(0).getString("log_type"),
                "log_type must survive the dedup, not fall back to the column's implicit default");
            assertEquals("Cabc123XYZ", deduplicated.get(0).getString("connection_uid"),
                "connection_uid must survive the dedup, not fall back to the column's implicit default");
        }
    }

    // Re-running the same query must give the same answer; "deterministic" is
    // half the requirement.
    @Test
    void isStableAcrossRepeatedRuns() throws Exception {
        try (Client client = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, "dedup_stable")) {
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 1.0f, "2026-08-27 10:03:11.402", "conn", "Cabc123XYZ");
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 9.0f, "2026-08-27 11:00:00.000", "conn", "Cabc123XYZ");

            float firstRun = firstValue(client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH)).get(0));
            float secondRun = firstValue(client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH)).get(0));

            assertEquals(firstRun, secondRun, 0.0f);
        }
    }

    // A snapshot is always taken for one frozen feature schema. Rows from another
    // schema version must not leak into it. The two events carry different
    // connection_uids here, matching reality: two different events belong to two
    // different Zeek connections, unlike the replay pair above.
    @Test
    void filtersToTheRequestedSchemaHash() throws Exception {
        try (Client client = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, "dedup_filter")) {
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 1.0f, "2026-08-27 10:03:11.402", "conn", "Cabc123XYZ");
            insert(client, "sensor-eu-1:b", OTHER_HASH, 5.0f, "2026-08-27 10:03:11.402", "conn", "Cdef456UVW");

            List<GenericRecord> rows = client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH));

            assertEquals(1, rows.size());
            assertEquals("sensor-eu-1:a", rows.get(0).getString("event_id"));
        }
    }
}
