package io.netsecml.platform.adapter.clickhouse.writer;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.adapter.clickhouse.mapper.FeatureVectorRowMapper;
import io.netsecml.platform.adapter.clickhouse.mapper.InvalidEventRowMapper;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The real HTTP path against a real server.
//
// This is also where the row records' @JsonProperty names meet the actual DDL:
// a mismatched column name fails here, on a two-row insert with a readable error,
// rather than deep inside the end-to-end test.
@Testcontainers(disabledWithoutDocker = true)
class ClientV2InserterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // Builds a config pointed at one database on the shared container. Each test
    // gets its own database, mirroring how ClickHouseTestSupport isolates the
    // DDL tests.
    private ClickHouseConfig configFor(String database) {
        return ClickHouseConfig.of(CLICKHOUSE.getHost(), CLICKHOUSE.getMappedPort(8123),
            database, "default", "");
    }

    // A representative FeatureVector with only the first of 20 values set, so
    // each test can drive event identity, the first feature value and the
    // dedup-relevant producedAt independently while every other field stays
    // fixed. logType/connectionUid/schemaId/schemaHash are structural filler --
    // not the point of these tests -- but must still be valid, non-null values.
    private FeatureVector vector(String eventId, float first, Instant producedAt) {
        float[] values = new float[20];
        values[0] = first;
        return new FeatureVector(eventId, Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ", "conn-feature-v1", "f".repeat(64),
            values, 0, producedAt);
    }

    // A two-row insert is enough to prove the client actually reaches the server
    // and that every one of the 20 packed float values round-trips intact.
    @Test
    void insertsFeatureVectorRowsWithAllTwentyValuesIntact() throws Exception {
        String database = "inserter_features";
        FeatureVectorRowMapper mapper = new FeatureVectorRowMapper();

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database);
             ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {

            List<String> lines = List.of(
                MAPPER.writeValueAsString(mapper.toRow(vector("sensor-eu-1:a", 1.5f, Instant.parse("2026-08-27T10:03:11.402Z")))),
                MAPPER.writeValueAsString(mapper.toRow(vector("sensor-eu-1:b", 2.5f, Instant.parse("2026-08-27T10:03:11.403Z")))));
            inserter.insert("feature_vectors", lines);

            List<GenericRecord> rows = query.queryAll(
                "SELECT event_id, sensor, schema_hash, length(`values`) AS n, `values`[1] AS first "
                    + "FROM feature_vectors ORDER BY event_id");

            assertEquals(2, rows.size());
            assertEquals("sensor-eu-1:a", rows.get(0).getString("event_id"));
            assertEquals("sensor-eu-1", rows.get(0).getString("sensor"));
            assertEquals("f".repeat(64), rows.get(0).getString("schema_hash"));
            assertEquals(20, rows.get(0).getInteger("n"), "all 20 feature values must survive the insert");
            assertEquals(1.5f, rows.get(0).getFloat("first"), 0.0001f);
        }
    }

    // Same path, the other row shape: invalid_events has its own mapper and its
    // own column set, so it needs its own round trip through a real insert.
    @Test
    void insertsInvalidEventRows() throws Exception {
        String database = "inserter_invalid";
        InvalidEventRowMapper mapper = new InvalidEventRowMapper();

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database);
             ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {

            RejectedEvent event = new RejectedEvent("sensor-eu-1:Cabc", "a".repeat(64),
                ReasonCode.INVALID_PORT, "port 70000 out of range", Instant.parse("2026-08-27T10:03:11.250Z"));
            inserter.insert("invalid_events", List.of(MAPPER.writeValueAsString(mapper.toRow(event))));

            List<GenericRecord> rows = query.queryAll(
                "SELECT event_id, stage, reason_code, detail, source_version, raw_payload_hash FROM invalid_events");

            assertEquals(1, rows.size());
            assertEquals("sensor-eu-1:Cabc", rows.get(0).getString("event_id"));
            assertEquals("MAP", rows.get(0).getString("stage"));
            assertEquals("INVALID_PORT", rows.get(0).getString("reason_code"));
            assertEquals("zeek-conn-source-v1", rows.get(0).getString("source_version"));
        }
    }

    // Server-side DEFAULT columns must be populated even though the client never
    // sends them — that is the whole reason they are omitted from the row records.
    @Test
    void serverSetsArchivedAtEvenThoughTheClientNeverSendsIt() throws Exception {
        String database = "inserter_defaults";

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database);
             ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {

            inserter.insert("feature_vectors", List.of(MAPPER.writeValueAsString(
                new FeatureVectorRowMapper().toRow(vector("sensor-eu-1:a", 1f, Instant.parse("2026-08-27T10:03:11.402Z"))))));

            List<GenericRecord> rows = query.queryAll(
                "SELECT toUnixTimestamp64Milli(archived_at) AS archived FROM feature_vectors");

            assertEquals(1, rows.size());
            assertTrue(rows.get(0).getLong("archived") > 0L, "archived_at must be set by the server DEFAULT");
        }
    }

    // A failed insert must surface as an exception, because the sink turns that
    // into a failed checkpoint and therefore into unadvanced Kafka offsets.
    @Test
    void throwsWhenTheServerRejectsTheBatch() throws Exception {
        String database = "inserter_rejects";
        ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database).close();

        try (ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {
            assertThrows(Exception.class,
                () -> inserter.insert("feature_vectors", List.of("{\"no_such_column\": 1}")),
                "an unknown column must not be silently discarded");
        }
    }
}
