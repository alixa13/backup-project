package io.netsecml.platform.adapter.clickhouse.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorRowMapperTest {
    private final FeatureVectorRowMapper mapper = new FeatureVectorRowMapper();

    // A representative FeatureVector: every field populated so each mapper
    // assertion below has a real value to check, not a default.
    private FeatureVector vector() {
        return new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ", "conn-feature-v1", "f".repeat(64),
            new float[]{1f, 2f, 3f}, 7, Instant.parse("2026-08-27T10:03:11.402Z"));
    }

    @Test
    void copiesIdentityAndPayloadAcross() {
        // The straightforward fields carry across unchanged, with SensorId
        // and LogType unwrapped to their String/wire forms for the row.
        FeatureVectorRow row = mapper.toRow(vector());

        assertEquals("sensor-eu-1:abc", row.eventId());
        assertEquals("sensor-eu-1", row.sensor());
        assertEquals("conn-feature-v1", row.schemaId());
        assertEquals("f".repeat(64), row.schemaHash());
        assertArrayEquals(new float[]{1f, 2f, 3f}, row.values(), 0.0f);
        assertEquals(7, row.qualityFlags());
    }

    // ClickHouse parses DateTime64(3) from 'yyyy-MM-dd HH:mm:ss.SSS'. ISO-8601
    // with T and Z is not reliably accepted, so the mapper must reformat.
    @Test
    void formatsTimestampsForDateTime64() {
        FeatureVectorRow row = mapper.toRow(vector());

        assertEquals("2026-08-27 10:03:11.250", row.eventTime());
        assertEquals("2026-08-27 10:03:11.402", row.rowVersion());
    }

    // producedAt becomes row_version. That is the whole deduplication contract.
    @Test
    void usesProducedAtAsRowVersion() {
        // Same identity, later producedAt: only row_version should move.
        FeatureVector later = new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ", "conn-feature-v1", "f".repeat(64),
            new float[]{1f}, 0, Instant.parse("2026-08-27T11:00:00.000Z"));

        assertEquals("2026-08-27 11:00:00.000", mapper.toRow(later).rowVersion());
    }

    // The @JsonProperty names ARE the ClickHouse column names — the sink writes
    // this record straight out as a JSONEachRow line. archived_at is absent on
    // purpose: the column carries DEFAULT now64(3) and is set server-side.
    @Test
    void serializesToExactlyTheClickHouseColumnNames() throws Exception {
        String json = new ObjectMapper().writeValueAsString(mapper.toRow(vector()));

        // Read the JSON back and collect only the field names, so this test
        // fails on any renamed/added/dropped column regardless of value.
        Set<String> emitted = new LinkedHashSet<>();
        new ObjectMapper().readTree(json).fieldNames().forEachRemaining(emitted::add);

        assertEquals(new LinkedHashSet<>(List.of(
            "event_id", "event_time", "sensor", "log_type", "connection_uid",
            "schema_id", "schema_hash", "values", "quality_flags", "row_version")), emitted);
    }

    // The row is handed to a serializer on another thread's flush; it must not
    // share the array it was built from.
    @Test
    void defensivelyCopiesValues() {
        // Mutating the source array after construction, and the returned array
        // after the fact, must both be invisible to the row's internal state.
        float[] values = new float[]{1f, 2f, 3f};
        FeatureVectorRow row = new FeatureVectorRow("id", "t", "s", "conn", "Cuid", "sid", "h", values, 0, "v");

        values[0] = -1f;
        assertEquals(1f, row.values()[0]);

        row.values()[0] = 99f;
        assertEquals(1f, row.values()[0]);
    }
}
