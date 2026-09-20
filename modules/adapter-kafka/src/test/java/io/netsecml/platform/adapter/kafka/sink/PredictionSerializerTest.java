package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.inference.Prediction;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PredictionSerializerTest {
    private static final String TOPIC = "netsec.prediction.v1";

    // One representative prediction reused by every assertion below. Every
    // component is deliberately distinct and recognisable -- modelName,
    // modelVersion and modelSha are different lengths/shapes on sight, and
    // schemaId/schemaHash are likewise unrelated to the model's own id/hash --
    // so a serializer or deserializer bug that swaps two same-typed fields
    // fails a round trip instead of passing by coincidence. eventTime and
    // producedAt are also deliberately different instants.
    private Prediction prediction() {
        String eventId = "sensor-eu-1:dns:Cabc123XYZ:7781";
        String modelName = "conn-scoring-rf";
        String modelVersion = "v3";
        return new Prediction(
            Prediction.deriveId(eventId, modelName, modelVersion),
            eventId,
            Instant.parse("2026-08-13T10:00:00Z"),
            modelName,
            modelVersion,
            "14ff129bd264a93daec7b276258fa38b4d7ebfb5b2deac81cfba48278fc1696",
            "dns-feature-v1",
            "4846c88e5a15ff5cc78bbc65e606a385059546702ed40d5ec13a2f82597cc6b",
            0.87f,
            true,
            0.5f,
            1234L,
            5,
            Instant.parse("2026-08-13T10:00:00.402Z"));
    }

    @Test
    void everyFieldSurvivesARoundTrip() {
        Prediction original = prediction();
        Prediction back = new PredictionDeserializer().deserialize(TOPIC,
            new PredictionSerializer().serialize(TOPIC, original));
        assertEquals(original, back);
    }

    @Test
    void theWireFormCarriesExactlyTheContractsFieldNames() throws Exception {
        // contracts/stream/prediction-v1.json is frozen; a renamed field would break
        // the archive reader silently, since JSON ignores what it does not recognise.
        // Lists (not Sets) on both sides: Set equality is order-insensitive, so it
        // would only prove the same names appear, not that they appear in the
        // contract's declared order -- and that order is what lets a human diff a
        // Kafka message against the contract top to bottom.
        JsonNode wire = new ObjectMapper().readTree(new PredictionSerializer().serialize(TOPIC, prediction()));
        JsonNode contract = new ObjectMapper().readTree(
            Path.of("..", "..", "contracts", "stream", "prediction-v1.json").toFile());

        List<String> expected = new ArrayList<>();
        contract.get("fields").forEach(f -> expected.add(f.get("name").asText()));
        List<String> actual = new ArrayList<>();
        wire.fieldNames().forEachRemaining(actual::add);

        assertEquals(expected, actual,
            "the serializer's field order must match contracts/stream/prediction-v1.json exactly");
    }

    // decision is a boolean per the contract; a naive implementation could
    // accidentally emit it as the string "true"/"false", which round-trips
    // through some JSON readers but not others and is not what the contract
    // declares (type: boolean).
    @Test
    void emitsDecisionAsAJsonBooleanNotAString() throws Exception {
        JsonNode wire = new ObjectMapper().readTree(new PredictionSerializer().serialize(TOPIC, prediction()));

        assertTrue(wire.get("decision").isBoolean(), "decision must be a JSON boolean, not a string");
        assertTrue(wire.get("decision").asBoolean());
    }

    // eventTime and producedAt are ISO-8601 UTC strings per the contract, with
    // millisecond precision preserved -- producedAt in particular becomes the
    // archive's row_version, so truncating it would break "last write wins"
    // deduplication.
    @Test
    void emitsTimestampsAsIso8601StringsWithMillisecondPrecision() throws Exception {
        JsonNode wire = new ObjectMapper().readTree(new PredictionSerializer().serialize(TOPIC, prediction()));

        assertEquals("2026-08-13T10:00:00Z", wire.get("eventTime").asText());
        assertEquals("2026-08-13T10:00:00.402Z", wire.get("producedAt").asText());
    }

    // score and threshold are float32 per the contract. Reading them back
    // through Jackson's floating-point node and narrowing to float must
    // reproduce the exact original value -- a serializer that widened them to
    // double first could introduce representable-but-different values.
    @Test
    void emitsScoreAndThresholdAsFloatingPointNumbers() throws Exception {
        JsonNode wire = new ObjectMapper().readTree(new PredictionSerializer().serialize(TOPIC, prediction()));

        assertTrue(wire.get("score").isFloatingPointNumber());
        assertTrue(wire.get("threshold").isFloatingPointNumber());
        assertEquals(0.87f, (float) wire.get("score").asDouble(), 0.0f);
        assertEquals(0.5f, (float) wire.get("threshold").asDouble(), 0.0f);
    }

    // inferenceMicros (int64) and qualityFlags (int32) are whole numbers per the
    // contract, not floating point.
    @Test
    void emitsInferenceMicrosAndQualityFlagsAsIntegralNumbers() throws Exception {
        JsonNode wire = new ObjectMapper().readTree(new PredictionSerializer().serialize(TOPIC, prediction()));

        assertTrue(wire.get("inferenceMicros").isIntegralNumber());
        assertEquals(1234L, wire.get("inferenceMicros").asLong());
        assertTrue(wire.get("qualityFlags").isIntegralNumber());
        assertEquals(5, wire.get("qualityFlags").asInt());
    }
}
