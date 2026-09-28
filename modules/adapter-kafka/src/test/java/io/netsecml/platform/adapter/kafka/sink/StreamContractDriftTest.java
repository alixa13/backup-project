package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// Contract-versus-code drift is the failure mode that actually happens: someone
// adds a field to a serializer and the frozen contract quietly stops describing
// the wire. These tests turn that into a build failure.
class StreamContractDriftTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // This module lives at modules/adapter-kafka, so the repo root is two up.
    private Path contract(String name) {
        return Paths.get("..", "..", "contracts", "stream", name);
    }

    // The field names the frozen contract declares, in declaration order.
    private Set<String> contractFields(String name) throws IOException {
        JsonNode contract = MAPPER.readTree(contract(name).toFile());
        Set<String> names = new LinkedHashSet<>();
        contract.get("fields").forEach(field -> names.add(field.get("name").asText()));
        return names;
    }

    // The field names a serialized message actually carries.
    private Set<String> messageFields(byte[] message) throws IOException {
        Set<String> names = new LinkedHashSet<>();
        MAPPER.readTree(message).fieldNames().forEachRemaining(names::add);
        return names;
    }

    // One representative feature vector reused by the drift check and the
    // round-trip test below; qualityFlags is deliberately non-zero (7) so a
    // round-trip bug that quietly zeroes an int field would still be caught.
    private FeatureVector vector() {
        return new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-13T10:00:00Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ",
            ConnFeatureSchemaV1.SCHEMA.id(), ConnFeatureSchemaV1.CONTENT_HASH,
            new float[]{1f, 2f, 3f}, 7, Instant.parse("2026-08-13T10:00:00.402Z"));
    }

    // One representative MAP-stage payload reused by the drift check and the
    // round-trip test below.
    private RejectedRecordPayload payload() {
        return new RejectedRecordPayload("{ broken".getBytes(StandardCharsets.UTF_8), "sensor-eu-1:Cabc",
            "MAP", "INVALID_PORT", "port 70000 out of range", Instant.parse("2026-08-27T10:03:11.250Z"));
    }

    // Set equality both directions: this fails if the serializer emits a field
    // the contract doesn't declare, OR the contract declares a field the
    // serializer no longer emits. A "contains" check would miss the second case.
    @Test
    void featureVectorSerializerEmitsExactlyTheFrozenContractFields() throws Exception {
        Set<String> emitted = messageFields(
            new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector()));

        assertEquals(contractFields("feature-vector-v1.json"), emitted,
            "the serializer and contracts/stream/feature-vector-v1.json must describe the same message");
    }

    // Same drift check as above, for the DLQ contract and its serializer.
    @Test
    void dlqSerializerEmitsExactlyTheFrozenContractFields() throws Exception {
        Set<String> emitted = messageFields(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload()));

        assertEquals(contractFields("dlq-v1.json"), emitted,
            "the serializer and contracts/stream/dlq-v1.json must describe the same message");
    }

    // Round-tripping proves the deserializer the archive job depends on actually
    // reads what the online job writes — not merely that both mention the same names.
    @Test
    void featureVectorRoundTripsThroughTheDeserializer() throws Exception {
        FeatureVector original = vector();
        byte[] wire = new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", original);

        FeatureVector restored = new FeatureVectorDeserializer()
            .deserialize("netsec.conn.feature-vector.v1", wire);

        assertEquals(original.eventId(), restored.eventId());
        assertEquals(original.eventTime(), restored.eventTime());
        assertEquals(original.sensor(), restored.sensor());
        assertEquals(original.logType(), restored.logType());
        assertEquals(original.connectionUid(), restored.connectionUid());
        assertEquals(original.schemaId(), restored.schemaId());
        assertEquals(original.schemaHash(), restored.schemaHash());
        assertArrayEquals(original.values(), restored.values(), 0.0f);
        assertEquals(original.qualityFlags(), restored.qualityFlags());
        assertEquals(original.producedAt(), restored.producedAt());
    }

    // Mirrors featureVectorRoundTripsThroughTheDeserializer above, but for the
    // DLQ side: proves RejectedEventDeserializer actually reconstructs the
    // domain RejectedEvent that RejectedRecordSerializer put on the wire,
    // including the reasonCode-string-to-enum and hash-length checks.
    @Test
    void rejectedEventRoundTripsThroughTheDeserializer() throws Exception {
        byte[] wire = new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload());

        RejectedEvent restored = new RejectedEventDeserializer().deserialize("netsec.conn.dlq.v1", wire);

        assertEquals("sensor-eu-1:Cabc", restored.eventId());
        assertEquals(ReasonCode.INVALID_PORT, restored.reason());
        assertEquals("port 70000 out of range", restored.detail());
        assertEquals(64, restored.rawPayloadHash().length());
        assertEquals(Instant.parse("2026-08-27T10:03:11.250Z"), restored.receivedAt());
    }

    private static ModbusDetectorPrediction scoredPrediction() {
        return new ModbusDetectorPrediction("a".repeat(64), "sensor-eu-1:u:7:RESPONSE:1", Instant.parse(
            "2026-09-26T12:00:00Z"), new SensorId("sensor-eu-1"), "u", "10.0.0.5", "10.0.0.9", "1",
            "modbus-stage1-detector", "v1", "b".repeat(64), "modbus-feature-v1", "c".repeat(64),
            DetectorVerdict.ANOMALY, 0.3f, 0.2f, 0.2483385f, 0.4121148f, DetectorTrigger.DENSE, 20, 8, 180L,
            Instant.parse("2026-09-26T12:00:00.004Z"));
    }

    @Test
    void modbusDetectorPredictionSerializerEmitsExactlyTheContractFields() throws Exception {
        Set<String> emitted = messageFields(new ModbusDetectorPredictionSerializer()
            .serialize("netsec.modbus.prediction.v1", scoredPrediction()));
        assertEquals(contractFields("modbus-detector-prediction-v1.json"), emitted,
            "the serializer and contracts/stream/modbus-detector-prediction-v1.json must describe the same message");
    }

    @Test
    void s7commDetectorPredictionSerializerEmitsExactlyTheContractFields() throws Exception {
        S7commDetectorPrediction scored = new S7commDetectorPrediction("a".repeat(64), "sensor-eu-1:C1:25:REQUEST:1",
            Instant.parse("2026-09-28T12:00:00Z"), new SensorId("sensor-eu-1"), "C1", "10.0.0.5", "10.0.0.9",
            "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1", "c".repeat(64),
            DetectorVerdict.NORMAL, 0.01f, 0.5, S7commScoreGroup.RESPONSE, 0.001, 20, 0, 90L,
            Instant.parse("2026-09-28T12:00:00.004Z"));
        Set<String> emitted = messageFields(new S7commDetectorPredictionSerializer()
            .serialize("netsec.s7comm.prediction.v1", scored));
        assertEquals(contractFields("s7comm-detector-prediction-v1.json"), emitted,
            "the serializer and contracts/stream/s7comm-detector-prediction-v1.json must describe the same message");
    }
}
