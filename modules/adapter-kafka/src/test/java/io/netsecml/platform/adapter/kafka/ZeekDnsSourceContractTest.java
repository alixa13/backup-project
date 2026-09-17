package io.netsecml.platform.adapter.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.adapter.kafka.dto.ZeekDnsEvent;
import io.netsecml.platform.adapter.kafka.mapper.DnsEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekDnsParser;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

// Binds contracts/source/zeek-dns-source-v1.json to the two places that would
// otherwise silently drift away from it: ZeekDnsEvent (what Jackson accepts off
// the wire) and DnsEventMapper (what the domain then requires). See
// docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md
// section 7.2 and section 9 step 1 for why every log type ships one of these.
class ZeekDnsSourceContractTest {
    private static final Path CONTRACT = Paths.get("..", "..", "contracts", "source", "zeek-dns-source-v1.json");
    private static final Path FIXTURE =
        Paths.get("..", "..", "tests", "fixtures", "zeek_dns", "valid-zeek-shaped.json");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // Pinned per field rather than derived, so a future change to what reason
    // code a field's removal produces has to edit this map by hand and explain
    // why, instead of the test quietly tracking whatever the code happens to do.
    // The eight fields JSON-required on ZeekDnsEvent never reach a DTO at all --
    // Jackson's own required=true creator check fails first, so the parser's
    // catch-all is what actually raises MALFORMED_JSON. qtype is NOT
    // JSON-required, so it parses fine and is instead rejected by
    // DnsEventMapper's own null check.
    private static final Map<String, ReasonCode> REQUIRED_FIELD_REASON = requiredFieldReasons();

    private static Map<String, ReasonCode> requiredFieldReasons() {
        Map<String, ReasonCode> reasons = new LinkedHashMap<>();
        for (String jsonRequiredField : List.of(
                "id", "ts", "id_orig_h", "id_orig_p", "id_resp_h", "id_resp_p", "trans_id", "query")) {
            reasons.put(jsonRequiredField, ReasonCode.MALFORMED_JSON);
        }
        reasons.put("qtype", ReasonCode.MISSING_REQUIRED_FIELD);
        return reasons;
    }

    private static JsonNode contract() throws IOException {
        return MAPPER.readTree(Files.readAllBytes(CONTRACT));
    }

    private static Set<String> namesOf(JsonNode fieldList) {
        Set<String> names = new LinkedHashSet<>();
        for (JsonNode field : fieldList) {
            names.add(field.get("name").asText());
        }
        return names;
    }

    // ZeekDnsEvent's @JsonProperty annotations sit in the record header, so the
    // compiler propagates each one onto whichever elements its own @Target
    // permits (JLS 8.10.3: a record component annotation flows to the backing
    // field, the accessor and the canonical constructor parameter, but only
    // where @Target allows it). Reflecting over the compiled class shows
    // JsonProperty's @Target includes METHOD and PARAMETER but not
    // RECORD_COMPONENT, so RecordComponent.getAnnotation() itself always
    // returns null here, while the accessor (equally, the constructor
    // parameter) carries it. The accessor is used below since
    // getRecordComponents() already returns components in declaration order,
    // with no need to line them up against constructor parameters by name.
    private static Set<String> dtoJsonPropertyNames() {
        Set<String> names = new LinkedHashSet<>();
        for (RecordComponent component : ZeekDnsEvent.class.getRecordComponents()) {
            JsonProperty jsonProperty = component.getAccessor().getAnnotation(JsonProperty.class);
            assertNotNull(jsonProperty, () -> component.getName() + " declares no @JsonProperty");
            names.add(jsonProperty.value());
        }
        return names;
    }

    private static byte[] fixtureWithout(String fieldName) throws IOException {
        ObjectNode node = (ObjectNode) MAPPER.readTree(Files.readAllBytes(FIXTURE));
        node.remove(fieldName);
        return MAPPER.writeValueAsBytes(node);
    }

    // Mirrors how the real pipeline is wired: a parse-stage rejection never
    // reaches the mapper at all.
    private static MappingResult<NetworkEvent> parseAndMapWithout(String fieldName) throws IOException {
        MappingResult<ZeekDnsEvent> parsed = new JsonZeekDnsParser().parse(fixtureWithout(fieldName));
        if (!parsed.isValid()) {
            return MappingResult.invalid(parsed.reason(), parsed.detail());
        }
        return new DnsEventMapper().map(parsed.value(), SENSOR);
    }

    @Test
    void contractIdIsZeekDnsSourceV1() throws IOException {
        assertEquals("zeek-dns-source-v1", contract().get("id").asText());
    }

    @Test
    void everyFieldNameAppearsInExactlyOneList() throws IOException {
        JsonNode contract = contract();
        Set<String> required = namesOf(contract.get("requiredFields"));
        Set<String> optional = namesOf(contract.get("optionalFields"));

        Set<String> overlap = new LinkedHashSet<>(required);
        overlap.retainAll(optional);

        assertTrue(overlap.isEmpty(), () -> "listed as both required and optional: " + overlap);
    }

    @Test
    void fieldListsCoverExactlyTheDtosJsonProperties() throws IOException {
        JsonNode contract = contract();
        Set<String> contractFields = new LinkedHashSet<>(namesOf(contract.get("requiredFields")));
        contractFields.addAll(namesOf(contract.get("optionalFields")));

        Set<String> dtoFields = dtoJsonPropertyNames();

        Set<String> missingFromContract = new LinkedHashSet<>(dtoFields);
        missingFromContract.removeAll(contractFields);
        Set<String> missingFromDto = new LinkedHashSet<>(contractFields);
        missingFromDto.removeAll(dtoFields);

        assertTrue(missingFromContract.isEmpty() && missingFromDto.isEmpty(),
            () -> "missing from contract: " + missingFromContract + "; missing from ZeekDnsEvent: " + missingFromDto);
    }

    // Guards the removal test below against passing vacuously: if the
    // unmodified fixture were already invalid, removing one more field could
    // never demonstrate that THAT field is what caused the rejection.
    @Test
    void unmodifiedFixtureMapsToAValidEvent() throws IOException {
        MappingResult<ZeekDnsEvent> parsed = new JsonZeekDnsParser().parse(Files.readAllBytes(FIXTURE));
        assertTrue(parsed.isValid(), () -> "fixture failed to parse: " + parsed.detail());

        MappingResult<NetworkEvent> mapped = new DnsEventMapper().map(parsed.value(), SENSOR);
        assertTrue(mapped.isValid(), () -> "fixture failed to map: " + mapped.detail());
    }

    @Test
    void requiredFieldReasonMapMatchesTheContractsRequiredFields() throws IOException {
        Set<String> contractRequired = namesOf(contract().get("requiredFields"));
        assertEquals(contractRequired, REQUIRED_FIELD_REASON.keySet(),
            "a contract required field with no pinned reason code (or vice versa) must fail here");
    }

    @TestFactory
    Stream<DynamicTest> requiredFieldRemovalYieldsItsPinnedReasonCode() {
        return REQUIRED_FIELD_REASON.entrySet().stream().map(entry -> DynamicTest.dynamicTest(entry.getKey(), () -> {
            MappingResult<NetworkEvent> result = parseAndMapWithout(entry.getKey());
            assertFalse(result.isValid(), () -> "removing " + entry.getKey() + " should be rejected");
            assertEquals(entry.getValue(), result.reason(),
                () -> "removing " + entry.getKey() + " produced the wrong reason code");
        }));
    }

    @TestFactory
    Stream<DynamicTest> optionalFieldRemovalStillYieldsAValidEvent() throws IOException {
        Set<String> optional = namesOf(contract().get("optionalFields"));
        return optional.stream().map(field -> DynamicTest.dynamicTest(field, () -> {
            MappingResult<NetworkEvent> result = parseAndMapWithout(field);
            assertTrue(result.isValid(), () -> "removing optional field " + field + " should still map, got " + result);
        }));
    }
}
