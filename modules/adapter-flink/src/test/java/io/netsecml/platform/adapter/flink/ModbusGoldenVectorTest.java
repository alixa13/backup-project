package io.netsecml.platform.adapter.flink;

import io.netsecml.platform.adapter.kafka.mapper.ModbusEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Spec section 11.2's worked vectors (F3): drives raw wire-shaped JSON through
// the REAL JsonZeekModbusParser -> ModbusEventMapper -> ModbusBuildFeaturesUseCase
// (state threaded from one record to the next, exactly as ModbusFeatureProcessFunction
// threads it in production), and asserts all 42 values against a hand derivation of
// two-models-info/modbus_/07b_materialize_feature_engine_v1.py's process_capture.
// This is the only test in the unit that exercises the FULL composed pipeline with
// microsecond wire timestamps -- every other modbus fixture (this class's own siblings
// included) uses millisecond-exact timestamps, which is exactly the shape that let F1
// (the causal engine seeing a millisecond-rounded ts instead of the wire's unrounded
// float64) through undetected. See ModbusEvent.tsSeconds()'s own javadoc for the fix.
//
// Fixture shape follows the platform's REAL wire, never a convenient one: underscored
// id_orig_h/id_resp_h, IDENTICAL on the request and its response (connection-level,
// exactly as Zeek emits them -- see CLAUDE.md's endpoint-orientation invariant), so the
// mapper's orientation-by-direction step is exercised, not bypassed by hand-building an
// already-per-packet ModbusEvent the way most other tests in this unit deliberately do
// for reduction.
class ModbusGoldenVectorTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    private final JsonZeekModbusParser parser = new JsonZeekModbusParser();
    private final ModbusEventMapper mapper = new ModbusEventMapper();
    private final ModbusBuildFeaturesUseCase useCase = new ModbusBuildFeaturesUseCase(Clock.systemUTC());

    // Real parser, then real mapper -- the same two stages
    // ModbusParseMapValidateFunction chains in production -- narrowed to
    // ModbusEvent with an explicit arm per sealed permits member, never a
    // default, mirroring ModbusEventMapperTest's own asModbusEvent.
    private ModbusEvent parseAndMap(String json) {
        MappingResult<io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord> parsed =
            parser.parse(json.getBytes(StandardCharsets.UTF_8));
        assertTrue(parsed.isValid(), () -> "parser rejected the fixture: " + parsed);
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), SENSOR);
        assertTrue(mapped.isValid(), () -> "mapper rejected the fixture: " + mapped.reason() + ": " + mapped.detail());
        return switch (mapped.value()) {
            case ModbusEvent m -> m;
            case S7commEvent s -> throw new AssertionError("ModbusEventMapper maps modbus_detailed.log exclusively; got an S7commEvent");
            case ConnEvent c -> throw new AssertionError("ModbusEventMapper maps modbus_detailed.log exclusively; got a ConnEvent");
            case DnsEvent d -> throw new AssertionError("ModbusEventMapper maps modbus_detailed.log exclusively; got a DnsEvent");
        };
    }

    // Builds a full 42-value expected vector from ONLY its non-zero features, named
    // by their schema name rather than by a hand-counted index -- so a value
    // transcribed against the wrong index cannot hide the way it could inside a
    // 42-line positional array literal. Fails fast on a typo'd name (one that
    // matches nothing in the registered schema) instead of silently leaving the
    // default zero in place, which would turn a mistyped feature name into a
    // false pass.
    private static float[] expectedVector(Map<String, Float> nonZeroByName) {
        Set<String> validNames = ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .map(FeatureDefinition::name)
            .collect(Collectors.toSet());
        for (String name : nonZeroByName.keySet()) {
            if (!validNames.contains(name)) {
                throw new IllegalArgumentException(
                    "no modbus-feature-v1 feature named \"" + name + "\" -- check for a typo");
            }
        }
        float[] vector = new float[ModbusFeatureSchemaV1.SCHEMA.featureCount()];
        for (FeatureDefinition definition : ModbusFeatureSchemaV1.SCHEMA.definitions()) {
            Float value = nonZeroByName.get(definition.name());
            vector[definition.index()] = value != null ? value : 0f;
        }
        return vector;
    }

    // -- Case 1: the RTT pair (F3's primary case; microsecond timestamps) --
    //
    // Request ts ...600.123456, response ts ...600.124001: a true gap of
    // 0.000545024871826... s. The unfixed mapper rounds both to whole
    // milliseconds (.123 and .124), so the causal engine would see a gap of
    // exactly 0.001 s instead -- 84% high, and the defect F1 fixes.
    private static final String CASE1_REQUEST = "{\"ts\":1789977600.123456,\"uid\":\"MBGOLD1\","
        + "\"id_orig_h\":\"10.0.0.5\",\"id_resp_h\":\"10.0.0.9\",\"request_response\":\"request\","
        + "\"tid\":17,\"unit\":\"1\",\"func\":\"READ_HOLDING_REGISTERS\",\"address\":100,\"quantity\":2,"
        + "\"request_values\":[],\"response_values\":[]}";
    private static final String CASE1_RESPONSE = "{\"ts\":1789977600.124001,\"uid\":\"MBGOLD1\","
        + "\"id_orig_h\":\"10.0.0.5\",\"id_resp_h\":\"10.0.0.9\",\"request_response\":\"response\","
        + "\"tid\":17,\"unit\":\"1\",\"func\":\"READ_HOLDING_REGISTERS\",\"address\":100,\"quantity\":2,"
        + "\"matched\":true,\"request_values\":[],\"response_values\":[7,9]}";

    @Test
    void theRttPairMatchesTheUpstreamEngineExactly() {
        ModbusEvent request = parseAndMap(CASE1_REQUEST);
        ModbusEvent response = parseAndMap(CASE1_RESPONSE);

        FeatureBuildResult<ModbusEntityState> requestResult = useCase.build(request, ModbusEntityState.empty());
        FeatureBuildResult<ModbusEntityState> responseResult = useCase.build(response, requestResult.newState());

        // Derived by hand from 07b's process_capture (roughly lines 300-559),
        // with float32 casts computed via struct.unpack('f', struct.pack('f', x))
        // in python3 (numpy is not installed) -- see this task's fix report for
        // the derivation script. The two timing values (inter_arrival_s, rtt_s)
        // are exactly what section 1 of the final review measured: 0.0005450249,
        // not the unfixed code's millisecond-rounded 0.0010001659.
        float[] expectedRequest = expectedVector(Map.ofEntries(
            Map.entry("fc_3", 1f),
            Map.entry("address_value", 100f),
            Map.entry("address_present", 1f),
            Map.entry("quantity_value", 2f),
            Map.entry("quantity_present", 1f),
            Map.entry("event_rate_1s", 1f),
            Map.entry("event_rate_10s", 0.1f),
            Map.entry("event_rate_60s", 0.016666668f),
            Map.entry("unique_function_count_10s", 1f),
            Map.entry("unique_address_count_10s", 1f),
            Map.entry("read_ratio_10s", 1f)));
        float[] expectedResponse = expectedVector(Map.ofEntries(
            Map.entry("is_response", 1f),
            Map.entry("fc_3", 1f),
            Map.entry("address_value", 100f),
            Map.entry("address_present", 1f),
            Map.entry("quantity_value", 2f),
            Map.entry("quantity_present", 1f),
            Map.entry("response_matched", 1f),
            Map.entry("response_values_present", 1f),
            Map.entry("response_value_count", 2f),
            Map.entry("response_value_min", 7f),
            Map.entry("response_value_max", 9f),
            Map.entry("response_value_mean", 8f),
            Map.entry("prev_event_available", 1f),
            Map.entry("inter_arrival_s", 0.0005450249f),
            Map.entry("address_delta_valid", 1f),
            Map.entry("quantity_delta_valid", 1f),
            Map.entry("outstanding_requests_before_event", 1f),
            Map.entry("rtt_valid", 1f),
            Map.entry("rtt_s", 0.0005450249f),
            Map.entry("event_rate_1s", 2f),
            Map.entry("event_rate_10s", 0.2f),
            Map.entry("event_rate_60s", 0.033333335f),
            Map.entry("unique_function_count_10s", 1f),
            Map.entry("unique_address_count_10s", 1f),
            Map.entry("read_ratio_10s", 1f)));

        assertArrayEquals(expectedRequest, requestResult.vector().values());
        assertArrayEquals(expectedResponse, responseResult.vector().values());
    }

    // -- Case 2: the 15 s segment boundary --
    //
    // Request ts ...600.000600, response ts ...615.001400: a true gap of
    // 15.0008 s, over GAP_SECONDS, so 07b (and the fixed Java) starts a new
    // segment for the response. Rounded to whole milliseconds the gap is
    // exactly 15.000 s, so the unfixed code would NOT reset -- ten wrong
    // values on the response, per the final review's section 1.
    private static final String CASE2_REQUEST = "{\"ts\":1789977600.000600,\"uid\":\"MBGOLD2\","
        + "\"id_orig_h\":\"10.0.0.5\",\"id_resp_h\":\"10.0.0.9\",\"request_response\":\"request\","
        + "\"tid\":501,\"unit\":\"1\",\"func\":\"READ_HOLDING_REGISTERS\",\"address\":100,\"quantity\":2,"
        + "\"request_values\":[],\"response_values\":[]}";
    private static final String CASE2_RESPONSE = "{\"ts\":1789977615.001400,\"uid\":\"MBGOLD2\","
        + "\"id_orig_h\":\"10.0.0.5\",\"id_resp_h\":\"10.0.0.9\",\"request_response\":\"response\","
        + "\"tid\":501,\"unit\":\"1\",\"func\":\"READ_HOLDING_REGISTERS\",\"address\":100,\"quantity\":2,"
        + "\"matched\":true,\"request_values\":[],\"response_values\":[7,9]}";

    @Test
    void aFifteenPointZeroZeroEightSecondGapStartsANewSegmentOnTheResponse() {
        ModbusEvent request = parseAndMap(CASE2_REQUEST);
        ModbusEvent response = parseAndMap(CASE2_RESPONSE);

        FeatureBuildResult<ModbusEntityState> requestResult = useCase.build(request, ModbusEntityState.empty());
        FeatureBuildResult<ModbusEntityState> responseResult = useCase.build(response, requestResult.newState());

        float[] expectedRequest = expectedVector(Map.ofEntries(
            Map.entry("fc_3", 1f),
            Map.entry("address_value", 100f),
            Map.entry("address_present", 1f),
            Map.entry("quantity_value", 2f),
            Map.entry("quantity_present", 1f),
            Map.entry("event_rate_1s", 1f),
            Map.entry("event_rate_10s", 0.1f),
            Map.entry("event_rate_60s", 0.016666668f),
            Map.entry("unique_function_count_10s", 1f),
            Map.entry("unique_address_count_10s", 1f),
            Map.entry("read_ratio_10s", 1f)));

        // The response's own vector is what proves the segment reset: with the
        // gap over 15 s, `before` for the response is ModbusEntityState.empty(),
        // so every C-group causal feature the request would otherwise have set
        // (prev_event_available, inter_arrival_s, outstanding_requests_before_event,
        // rtt_valid/rtt_s) is zero, and the pending-TID map holds nothing, so an
        // otherwise-matched response is flagged response_without_request instead.
        float[] expectedResponse = expectedVector(Map.ofEntries(
            Map.entry("is_response", 1f),
            Map.entry("fc_3", 1f),
            Map.entry("address_value", 100f),
            Map.entry("address_present", 1f),
            Map.entry("quantity_value", 2f),
            Map.entry("quantity_present", 1f),
            Map.entry("response_matched", 1f),
            Map.entry("response_values_present", 1f),
            Map.entry("response_value_count", 2f),
            Map.entry("response_value_min", 7f),
            Map.entry("response_value_max", 9f),
            Map.entry("response_value_mean", 8f),
            Map.entry("response_without_request", 1f),
            Map.entry("event_rate_1s", 1f),
            Map.entry("event_rate_10s", 0.1f),
            Map.entry("event_rate_60s", 0.016666668f),
            Map.entry("unique_function_count_10s", 1f),
            Map.entry("unique_address_count_10s", 1f),
            Map.entry("read_ratio_10s", 1f)));

        assertArrayEquals(expectedRequest, requestResult.vector().values());
        assertArrayEquals(expectedResponse, responseResult.vector().values());
    }

    // -- Case 3: the 1 s window edge --
    //
    // Two requests 0.9999 s apart (< 1.0 s): 07b's window is the half-open
    // interval (t-1, t], so an entry stored exactly 1.0 s before `ts` is
    // EXCLUDED but one stored 0.9999 s before is INCLUDED -- the second
    // event's event_rate_1s must be 2, counting itself and the first. Rounded
    // to whole milliseconds this gap becomes exactly 1.000 s under the
    // unfixed code, which the strict `> cutoff` filter then excludes,
    // reporting event_rate_1s 1 instead of 2.
    private static final String CASE3_FIRST = "{\"ts\":1789977700.0,\"uid\":\"MBGOLD3\","
        + "\"id_orig_h\":\"10.0.0.5\",\"id_resp_h\":\"10.0.0.9\",\"request_response\":\"request\","
        + "\"tid\":901,\"unit\":\"1\",\"func\":\"READ_HOLDING_REGISTERS\",\"address\":100,\"quantity\":2,"
        + "\"request_values\":[],\"response_values\":[]}";
    private static final String CASE3_SECOND = "{\"ts\":1789977700.9999,\"uid\":\"MBGOLD3\","
        + "\"id_orig_h\":\"10.0.0.5\",\"id_resp_h\":\"10.0.0.9\",\"request_response\":\"request\","
        + "\"tid\":902,\"unit\":\"1\",\"func\":\"READ_HOLDING_REGISTERS\",\"address\":100,\"quantity\":2,"
        + "\"request_values\":[],\"response_values\":[]}";

    @Test
    void anEventNineNineNineNineSecondsAfterThePreviousOneCountsBothInTheOneSecondWindow() {
        ModbusEvent first = parseAndMap(CASE3_FIRST);
        ModbusEvent second = parseAndMap(CASE3_SECOND);

        FeatureBuildResult<ModbusEntityState> firstResult = useCase.build(first, ModbusEntityState.empty());
        FeatureBuildResult<ModbusEntityState> secondResult = useCase.build(second, firstResult.newState());

        // Full vector for the second event, not just event_rate_1s: cheap here
        // (two requests, no response bookkeeping), and it also pins
        // outstanding_requests_before_event (the first request is still
        // pending) and the delta features (address/quantity unchanged).
        float[] expectedSecond = expectedVector(Map.ofEntries(
            Map.entry("fc_3", 1f),
            Map.entry("address_value", 100f),
            Map.entry("address_present", 1f),
            Map.entry("quantity_value", 2f),
            Map.entry("quantity_present", 1f),
            Map.entry("prev_event_available", 1f),
            Map.entry("inter_arrival_s", 0.9999001f),
            Map.entry("address_delta_valid", 1f),
            Map.entry("quantity_delta_valid", 1f),
            Map.entry("outstanding_requests_before_event", 1f),
            Map.entry("event_rate_1s", 2f),
            Map.entry("event_rate_10s", 0.2f),
            Map.entry("event_rate_60s", 0.033333335f),
            Map.entry("unique_function_count_10s", 1f),
            Map.entry("unique_address_count_10s", 1f),
            Map.entry("read_ratio_10s", 1f)));

        assertEquals(2.0f, secondResult.vector().values()[35], "event_rate_1s must count both events");
        assertArrayEquals(expectedSecond, secondResult.vector().values());
    }
}
