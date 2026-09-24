package io.netsecml.platform.adapter.kafka.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

// Pins upstream's field-value rules (two-models-info/S7___/kafka_source.py):
// _first, _int_or_none, _bool_or_none, int() for ports and float() for ts.
class S7commFieldValuesTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    // One JSON value, as Jackson reads it off the wire.
    private static JsonNode v(String json) throws Exception {
        return JSON.readTree("{\"v\":" + json + "}").get("v");
    }

    @Test
    void firstSkipsAbsentNullAndEmptyStringButNotZeroOrFalse() throws Exception {
        // upstream _first: `value is not None and value != ""`.
        JsonNode zero = v("0");
        assertSame(zero, S7commFieldValues.first(null, v("null"), v("\"\""), zero, v("5")));
        JsonNode no = v("false");
        assertSame(no, S7commFieldValues.first(v("\"\""), no));
        assertNull(S7commFieldValues.first(null, v("null"), v("\"\"")));
    }

    @Test
    void intOrNoneFollowsPythonsIntWithBaseZeroThenFloat() throws Exception {
        assertEquals(4L, S7commFieldValues.intOrNone(v("4")));
        assertEquals(4L, S7commFieldValues.intOrNone(v("\"4\"")));
        assertEquals(4L, S7commFieldValues.intOrNone(v("\"0x04\"")));
        assertEquals(26L, S7commFieldValues.intOrNone(v("\"0X1A\"")));
        assertEquals(15L, S7commFieldValues.intOrNone(v("\"0o17\"")));
        assertEquals(5L, S7commFieldValues.intOrNone(v("\"0b101\"")));
        assertEquals(-3L, S7commFieldValues.intOrNone(v("\"-3\"")));
        assertEquals(7L, S7commFieldValues.intOrNone(v("\" +7 \"")), "int() strips whitespace");
        assertEquals(7L, S7commFieldValues.intOrNone(v("\"007\"")), "int('007', 0) fails; float('007') is 7.0");
        assertEquals(3L, S7commFieldValues.intOrNone(v("\"3.9\"")), "int(float(text)) truncates toward zero");
        assertEquals(-3L, S7commFieldValues.intOrNone(v("\"-3.9\"")));
        assertEquals(1000L, S7commFieldValues.intOrNone(v("\"1e3\"")));
        assertEquals(3L, S7commFieldValues.intOrNone(v("3.9")), "a JSON float truncates too");
        assertEquals(1L, S7commFieldValues.intOrNone(v("true")), "bool is an int subclass in Python");
        assertEquals(0L, S7commFieldValues.intOrNone(v("false")));
        assertNull(S7commFieldValues.intOrNone(v("\"\"")));
        assertNull(S7commFieldValues.intOrNone(v("null")));
        assertNull(S7commFieldValues.intOrNone(null));
    }

    @Test
    void intOrNoneRejectsWhatPythonRaisesOn() throws Exception {
        for (String bad : new String[] {"\"abc\"", "\"0x\"", "\"nan\"", "\"inf\"", "\" \"", "[1]"}) {
            assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.intOrNone(v(bad)), bad);
        }
    }

    @Test
    void aLongMalformedNumberIsRejectedInLinearTime() throws Exception {
        // One malformed record must reach the DLQ, not stall the operator: a
        // pattern whose digit runs can split two ways backtracks quadratically,
        // and 100,000 digits then take minutes. Both parse paths that use the
        // decimal pattern are checked.
        JsonNode longDigits = v("\"" + "1".repeat(100_000) + "x\"");
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.intOrNone(longDigits));
            assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.seconds(longDigits));
        });
    }

    @Test
    void boolOrNoneFollowsUpstreamsSpellings() throws Exception {
        for (String yes : new String[] {"true", "1", "2", "\"true\"", "\"T\"", "\"1\"", "\"yes\"", "\"Y\"", "\"orig\"",
                "\"originator\""}) {
            assertEquals(Boolean.TRUE, S7commFieldValues.boolOrNone(v(yes)), yes);
        }
        for (String no : new String[] {"false", "0", "0.5", "\"false\"", "\"f\"", "\"0\"", "\"no\"", "\"N\"",
                "\"resp\"", "\"Responder\""}) {
            assertEquals(Boolean.FALSE, S7commFieldValues.boolOrNone(v(no)), no);
        }
        assertNull(S7commFieldValues.boolOrNone(v("null")));
        assertNull(S7commFieldValues.boolOrNone(v("\"\"")));
        assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.boolOrNone(v("\"maybe\"")));
    }

    @Test
    void portFollowsPythonsPlainInt() throws Exception {
        // _resolve_endpoints calls int(source_p): base 10 only, floats truncate.
        assertEquals(102L, S7commFieldValues.port(v("102")));
        assertEquals(102L, S7commFieldValues.port(v("\"102\"")));
        assertEquals(102L, S7commFieldValues.port(v("102.9")));
        assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.port(v("\"0x66\"")));
        assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.port(v("\"102.0\"")));
    }

    @Test
    void secondsFollowsPythonsFloat() throws Exception {
        assertEquals(1790000000.25, S7commFieldValues.seconds(v("1790000000.25")));
        assertEquals(1790000000.25, S7commFieldValues.seconds(v("\"1790000000.25\"")));
        assertEquals(1000.0, S7commFieldValues.seconds(v("\"1e3\"")));
        assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.seconds(v("\"soon\"")));
    }

    @Test
    void hostIsTheValuesTextAndRejectsContainers() throws Exception {
        assertEquals("10.0.0.5", S7commFieldValues.host(v("\"10.0.0.5\"")));
        assertThrows(IllegalArgumentException.class, () -> S7commFieldValues.host(v("{\"a\":1}")));
    }
}
