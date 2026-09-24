package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Pins the categorical codes against upstream's _cat and _operation
// (two-models-info/S7___/customer_icsnpp_enriched_builder.py): decoding what
// encode produced must give upstream's exact category string.
class S7commCategoriesTest {

    @Test
    void everyFunctionCodeDecodesToUpstreamsOperationString() {
        // upstream: FUNCTION_NAMES.get(code, f"FUNCTION_0x{code:02X}") -- all 256
        // one-byte codes, plus a wider one to pin the %02X width rule.
        for (int code = 0; code < 256; code++) {
            String expected = S7commCategories.functionNames().getOrDefault(code,
                String.format(Locale.ROOT, "FUNCTION_0x%02X", code));
            assertEquals(expected, S7commCategories.decodeOperation(S7commCategories.encodeOperation(code, null)));
        }
        assertEquals("FUNCTION_0x100", S7commCategories.decodeOperation(0x100));
        assertEquals("READ_VAR", S7commCategories.decodeOperation(0x04));
        assertEquals("SETUP_COMMUNICATION", S7commCategories.decodeOperation(0xF0));
    }

    @Test
    void theFunctionCodeWinsOverAFunctionName() {
        assertEquals(0x04, S7commCategories.encodeOperation(0x04, "PLC_STOP"));
    }

    @Test
    void aCodeLessKnownNameMapsBackToItsCodeInAnyCase() {
        // upstream upper-cases the name; the result equals a trained category.
        assertEquals(0x04, S7commCategories.encodeOperation(null, "read_var"));
        assertEquals(0x29, S7commCategories.encodeOperation(null, "PLC_STOP"));
        assertEquals("PLC_STOP", S7commCategories.decodeOperation(S7commCategories.encodeOperation(null, "plc_stop")));
    }

    @Test
    void aCodeLessUnknownNameIsTheUnseenCode() {
        // Includes upstream's own FUNCTION_0x.. spelling, which upper() turns
        // into FUNCTION_0X.. -- a string no code ever produces.
        assertEquals(S7commCategories.UNSEEN_NAME, S7commCategories.encodeOperation(null, "Function_0x12"));
        assertEquals(S7commCategories.UNSEEN_NAME, S7commCategories.encodeOperation(null, "Something Else"));
        assertEquals("__UNSEEN__", S7commCategories.decodeOperation(S7commCategories.UNSEEN_NAME));
    }

    @Test
    void anEmptyNameIsAbsentAsInPython() {
        // upstream: `if e.function_name:` -- "" is falsy.
        assertEquals(S7commCategories.MISSING, S7commCategories.encodeOperation(null, ""));
        assertEquals(S7commCategories.MISSING, S7commCategories.encodeOperation(null, null));
        assertEquals("__MISSING__", S7commCategories.decodeOperation(S7commCategories.MISSING));
    }

    @Test
    void rosctrDecodesToItsDecimalStringOrMissing() {
        // upstream _cat(rosctr_code): str(int), or "__MISSING__".
        assertEquals("1", S7commCategories.decodeRosctr(S7commCategories.encodeRosctr(1)));
        assertEquals("0", S7commCategories.decodeRosctr(S7commCategories.encodeRosctr(0)));
        assertEquals("7", S7commCategories.decodeRosctr(S7commCategories.encodeRosctr(7)));
        assertEquals("__MISSING__", S7commCategories.decodeRosctr(S7commCategories.encodeRosctr(null)));
    }

    @Test
    void negativeCodesOtherThanTheSentinelsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> S7commCategories.decodeRosctr(-2));
        assertThrows(IllegalArgumentException.class, () -> S7commCategories.decodeOperation(-3));
    }
}
