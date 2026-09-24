package io.netsecml.platform.domain.feature;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

// The two categorical features of s7comm-feature-v1 as numeric codes. Every
// feature vector on this platform is float32, but upstream's categories are
// strings (_cat and _operation in two-models-info/S7___/customer_icsnpp_enriched_builder.py).
// This class is the ONE place that turns a record's fields into a code, and a
// code back into upstream's exact category string. The extractor encodes; a
// scorer decodes, to feed the model team's ONNX graph, whose fitted one-hot
// (Stage 1) and ordinal (Stage 2) encoders were trained on the strings. See
// contracts/features/s7comm-feature-schema-v1.json's categoricalCodes and
// docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 4.2.
public final class S7commCategories {

    // The code of an absent field; decodes to upstream's "__MISSING__".
    public static final int MISSING = -1;

    // A function_name with no function code that is not one of FUNCTION_NAMES.
    // Upstream uses the upper-cased name itself as the category, which a float
    // cannot carry; it decodes to UNSEEN_CATEGORY, which no trained encoder
    // knows -- the spec's ruling R6 records when that is not exact.
    public static final int UNSEEN_NAME = -2;

    public static final String MISSING_CATEGORY = "__MISSING__";
    public static final String UNSEEN_CATEGORY = "__UNSEEN__";

    // FUNCTION_NAMES from two-models-info/S7___/s7_parser.py, verbatim.
    private static final Map<Integer, String> FUNCTION_NAMES = Map.ofEntries(
        Map.entry(0x04, "READ_VAR"),
        Map.entry(0x05, "WRITE_VAR"),
        Map.entry(0x1A, "REQUEST_DOWNLOAD"),
        Map.entry(0x1B, "DOWNLOAD_BLOCK"),
        Map.entry(0x1C, "DOWNLOAD_ENDED"),
        Map.entry(0x1D, "START_UPLOAD"),
        Map.entry(0x1E, "UPLOAD"),
        Map.entry(0x1F, "END_UPLOAD"),
        Map.entry(0x28, "PI_SERVICE"),
        Map.entry(0x29, "PLC_STOP"),
        Map.entry(0xF0, "SETUP_COMMUNICATION"));

    // The inverse, for mapping a code-less function_name back to its code.
    private static final Map<String, Integer> CODE_BY_NAME = invert(FUNCTION_NAMES);

    // Non-instantiable: every member is static.
    private S7commCategories() {
    }

    private static Map<String, Integer> invert(Map<Integer, String> names) {
        Map<String, Integer> inverse = new HashMap<>();
        names.forEach((code, name) -> inverse.put(name, code));
        return Map.copyOf(inverse);
    }

    // The 11 named function codes, for the contract-file drift test and a
    // scorer's own checks.
    public static Map<Integer, String> functionNames() {
        return FUNCTION_NAMES;
    }

    // upstream _cat(rosctr_code): absent -> __MISSING__, else the code itself.
    public static int encodeRosctr(Integer rosctrCode) {
        return rosctrCode == null ? MISSING : rosctrCode;
    }

    // upstream _operation: a function code always wins; without one, a
    // non-empty function_name (Python's `if e.function_name:`) upper-cased;
    // otherwise __MISSING__. Only the 11 FUNCTION_NAMES values can be carried
    // as codes, so any other name becomes UNSEEN_NAME.
    public static int encodeOperation(Integer functionCode, String functionName) {
        if (functionCode != null) {
            return functionCode;
        }
        if (functionName != null && !functionName.isEmpty()) {
            Integer code = CODE_BY_NAME.get(functionName.toUpperCase(Locale.ROOT));
            return code != null ? code : UNSEEN_NAME;
        }
        return MISSING;
    }

    // Code -> upstream's s7_rosctr string: "__MISSING__" or str(code).
    public static String decodeRosctr(int code) {
        if (code == MISSING) {
            return MISSING_CATEGORY;
        }
        if (code < 0) {
            throw new IllegalArgumentException("not an s7_rosctr code: " + code);
        }
        return Integer.toString(code);
    }

    // Code -> upstream's s7_operation string: the sentinels, a FUNCTION_NAMES
    // name, or f"FUNCTION_0x{code:02X}".
    public static String decodeOperation(int code) {
        if (code == MISSING) {
            return MISSING_CATEGORY;
        }
        if (code == UNSEEN_NAME) {
            return UNSEEN_CATEGORY;
        }
        if (code < 0) {
            throw new IllegalArgumentException("not an s7_operation code: " + code);
        }
        String name = FUNCTION_NAMES.get(code);
        return name != null ? name : String.format(Locale.ROOT, "FUNCTION_0x%02X", code);
    }
}
