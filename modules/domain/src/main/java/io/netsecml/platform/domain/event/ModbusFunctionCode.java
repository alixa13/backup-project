package io.netsecml.platform.domain.event;

import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// The frozen Modbus function-code registry: which numeric codes count as a
// READ or a WRITE for the upstream contract's read_ratio_10s/write_ratio_10s
// features, and the name->code table ModbusEventMapper uses to resolve
// func's string form (ICSNPP emits it as either a name, e.g.
// "READ_HOLDING_REGISTERS", or a bare/hex number, depending on build).
//
// READ_FUNCTIONS, WRITE_FUNCTIONS, NAME_TO_CODE and the codeOf(String)
// normalizer are transcribed from, and deliberately mirror, the model
// team's own two-models-info/modbus_/0761_rebuild_attack_characterization_corrected.py
// (FUNCTION_NAME_TO_CODE, READ_CODES/WRITE_CODES and _normalize_function_name)
// -- NOT re-derived from the Modbus specification by hand, so this registry
// cannot silently drift from what the upstream detector was actually trained
// against.
//
// NOT transcribed from that same file: FC_ONE_HOT = [1, 2, 3, 4, 5, 6, 15,
// 16, 23]. That is the Stage 2 attack-family one-hot set, and the model
// team's own README says that file "does not replace the frozen Stage-1
// detector feature contract". Stage 1's frozen contract
// (ModbusFeatureSchemaV1) one-hots exactly {1, 2, 3, 4, 5, 6} and folds
// every other code -- 15, 16 and 23 included -- into fc_other. That folding
// decision belongs to whatever builds the feature vector, not to this
// registry, which only classifies codes as read/write and resolves names to
// codes.
public final class ModbusFunctionCode {

    // READ_WRITE_MULTIPLE_REGISTERS (23) appears in BOTH sets: it performs a
    // read and a write in one PDU, and the upstream engine counts it in both
    // tallies -- confirmed identical to that file's own READ_CODES = {1, 2,
    // 3, 4, 20, 23, 24} and WRITE_CODES = {5, 6, 15, 16, 21, 22, 23} (order
    // differs here only because Set.of has no defined iteration order; the
    // members are the same).
    public static final Set<Integer> READ_FUNCTIONS = Set.of(1, 2, 3, 4, 20, 24, 23);
    public static final Set<Integer> WRITE_FUNCTIONS = Set.of(5, 6, 15, 16, 21, 22, 23);

    // Transcribed verbatim from FUNCTION_NAME_TO_CODE in the source file
    // named in this class's javadoc above. Two pairs of names alias the same
    // code: GET_COMM_EVENT_COUNTER/GET_COMM_EVENT_COUNTERS both mean 11, and
    // READ_WRITE_MULTIPLE_REGISTERS/READ_WRITE_REGISTER both mean 23.
    //
    // NOTE: the source file's own dict literal has exactly 21 entries, not
    // 22 -- counted by hand against the dict as written. Transcribed as
    // written (21 entries) rather than padded to a stated count that does
    // not match the source.
    private static final Map<String, Integer> NAME_TO_CODE = Map.ofEntries(
        Map.entry("READ_COILS", 1),
        Map.entry("READ_DISCRETE_INPUTS", 2),
        Map.entry("READ_HOLDING_REGISTERS", 3),
        Map.entry("READ_INPUT_REGISTERS", 4),
        Map.entry("WRITE_SINGLE_COIL", 5),
        Map.entry("WRITE_SINGLE_REGISTER", 6),
        Map.entry("READ_EXCEPTION_STATUS", 7),
        Map.entry("DIAGNOSTICS", 8),
        Map.entry("GET_COMM_EVENT_COUNTER", 11),
        Map.entry("GET_COMM_EVENT_COUNTERS", 11),
        Map.entry("GET_COMM_EVENT_LOG", 12),
        Map.entry("WRITE_MULTIPLE_COILS", 15),
        Map.entry("WRITE_MULTIPLE_REGISTERS", 16),
        Map.entry("REPORT_SLAVE_ID", 17),
        Map.entry("READ_FILE_RECORD", 20),
        Map.entry("WRITE_FILE_RECORD", 21),
        Map.entry("MASK_WRITE_REGISTER", 22),
        Map.entry("READ_WRITE_MULTIPLE_REGISTERS", 23),
        Map.entry("READ_WRITE_REGISTER", 23),
        Map.entry("READ_FIFO_QUEUE", 24),
        Map.entry("ENCAPSULATED_INTERFACE_TRANSPORT", 43));

    // Mirrors the source file's own final fallback:
    // re.search(r"(?:FC|FUNCTION)_?(\d+)", norm) -- a SEARCH, not an anchored
    // match, so "FC23" and "SOME_FC_23_ALIAS" both resolve via this pattern
    // too, not only a bare "FC_23".
    private static final Pattern FC_OR_FUNCTION_SUFFIX = Pattern.compile("(?:FC|FUNCTION)_?(\\d+)");

    private ModbusFunctionCode() {
    }

    // Resolves func's wire value -- a name ("READ_HOLDING_REGISTERS"), a
    // bare or 0x-prefixed hex number, or an FC_<n>/FUNCTION_<n> alias -- to
    // its numeric code, in the exact order the source file's own
    // _normalize_function_name resolves it. Empty means unresolved;
    // ModbusEventMapper turns that into a MISSING_REQUIRED_FIELD rejection,
    // never a guess -- function_code is required and the upstream engine
    // hard-fails on it too.
    public static OptionalInt codeOf(String raw) {
        if (raw == null) {
            return OptionalInt.empty();
        }
        String text = raw.strip();
        if (text.isEmpty()) {
            return OptionalInt.empty();
        }

        // 1. A value that already IS a number passes through unchanged,
        // including a 0x-prefixed hex string and a decimal with a
        // fractional part (truncated toward zero, matching the source
        // file's own int(float(text))).
        OptionalInt numeric = parseNumeric(text);
        if (numeric.isPresent()) {
            return numeric;
        }

        // 2. Otherwise normalize: uppercase, collapse every run of
        // non-alphanumeric characters (spaces, hyphens, slashes...) to a
        // single underscore, then trim any leading/trailing underscore the
        // collapse produced (mirrors the source file's own .strip("_")).
        String normalized = text.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]+", "_");
        normalized = stripUnderscores(normalized);
        if (normalized.isEmpty()) {
            return OptionalInt.empty();
        }

        // 3. Strip a leading MODBUS_ some builds prefix every function name
        // with.
        if (normalized.startsWith("MODBUS_")) {
            normalized = normalized.substring("MODBUS_".length());
        }

        // 4. FUNCTION_<digits> exactly: a trailing digit run after this
        // specific prefix IS the code, checked before the name table so a
        // numeric-suffixed alias never has to appear in NAME_TO_CODE.
        if (normalized.startsWith("FUNCTION_")) {
            String suffix = normalized.substring("FUNCTION_".length());
            if (!suffix.isEmpty() && isAllDigits(suffix)) {
                OptionalInt fromSuffix = parseIntSafe(suffix);
                if (fromSuffix.isPresent()) {
                    return fromSuffix;
                }
            }
        }

        // 5. The name table itself.
        Integer tableCode = NAME_TO_CODE.get(normalized);
        if (tableCode != null) {
            return OptionalInt.of(tableCode);
        }

        // 6. Last-resort fallback: FC_<n> or FUNCTION_<n> found anywhere in
        // the normalized string, exactly like the source file's own
        // trailing re.search fallback.
        Matcher matcher = FC_OR_FUNCTION_SUFFIX.matcher(normalized);
        if (matcher.find()) {
            return parseIntSafe(matcher.group(1));
        }

        return OptionalInt.empty();
    }

    private static String stripUnderscores(String s) {
        int start = 0;
        int end = s.length();
        while (start < end && s.charAt(start) == '_') {
            start++;
        }
        while (end > start && s.charAt(end - 1) == '_') {
            end--;
        }
        return s.substring(start, end);
    }

    private static boolean isAllDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static OptionalInt parseNumeric(String text) {
        try {
            String lower = text.toLowerCase(Locale.ROOT);
            if (lower.startsWith("0x")) {
                return OptionalInt.of(Integer.parseInt(text.substring(2), 16));
            }
            double parsed = Double.parseDouble(text);
            if (Double.isNaN(parsed) || Double.isInfinite(parsed)) {
                return OptionalInt.empty();
            }
            return OptionalInt.of((int) parsed);
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
    }

    private static OptionalInt parseIntSafe(String digits) {
        try {
            return OptionalInt.of(Integer.parseInt(digits));
        } catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
    }
}
