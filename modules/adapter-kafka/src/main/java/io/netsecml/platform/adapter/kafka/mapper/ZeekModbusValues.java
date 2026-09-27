package io.netsecml.platform.adapter.kafka.mapper;

// icsnpp-modbus v1.0.0 writes a record's register or coil values into one
// `values` string: comma-separated decimals for registers ("170,171"), T/F for
// coils and discrete inputs ("T,F,F"). The detector was trained on numeric
// arrays built by upstream's own capture adapter, so this turns the string into
// the same numbers (docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md
// section 2.1, F1). A string that is not wholly numeric -- "\x00\x00",
// "see modbus_mask_write_register.log", an exception name, "" -- is absent:
// upstream's parse_numeric_vector cannot read such strings either, so its
// training data never held them.
public final class ZeekModbusValues {

    private static final double[] ABSENT = new double[0];

    // Ten digits hold any Modbus register (at most 65535) with room to spare,
    // and cannot overflow Long.parseLong.
    private static final int MAX_DIGITS = 10;

    private ZeekModbusValues() {
    }

    // The values as numbers, or an empty array when absent or not wholly numeric.
    public static double[] parse(String values) {
        // Absent or blank: no values at all.
        if (values == null || values.isBlank()) {
            return ABSENT;
        }
        // limit -1 keeps a trailing empty token, so "170," is not wholly numeric.
        String[] tokens = values.split(",", -1);
        double[] result = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            if (token.equals("T")) {
                // A coil or discrete input that is on.
                result[i] = 1.0;
            } else if (token.equals("F")) {
                // A coil or discrete input that is off.
                result[i] = 0.0;
            } else if (isUnsignedDecimal(token)) {
                // A register value.
                result[i] = Long.parseLong(token);
            } else {
                // One non-numeric token makes the whole string absent.
                return ABSENT;
            }
        }
        return result;
    }

    // ASCII digits only (Character.isDigit would admit other scripts' digits).
    private static boolean isUnsignedDecimal(String token) {
        if (token.isEmpty() || token.length() > MAX_DIGITS) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}
