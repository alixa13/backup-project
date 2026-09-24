package io.netsecml.platform.adapter.kafka.mapper;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

// Upstream's field-value rules from two-models-info/S7___/kafka_source.py,
// ported for Jackson nodes: _first, _int_or_none, _bool_or_none, the plain
// int() its endpoint resolver applies to ports, and the float() it applies to
// ts. Each parse method throws IllegalArgumentException wherever the Python
// raises, and S7commEventMapper turns that into a DLQ reason.
final class S7commFieldValues {

    // Python int(text, 0) without underscore separators: an optional sign, then
    // 0x/0o/0b digits, a decimal without leading zeros, or zeros alone.
    private static final Pattern INT_LITERAL =
        Pattern.compile("[+-]?(0[xX][0-9a-fA-F]+|0[oO][0-7]+|0[bB][01]+|[1-9][0-9]*|0+)");

    // Python float(text) for finite decimal forms: digits with an optional
    // fraction and exponent. inf and nan are left out: int() raises on them.
    // Written so a digit run can match only one way -- the fraction needs its
    // '.' -- which keeps a failed match linear: a pattern like [0-9]+\\.?[0-9]*
    // can split one run of digits between its two quantifiers in n ways and
    // backtracks quadratically, so one malformed record of long digits would
    // stall the operator instead of reaching the DLQ.
    private static final Pattern DECIMAL =
        Pattern.compile("[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][+-]?[0-9]+)?");

    // Python int(text) for a port: base 10 only.
    private static final Pattern DECIMAL_INTEGER = Pattern.compile("[+-]?[0-9]+");

    // _bool_or_none's two spelling sets, compared after strip() and lower().
    private static final Set<String> TRUE_WORDS = Set.of("true", "t", "1", "yes", "y", "orig", "originator");
    private static final Set<String> FALSE_WORDS = Set.of("false", "f", "0", "no", "n", "resp", "responder");

    // Non-instantiable: every member is static.
    private S7commFieldValues() {
    }

    // _first: the first candidate that is present, not JSON null and not the
    // empty string. 0 and false are values, not absence.
    static JsonNode first(JsonNode... candidates) {
        for (JsonNode node : candidates) {
            if (isAbsent(node)) {
                continue;
            }
            if (node.isTextual() && node.textValue().isEmpty()) {
                continue;
            }
            return node;
        }
        return null;
    }

    // _int_or_none: absent/null/"" -> null; a boolean -> 0/1; a JSON integer ->
    // itself; a JSON float -> truncated; a string -> a Python integer literal,
    // else a decimal float truncated toward zero; anything else throws.
    static Long intOrNone(JsonNode node) {
        if (isAbsent(node)) {
            return null;
        }
        if (node.isBoolean()) {
            return node.booleanValue() ? 1L : 0L;
        }
        if (node.isIntegralNumber()) {
            if (!node.canConvertToLong()) {
                throw new IllegalArgumentException("integer out of range: " + node);
            }
            return node.longValue();
        }
        if (node.isFloatingPointNumber()) {
            return truncate(node.doubleValue());
        }
        if (!node.isTextual()) {
            throw new IllegalArgumentException("not an integer: " + node);
        }
        String text = node.textValue();
        if (text.isEmpty()) {
            return null;
        }
        String stripped = text.strip();
        if (INT_LITERAL.matcher(stripped).matches()) {
            return parseLiteral(stripped);
        }
        if (DECIMAL.matcher(stripped).matches()) {
            return truncate(Double.parseDouble(stripped));
        }
        throw new IllegalArgumentException("not an integer: \"" + text + "\"");
    }

    // _bool_or_none: absent/null/"" -> null; a boolean -> itself; a number ->
    // bool(int(value)); a string -> one of the two spelling sets; else throws.
    static Boolean boolOrNone(JsonNode node) {
        if (isAbsent(node)) {
            return null;
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isNumber()) {
            return truncate(node.doubleValue()) != 0L;
        }
        if (!node.isTextual()) {
            throw new IllegalArgumentException("not a boolean: " + node);
        }
        String text = node.textValue();
        if (text.isEmpty()) {
            return null;
        }
        String word = text.strip().toLowerCase(Locale.ROOT);
        if (TRUE_WORDS.contains(word)) {
            return true;
        }
        if (FALSE_WORDS.contains(word)) {
            return false;
        }
        throw new IllegalArgumentException("not a boolean: \"" + text + "\"");
    }

    // int(port) as _resolve_endpoints applies it: a JSON integer; a JSON float
    // truncated; a base-10 integer string. Range checks are the mapper's.
    static long port(JsonNode node) {
        if (node.isIntegralNumber() && node.canConvertToLong()) {
            return node.longValue();
        }
        if (node.isFloatingPointNumber()) {
            return truncate(node.doubleValue());
        }
        if (node.isTextual() && DECIMAL_INTEGER.matcher(node.textValue().strip()).matches()) {
            return Long.parseLong(node.textValue().strip());
        }
        throw new IllegalArgumentException("not a port: " + node);
    }

    // float(message["ts"]): a JSON number, or a finite decimal string.
    static double seconds(JsonNode node) {
        if (node.isNumber()) {
            return node.doubleValue();
        }
        if (node.isTextual() && DECIMAL.matcher(node.textValue().strip()).matches()) {
            return Double.parseDouble(node.textValue().strip());
        }
        throw new IllegalArgumentException("not a number: " + node);
    }

    // str(value) for a host: any scalar's text; an object or array throws.
    static String host(JsonNode node) {
        if (!node.isValueNode()) {
            throw new IllegalArgumentException("not a host: " + node);
        }
        return node.asText();
    }

    // Absent from the object, or JSON null.
    private static boolean isAbsent(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode();
    }

    // Python's int(float): truncation toward zero, and an error for a
    // non-finite value or one no long can hold.
    private static long truncate(double value) {
        if (!Double.isFinite(value) || Math.abs(value) >= 9.2e18) {
            throw new IllegalArgumentException("not a finite integer: " + value);
        }
        return (long) value;
    }

    // A matched INT_LITERAL: sign, then the prefix picks the radix.
    private static long parseLiteral(String literal) {
        boolean negative = literal.startsWith("-");
        String body = negative || literal.startsWith("+") ? literal.substring(1) : literal;
        String lower = body.toLowerCase(Locale.ROOT);
        long magnitude;
        if (lower.startsWith("0x")) {
            magnitude = Long.parseLong(body.substring(2), 16);
        } else if (lower.startsWith("0o")) {
            magnitude = Long.parseLong(body.substring(2), 8);
        } else if (lower.startsWith("0b")) {
            magnitude = Long.parseLong(body.substring(2), 2);
        } else {
            magnitude = Long.parseLong(body);
        }
        return negative ? -magnitude : magnitude;
    }
}
