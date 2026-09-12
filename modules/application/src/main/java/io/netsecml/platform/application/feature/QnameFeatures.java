package io.netsecml.platform.application.feature;

import java.util.HashMap;
import java.util.Map;

// DNS query name features derived from the string's statistical properties.
// These measurements isolate the shape of a domain name without storing it—
// the raw string is excluded from every schema because it is user-identifying
// and cannot be frozen. A DGA-generated name and a human-chosen one differ
// sharply in entropy and digit ratio, and that is the signal.
public final class QnameFeatures {

    private QnameFeatures() {
        // Utility class, not instantiable.
    }

    // Character count in the input string.
    public static int length(String qname) {
        return qname.length();
    }

    // Base-2 Shannon entropy over character frequencies. A single repeated
    // character carries no information (entropy = 0). Four distinct characters
    // in equal proportion carry exactly 2 bits (entropy = 2.0).
    // Entropy = -Σ(p_i * log2(p_i)) where p_i is the frequency of character i.
    public static double shannonEntropy(String qname) {
        if (qname.isEmpty()) {
            return 0.0;
        }

        // Count occurrences of each character.
        Map<Character, Integer> frequencies = new HashMap<>();
        for (char c : qname.toCharArray()) {
            frequencies.put(c, frequencies.getOrDefault(c, 0) + 1);
        }

        // Compute Shannon entropy in base 2.
        double entropy = 0.0;
        int length = qname.length();
        for (int count : frequencies.values()) {
            double probability = (double) count / length;
            entropy -= probability * (Math.log(probability) / Math.log(2));
        }

        return entropy;
    }

    // Count dot-separated labels. A trailing dot is the DNS root label (FQDN)
    // and is not counted as a label. This distinction is critical: counting it
    // would shift every fully-qualified name's feature by exactly 1 against
    // every relative name's.
    public static int labelCount(String qname) {
        if (qname.isEmpty()) {
            return 0;
        }

        // Remove trailing dot if present (DNS root).
        String normalized = qname;
        if (qname.endsWith(".")) {
            normalized = qname.substring(0, qname.length() - 1);
        }

        if (normalized.isEmpty()) {
            return 0;
        }

        // Count dots + 1 gives the label count. No dots means 1 label.
        int dotCount = 0;
        for (char c : normalized.toCharArray()) {
            if (c == '.') {
                dotCount++;
            }
        }
        return dotCount + 1;
    }

    // Fraction of the string that is digits (0-9), as a ratio of total length.
    // Returns 0.0 for empty strings to avoid NaN, which would poison any model
    // that sees it in the feature vector.
    public static double digitRatio(String qname) {
        if (qname.isEmpty()) {
            return 0.0;
        }

        int digitCount = 0;
        for (char c : qname.toCharArray()) {
            if (Character.isDigit(c)) {
                digitCount++;
            }
        }

        return (double) digitCount / qname.length();
    }

    // Fraction of the string that is hyphens ('-'), as a ratio of total length.
    // Returns 0.0 for empty strings to avoid NaN, which would poison any model
    // that sees it in the feature vector.
    public static double hyphenRatio(String qname) {
        if (qname.isEmpty()) {
            return 0.0;
        }

        int hyphenCount = 0;
        for (char c : qname.toCharArray()) {
            if (c == '-') {
                hyphenCount++;
            }
        }

        return (double) hyphenCount / qname.length();
    }
}
