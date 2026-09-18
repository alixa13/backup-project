package io.netsecml.platform.application.feature;

import java.util.Arrays;

// DNS query name features derived from the string's statistical properties.
// These measurements isolate the shape of a domain name without storing it --
// the raw string is excluded from every schema because it is user-identifying
// and cannot be frozen. A DGA-generated name and a human-chosen one differ
// sharply in entropy and digit ratio, and that is the signal.
public final class QnameFeatures {

    private QnameFeatures() {
        // Utility class, not instantiable.
    }

    // Character count in the input string. Counts UTF-16 code units
    // (String.length()'s own unit), not Unicode code points -- a non-BMP
    // character (a surrogate pair) counts as 2. DNS labels are LDH-restricted
    // to ASCII, so this does not matter for a well-formed name.
    public static int length(String qname) {
        return qname.length();
    }

    // Base-2 Shannon entropy over character frequencies. A single repeated
    // character carries no information (entropy = 0). Four distinct characters
    // in equal proportion carry exactly 2 bits (entropy = 2.0).
    // Entropy = -Σ(p_i * log2(p_i)) where p_i is the frequency of character i.
    // "Character" here means UTF-16 code unit (char), the same unit length()
    // above counts in -- a non-BMP character's two surrogate code units are
    // tallied as two distinct symbols, not as one character.
    public static double shannonEntropy(String qname) {
        if (qname.isEmpty()) {
            return 0.0;
        }

        // Sort a copy and count runs, rather than tallying into a Map. This runs
        // once per DNS record on the online scoring path, and a
        // Map<Character, Integer> boxes a Character and an Integer for every
        // character of every name queried. Sorting is O(n log n) against the
        // Map's O(n), but n is a domain name -- 253 characters at the absolute
        // limit -- so the constant factor of boxing and hashing dominates the
        // asymptotics at every size that actually occurs. A fixed-size frequency
        // array would be faster still, but only by assuming an alphabet; this
        // stays correct for any char.
        char[] sorted = qname.toCharArray();
        Arrays.sort(sorted);

        double entropy = 0.0;
        int length = sorted.length;
        int runStart = 0;
        for (int i = 1; i <= length; i++) {
            // A run ends at the first different character, and at the end of the
            // array.
            if (i == length || sorted[i] != sorted[runStart]) {
                double probability = (double) (i - runStart) / length;
                entropy -= probability * (Math.log(probability) / Math.log(2));
                runStart = i;
            }
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

    // Fraction of the string that is ASCII digits, as a ratio of total length.
    // Returns 0.0 for empty strings to avoid NaN, which would poison any model
    // that sees it in the feature vector.
    //
    // Deliberately NOT Character.isDigit, which also matches Devanagari and
    // Arabic-Indic digits. DNS labels are LDH-restricted to ASCII and an
    // internationalised name arrives Punycode-encoded, so a Unicode digit
    // reaching this method is malformed input -- which is exactly the traffic
    // this platform exists to score. A frozen feature must behave the same way
    // on adversarial input as on ordinary input, so the range is explicit.
    public static double digitRatio(String qname) {
        if (qname.isEmpty()) {
            return 0.0;
        }

        int digitCount = 0;
        for (char c : qname.toCharArray()) {
            if (c >= '0' && c <= '9') {
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
