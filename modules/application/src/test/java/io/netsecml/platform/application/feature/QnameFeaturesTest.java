package io.netsecml.platform.application.feature;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class QnameFeaturesTest {

    // Entropy is the feature most likely to be quietly wrong, because a plausible
    // implementation returns a plausible number for every input. These are hand-
    // computable cases: a single repeated character carries no information, and
    // four distinct characters in equal proportion carry exactly 2 bits.
    @Test
    void entropyIsZeroForAUniformStringAndTwoBitsForFourEqualSymbols() {
        assertEquals(0.0, QnameFeatures.shannonEntropy("aaaa"), 1e-9);
        assertEquals(2.0, QnameFeatures.shannonEntropy("abcd"), 1e-9);
    }

    // A DGA domain is exactly the case this feature exists to separate from a
    // human-chosen one, so assert the ordering rather than only the arithmetic.
    @Test
    void entropyRanksARandomLabelAboveAnEnglishOne() {
        assertTrue(QnameFeatures.shannonEntropy("x7q2m9v4z1kd.com")
            > QnameFeatures.shannonEntropy("www.example.com"));
    }

    @Test
    void labelCountCountsDotSeparatedLabels() {
        assertEquals(3, QnameFeatures.labelCount("www.example.com"));
        assertEquals(1, QnameFeatures.labelCount("localhost"));

        // A trailing dot is the DNS root and is legal in a qname. It is not a
        // fourth label, and counting it as one would shift every fully-qualified
        // name's feature by exactly 1 against every relative name's.
        assertEquals(3, QnameFeatures.labelCount("www.example.com."));
    }

    @Test
    void ratiosAreOverTotalLengthAndSafeOnEmptyInput() {
        assertEquals(0.5, QnameFeatures.digitRatio("ab12"), 1e-9);
        assertEquals(0.25, QnameFeatures.hyphenRatio("a-bc"), 1e-9);

        // Division by zero would produce NaN, which serializes into the vector and
        // poisons any model that sees it. Empty is not reachable today (DnsEvent
        // requires a query) but the guard is one line and the failure is silent.
        assertEquals(0.0, QnameFeatures.digitRatio(""), 1e-9);

        // Both ratios, because the test's name says "ratios" -- asserting only
        // digitRatio would leave hyphenRatio's guard unproven while the name
        // claimed otherwise.
        assertEquals(0.0, QnameFeatures.hyphenRatio(""), 1e-9);

        // Character.isDigit would count this Arabic-Indic digit; the frozen
        // feature counts ASCII only, so a non-ASCII digit is not a digit here.
        assertEquals(0.0, QnameFeatures.digitRatio("\u0661\u0662"), 1e-9,
            "digitRatio is ASCII-only by design");
    }
}
