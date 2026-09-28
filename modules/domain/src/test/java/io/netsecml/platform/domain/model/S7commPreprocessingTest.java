package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.feature.S7commCategories;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// preprocessor_contract.json's transform on a hand-built contract whose outputs
// are worked out by hand. The real contract's numbers are pinned against the
// released Python preprocessor in S7commDetectorBundleLoaderTest.
class S7commPreprocessingTest {

    // c0: unbounded (median 1, center 1, scale 2); c1: bounded to [0, 1]
    // (median 0.5); c2..c11: unbounded identity. Categories spelled as v2's
    // contract spells them (S7commCategories' own strings).
    private static S7commPreprocessing contract() {
        return contract(List.of("FUNCTION_0x00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
    }

    private static S7commPreprocessing contract(List<String> operations) {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        cs.add(new S7commPreprocessing.Continuous("c0", 1.0, 1.0, 2.0, Double.NaN, Double.NaN));
        cs.add(new S7commPreprocessing.Continuous("c1", 0.5, 0.0, 1.0, 0.0, 1.0));
        for (int i = 2; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("c" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        return new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"), operations);
    }

    // A raw vector with one value set.
    private static float[] raw(int index, float value) {
        float[] raw = new float[16];
        raw[index] = value;
        return raw;
    }

    @Test
    void anUnboundedFeatureIsRobustScaled() {
        assertEquals(2f, contract().apply(raw(0, 5f))[0], "(5 - 1) / 2");
    }

    @Test
    void anUnboundedFeatureIsClippedToTheTransformedClip() {
        assertEquals(20f, contract().apply(raw(0, 1000f))[0]);
        assertEquals(-20f, contract().apply(raw(2, -50f))[2]);
        assertEquals(20f, contract().apply(raw(2, Float.POSITIVE_INFINITY))[2]);
    }

    @Test
    void aBoundedFeatureIsClippedToItsRangeAndNeverScaled() {
        assertEquals(1f, contract().apply(raw(1, 1.7f))[1]);
        assertEquals(0f, contract().apply(raw(1, -0.2f))[1]);
        assertEquals(0.25f, contract().apply(raw(1, 0.25f))[1]);
    }

    @Test
    void aMissingContinuousValueTakesItsMedian() {
        assertEquals(0f, contract().apply(raw(0, Float.NaN))[0], "(median 1 - 1) / 2");
        assertEquals(0.5f, contract().apply(raw(1, Float.NaN))[1]);
    }

    @Test
    void binaryValuesPassThroughAndAMissingOneIsZero() {
        assertEquals(1f, contract().apply(raw(12, 1f))[12]);
        assertEquals(0f, contract().apply(raw(13, Float.NaN))[13]);
    }

    // Columns 14-16: s7_rosctr 1, 3, 7; anything else is all zeros.
    @Test
    void theRosctrCodeIsOneHot() {
        float[] three = contract().apply(raw(14, 3f));
        assertEquals(List.of(0f, 1f, 0f), List.of(three[14], three[15], three[16]));
        float[] ack = contract().apply(raw(14, 2f));
        assertEquals(List.of(0f, 0f, 0f), List.of(ack[14], ack[15], ack[16]), "ROSCTR 2 was never trained on");
        float[] missing = contract().apply(raw(14, S7commCategories.MISSING));
        assertEquals(List.of(0f, 0f, 0f), List.of(missing[14], missing[15], missing[16]));
    }

    // Columns 17-20: FUNCTION_0x00, READ_VAR, SETUP_COMMUNICATION, WRITE_VAR.
    // The operation is one-hot exactly as S7commCategories decodes it (scoring
    // spec amendment A1: v2 was trained on these strings, no upper-casing).
    @Test
    void theOperationIsOneHotAsDecoded() {
        assertEquals(1f, contract().apply(raw(15, 4f))[18], "READ_VAR");
        assertEquals(1f, contract().apply(raw(15, 0f))[17], "code 0 decodes to FUNCTION_0x00 exactly");
        // A contract spelling it FUNCTION_0X00 never matches: nothing is upper-cased.
        float[] upper = contract(List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"))
            .apply(raw(15, 0f));
        assertEquals(List.of(0f, 0f, 0f, 0f), List.of(upper[17], upper[18], upper[19], upper[20]));
        for (float code : new float[]{0x29, S7commCategories.UNSEEN_NAME, S7commCategories.MISSING}) {
            float[] out = contract().apply(raw(15, code));
            assertEquals(List.of(0f, 0f, 0f, 0f), List.of(out[17], out[18], out[19], out[20]), "code " + code);
        }
    }

    @Test
    void theWidthAndTheColumnNamesFollowTheContract() {
        S7commPreprocessing p = contract();
        assertEquals(21, p.width());
        List<String> names = p.transformedFeatureOrder();
        assertEquals("continuous__c0", names.get(0));
        assertEquals("binary__b0", names.get(12));
        assertEquals("categorical__s7_rosctr_1", names.get(14));
        assertEquals("categorical__s7_operation_FUNCTION_0x00", names.get(17));
        assertEquals("categorical__s7_operation_WRITE_VAR", names.get(20));
        assertEquals(List.of("s7_rosctr", "s7_operation"), p.rawFeatureOrder().subList(14, 16));
    }

    // upstream's transformed_columns_for_raw_features: every column a listed
    // raw feature produces weighs 0.
    @Test
    void theScoreWeightsZeroEveryColumnOfAListedFeature() {
        float[] weights = contract().scoreWeights(List.of("s7_operation"));
        for (int i = 0; i < 17; i++) {
            assertEquals(1f, weights[i], "column " + i);
        }
        for (int i = 17; i < 21; i++) {
            assertEquals(0f, weights[i], "column " + i);
        }
        assertEquals(0f, contract().scoreWeights(List.of("c1"))[1]);
        assertThrows(IllegalArgumentException.class, () -> contract().scoreWeights(List.of("no_such_feature")));
    }

    @Test
    void theShapeIsChecked() {
        assertThrows(IllegalArgumentException.class, () -> contract().apply(new float[15]));
        List<S7commPreprocessing.Continuous> eleven = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            eleven.add(new S7commPreprocessing.Continuous("c" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commPreprocessing(eleven, List.of("b0", "b1"),
            20.0, List.of("1"), List.of("READ_VAR")));
        assertThrows(IllegalArgumentException.class,
            () -> new S7commPreprocessing.Continuous("c", 0.0, 0.0, 0.0, Double.NaN, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> new S7commPreprocessing.Continuous("c", 0.0, 0.0, 1.0,
            1.0, 0.0));
    }
}
