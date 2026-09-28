package io.netsecml.platform.domain.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The seven transform_definitions of modbus_preprocessing_contract_v1, on a
// hand-built contract whose expected outputs are worked out by hand. The real
// contract's numbers are pinned against Python in SequenceDetectorBundleLoaderTest.
class ModbusPreprocessingTest {

    // Feature 0 is the mask every conditional feature below points at.
    private static ModbusPreprocessing contract() {
        return new ModbusPreprocessing(List.of(
            new FeaturePreprocessing("mask", PreprocessingPolicy.PASSTHROUGH_BINARY, -1, Double.NaN, Double.NaN),
            new FeaturePreprocessing("bounded", PreprocessingPolicy.PASSTHROUGH_BOUNDED_OR_CONSTANT, -1, Double.NaN, Double.NaN),
            new FeaturePreprocessing("gstd", PreprocessingPolicy.GLOBAL_STANDARD, -1, 2.0, 4.0),
            new FeaturePreprocessing("glog", PreprocessingPolicy.GLOBAL_LOG1P_ONLY, -1, Double.NaN, Double.NaN),
            new FeaturePreprocessing("cstd", PreprocessingPolicy.CONDITIONAL_STANDARD, 0, 2.0, 4.0),
            new FeaturePreprocessing("clog", PreprocessingPolicy.CONDITIONAL_LOG1P_ONLY, 0, Double.NaN, Double.NaN),
            new FeaturePreprocessing("clogstd", PreprocessingPolicy.CONDITIONAL_LOG1P_THEN_STANDARD, 0, 1.0, 2.0)));
    }

    @Test
    void everyPolicyAppliesItsFormulaWhereTheMaskIsOne() {
        float[] out = contract().apply(new float[]{1f, 7f, 10f, 3f, 6f, 3f, 0f});
        assertEquals(1f, out[0], "passthrough binary");
        assertEquals(7f, out[1], "passthrough bounded");
        assertEquals(2f, out[2], "(10 - 2) / 4");
        assertEquals((float) Math.log1p(3), out[3], "log1p(3)");
        assertEquals(1f, out[4], "(6 - 2) / 4");
        assertEquals((float) Math.log1p(3), out[5], "log1p(3)");
        assertEquals(-0.5f, out[6], "(log1p(0) - 1) / 2");
    }

    // A conditional feature whose mask is 0 is exactly 0.0, whatever its value.
    @Test
    void aConditionalFeatureWhoseMaskIsZeroIsExactlyZero() {
        float[] out = contract().apply(new float[]{0f, 7f, 10f, 3f, 6f, 3f, 5f});
        assertEquals(0f, out[4]);
        assertEquals(0f, out[5]);
        assertEquals(0f, out[6]);
    }

    // The contract never clips; a value outside log1p's domain comes out
    // non-finite, and the caller (the use case) turns that into UNSCORABLE.
    @Test
    void aValueOutsideLog1psDomainComesOutNonFinite() {
        float[] out = contract().apply(new float[]{1f, 0f, 0f, -2f, 0f, 0f, 0f});
        assertTrue(Float.isNaN(out[3]));
    }

    @Test
    void theWidthMustMatch() {
        assertThrows(IllegalArgumentException.class, () -> contract().apply(new float[3]));
    }

    @Test
    void aStandardizedPolicyNeedsAPositiveStd() {
        assertThrows(IllegalArgumentException.class, () -> new ModbusPreprocessing(List.of(
            new FeaturePreprocessing("gstd", PreprocessingPolicy.GLOBAL_STANDARD, -1, 2.0, 0.0))));
    }

    @Test
    void aConditionalPolicyNeedsAMaskInsideTheVector() {
        assertThrows(IllegalArgumentException.class, () -> new ModbusPreprocessing(List.of(
            new FeaturePreprocessing("clog", PreprocessingPolicy.CONDITIONAL_LOG1P_ONLY, 5, Double.NaN, Double.NaN))));
    }

    @Test
    void theFeatureOrderIsTheContractsOrder() {
        assertEquals(List.of("mask", "bounded", "gstd", "glog", "cstd", "clog", "clogstd"), contract().featureOrder());
        assertEquals(7, contract().width());
    }
}
