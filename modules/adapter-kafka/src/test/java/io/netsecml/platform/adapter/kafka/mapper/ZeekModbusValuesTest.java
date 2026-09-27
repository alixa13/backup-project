package io.netsecml.platform.adapter.kafka.mapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

// icsnpp-modbus v1.0.0's `values` string, in every shape the real ICSNPP
// sample (tests/fixtures/zeek/) and the plan's Review Focus 2 name.
class ZeekModbusValuesTest {

    private static final double[] ABSENT = new double[0];

    @Test
    void registersParseAsNumbers() {
        assertArrayEquals(new double[]{170}, ZeekModbusValues.parse("170"));
        assertArrayEquals(new double[]{170, 170, 187, 204, 61316},
            ZeekModbusValues.parse("170,170,187,204,61316"));
    }

    @Test
    void coilsParseAsOneAndZero() {
        assertArrayEquals(new double[]{1, 0, 0, 1}, ZeekModbusValues.parse("T,F,F,T"));
        assertArrayEquals(new double[]{1}, ZeekModbusValues.parse("T"));
    }

    @Test
    void spacesAroundATokenAreIgnored() {
        assertArrayEquals(new double[]{170, 171}, ZeekModbusValues.parse("170, 171"));
    }

    @Test
    void anAbsentOrBlankStringIsAbsent() {
        assertArrayEquals(ABSENT, ZeekModbusValues.parse(null));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse(""));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("   "));
    }

    // Anything not wholly numeric is absent, never a rejection: upstream's
    // parse_numeric_vector cannot read these either.
    @Test
    void aStringThatIsNotWhollyNumericIsAbsent() {
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("\\x00\\x00"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("see modbus_mask_write_register.log"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("ILLEGAL_DATA_ADDRESS"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("170,x"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("170,"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("1.5"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("-3"));
    }

    // Eleven digits exceed any Modbus register and could overflow a parse.
    @Test
    void anOverlongNumberIsAbsent() {
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("12345678901"));
    }
}
