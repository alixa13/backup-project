package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;

import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ModbusFunctionCodeTest {

    // Required by the task brief.
    @Test
    void functionCode23IsBothReadAndWrite() {
        // READ_WRITE_MULTIPLE_REGISTERS. The upstream engine counts it in both
        // tallies, which is what makes read_ratio_10s and write_ratio_10s the
        // "READ/READ_WRITE" and "WRITE/READ_WRITE" fractions the contract names.
        assertTrue(ModbusFunctionCode.READ_FUNCTIONS.contains(23));
        assertTrue(ModbusFunctionCode.WRITE_FUNCTIONS.contains(23));
    }

    // Required by the task brief. Pins the exact sets against the source
    // file's own READ_CODES/WRITE_CODES (see ModbusFunctionCode's javadoc).
    @Test
    void theFrozenFunctionSetsAreExactlyTheUpstreamContracts() {
        assertEquals(Set.of(1, 2, 3, 4, 20, 24, 23), ModbusFunctionCode.READ_FUNCTIONS);
        assertEquals(Set.of(5, 6, 15, 16, 21, 22, 23), ModbusFunctionCode.WRITE_FUNCTIONS);
    }

    // Every one of the source file's 21 FUNCTION_NAME_TO_CODE entries, spot
    // checking both members of each of the two alias pairs so an aliasing
    // regression (one member kept, the other dropped) would be caught.
    @Test
    void everyTranscribedNameResolvesToItsTableCode() {
        assertEquals(OptionalInt.of(1), ModbusFunctionCode.codeOf("READ_COILS"));
        assertEquals(OptionalInt.of(2), ModbusFunctionCode.codeOf("READ_DISCRETE_INPUTS"));
        assertEquals(OptionalInt.of(3), ModbusFunctionCode.codeOf("READ_HOLDING_REGISTERS"));
        assertEquals(OptionalInt.of(4), ModbusFunctionCode.codeOf("READ_INPUT_REGISTERS"));
        assertEquals(OptionalInt.of(5), ModbusFunctionCode.codeOf("WRITE_SINGLE_COIL"));
        assertEquals(OptionalInt.of(6), ModbusFunctionCode.codeOf("WRITE_SINGLE_REGISTER"));
        assertEquals(OptionalInt.of(7), ModbusFunctionCode.codeOf("READ_EXCEPTION_STATUS"));
        assertEquals(OptionalInt.of(8), ModbusFunctionCode.codeOf("DIAGNOSTICS"));
        assertEquals(OptionalInt.of(11), ModbusFunctionCode.codeOf("GET_COMM_EVENT_COUNTER"));
        assertEquals(OptionalInt.of(11), ModbusFunctionCode.codeOf("GET_COMM_EVENT_COUNTERS"));
        assertEquals(OptionalInt.of(12), ModbusFunctionCode.codeOf("GET_COMM_EVENT_LOG"));
        assertEquals(OptionalInt.of(15), ModbusFunctionCode.codeOf("WRITE_MULTIPLE_COILS"));
        assertEquals(OptionalInt.of(16), ModbusFunctionCode.codeOf("WRITE_MULTIPLE_REGISTERS"));
        assertEquals(OptionalInt.of(17), ModbusFunctionCode.codeOf("REPORT_SLAVE_ID"));
        assertEquals(OptionalInt.of(20), ModbusFunctionCode.codeOf("READ_FILE_RECORD"));
        assertEquals(OptionalInt.of(21), ModbusFunctionCode.codeOf("WRITE_FILE_RECORD"));
        assertEquals(OptionalInt.of(22), ModbusFunctionCode.codeOf("MASK_WRITE_REGISTER"));
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("READ_WRITE_MULTIPLE_REGISTERS"));
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("READ_WRITE_REGISTER"));
        assertEquals(OptionalInt.of(24), ModbusFunctionCode.codeOf("READ_FIFO_QUEUE"));
        assertEquals(OptionalInt.of(43), ModbusFunctionCode.codeOf("ENCAPSULATED_INTERFACE_TRANSPORT"));
    }

    // Lowercase, mixed spacing/punctuation and a lowercase name must all
    // normalize the same as the canonical uppercase form -- ICSNPP configs
    // are not guaranteed to emit one exact casing.
    @Test
    void aLowercaseNameWithPunctuationNormalizesToTheSameCode() {
        assertEquals(OptionalInt.of(3), ModbusFunctionCode.codeOf("read-holding registers"));
        assertEquals(OptionalInt.of(3), ModbusFunctionCode.codeOf("Read_Holding_Registers"));
    }

    // A leading MODBUS_ prefix some builds emit must be stripped before the
    // table lookup, not treated as part of the name.
    @Test
    void aLeadingModbusPrefixIsStrippedBeforeTableLookup() {
        assertEquals(OptionalInt.of(1), ModbusFunctionCode.codeOf("MODBUS_READ_COILS"));
    }

    // A bare decimal number passes straight through, unchanged.
    @Test
    void aBareDecimalNumberPassesThrough() {
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("23"));
    }

    // A 0x-prefixed hex string becomes the number it encodes -- 0x17 is 23
    // decimal, READ_WRITE_MULTIPLE_REGISTERS's code.
    @Test
    void aHexPrefixedStringBecomesItsNumber() {
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("0x17"));
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("0X17"));
    }

    // FUNCTION_<n> and FC_<n> are both accepted as numeric aliases, resolved
    // without ever appearing in NAME_TO_CODE.
    @Test
    void functionAndFcNumericAliasesResolveToTheirDigitSuffix() {
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("FUNCTION_23"));
        assertEquals(OptionalInt.of(23), ModbusFunctionCode.codeOf("FC_23"));
    }

    // A name that resolves to nothing -- not numeric, not in the table, not
    // an FC_<n>/FUNCTION_<n> alias -- comes back empty rather than throwing
    // or guessing. ModbusEventMapper turns this into a MISSING_REQUIRED_FIELD
    // rejection.
    @Test
    void anUnresolvableNameComesBackEmpty() {
        assertEquals(OptionalInt.empty(), ModbusFunctionCode.codeOf("NOT_A_REAL_FUNCTION"));
    }

    // null and blank are both unresolvable, not a NullPointerException or an
    // ArrayIndexOutOfBoundsException -- func is validated non-null upstream
    // by JsonZeekModbusParser, but this registry must not assume that.
    @Test
    void nullAndBlankAreUnresolvable() {
        assertEquals(OptionalInt.empty(), ModbusFunctionCode.codeOf(null));
        assertEquals(OptionalInt.empty(), ModbusFunctionCode.codeOf("   "));
    }
}
