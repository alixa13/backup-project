package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MappingResultTest {
    @Test
    void validResultExposesValue() {
        MappingResult<String> result = MappingResult.valid("ok");
        assertTrue(result.isValid());
        assertEquals("ok", result.value());
        assertThrows(IllegalStateException.class, result::reason);
    }

    @Test
    void invalidResultExposesReasonAndDetail() {
        MappingResult<String> result = MappingResult.invalid(ReasonCode.INVALID_PORT, "port -1 out of range");
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_PORT, result.reason());
        assertEquals("port -1 out of range", result.detail());
        assertThrows(IllegalStateException.class, result::value);
    }

    @Test
    void validRejectsNullValue() {
        assertThrows(IllegalArgumentException.class, () -> MappingResult.valid(null));
    }
}
