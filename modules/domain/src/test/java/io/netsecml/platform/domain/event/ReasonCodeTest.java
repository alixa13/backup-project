package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ReasonCodeTest {
    // Guard test: a new ReasonCode added without a stage fails here rather than
    // silently writing a null into invalid_events.stage months later.
    @Test
    void everyReasonCodeDeclaresAStage() {
        for (ReasonCode code : ReasonCode.values()) {
            assertNotNull(code.stage(), code + " must declare a stage");
        }
    }

    // Parse-stage codes are the ones raised before a ZeekConnEvent exists.
    @Test
    void parseFailuresAreParseStage() {
        assertEquals(ReasonCode.Stage.PARSE, ReasonCode.MALFORMED_JSON.stage());
        assertEquals(ReasonCode.Stage.PARSE, ReasonCode.MISSING_REQUIRED_FIELD.stage());
    }

    // Map-stage codes are raised by domain validation after a successful parse.
    @Test
    void domainValidationFailuresAreMapStage() {
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_TIMESTAMP.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_PORT.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_COUNTER.stage());
    }
}
