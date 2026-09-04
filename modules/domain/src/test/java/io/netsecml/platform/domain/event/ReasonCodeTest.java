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
    // MALFORMED_JSON is the only one: JsonZeekConnParser is the sole PARSE-stage
    // producer and it never returns anything else (see
    // JsonZeekConnParserTest.missingRequiredFieldFailsToParse, which asserts
    // MALFORMED_JSON even for a structurally missing field).
    @Test
    void parseFailuresAreParseStage() {
        assertEquals(ReasonCode.Stage.PARSE, ReasonCode.MALFORMED_JSON.stage());
    }

    // Map-stage codes are raised by domain validation after a successful parse.
    // MISSING_REQUIRED_FIELD belongs here, not in PARSE: it is EventMapper's own
    // required-field check (id, id_orig_h/id_resp_h, conn_state), raised only
    // after a ZeekConnEvent has already parsed successfully — JsonZeekConnParser
    // itself never returns this code. Do not move it back to the PARSE test
    // above; that was a real bug caught in Task 4 review (DLQ rows for these
    // three MAP-stage rejections were shipping with stage=PARSE).
    @Test
    void domainValidationFailuresAreMapStage() {
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.MISSING_REQUIRED_FIELD.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_TIMESTAMP.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_PORT.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_COUNTER.stage());
    }
}
