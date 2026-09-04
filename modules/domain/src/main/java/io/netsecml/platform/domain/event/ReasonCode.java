package io.netsecml.platform.domain.event;

// Why a record is rejected, and which pipeline stage rejected it.
// The archive job writes stage() into invalid_events.stage, so the mapping is
// domain knowledge rather than something an adapter re-derives from the name.
public enum ReasonCode {
    // PARSE: JsonZeekConnParser is the only PARSE-stage producer, and
    // MALFORMED_JSON is the only reason it ever returns — a structurally
    // invalid payload and one missing a Jackson-required field both surface as
    // the same Jackson exception, so no ZeekConnEvent was produced either way.
    MALFORMED_JSON(Stage.PARSE),

    // MAP: the DTO parsed cleanly, so everything below is raised by domain
    // validation (EventMapper) after that point. MISSING_REQUIRED_FIELD is
    // EventMapper's own required-field check — id, id_orig_h/id_resp_h,
    // conn_state — for fields Jackson's required=true cannot enforce
    // structurally; it is MAP, never PARSE, because the parser itself never
    // raises it.
    MISSING_REQUIRED_FIELD(Stage.MAP),
    INVALID_TIMESTAMP(Stage.MAP),
    INVALID_PORT(Stage.MAP),
    INVALID_COUNTER(Stage.MAP);

    // The two points in the pipeline where a record can be rejected.
    public enum Stage { PARSE, MAP }

    // Immutable per constant — set once by the constructor below.
    private final Stage stage;

    // Enum constructor: each constant above supplies its own stage.
    ReasonCode(Stage stage) {
        this.stage = stage;
    }

    // Exposes the stage assigned by the constructor above.
    public Stage stage() {
        return stage;
    }
}
