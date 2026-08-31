package io.netsecml.platform.domain.event;

// Why a record is rejected, and which pipeline stage rejected it.
// The archive job writes stage() into invalid_events.stage, so the mapping is
// domain knowledge rather than something an adapter re-derives from the name.
public enum ReasonCode {
    // PARSE: raised before a ZeekConnEvent exists — the bytes are not a usable
    // source record at all.
    MALFORMED_JSON(Stage.PARSE),
    MISSING_REQUIRED_FIELD(Stage.PARSE),

    // MAP: the DTO parsed cleanly, but domain validation refused a value.
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
