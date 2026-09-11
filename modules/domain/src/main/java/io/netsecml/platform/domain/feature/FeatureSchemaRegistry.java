package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import java.util.Map;

// Resolves a feature schema two ways: from a log type at wiring time, and from a
// schema id held by something that already has a record.
//
// byId is what makes an archived row self-describing -- training reads the
// schemaId off the row and resolves the definitions, rather than assuming which
// schema a table's rows follow.
//
// A static map over the frozen schema constants: no framework, no configuration,
// no I/O. Adding a log type means adding one entry here, and the test that walks
// LogType.values() fails until you do.
public final class FeatureSchemaRegistry {

    // Only conn today. An entry is added when a log type has a frozen schema
    // behind it -- the same rule LogType states about its own constants.
    private static final Map<LogType, FeatureSchema> BY_LOG_TYPE =
        Map.of(LogType.CONN, ConnFeatureSchemaV1.SCHEMA);

    // Derived from the same source, so the two lookups cannot disagree about
    // which schema a log type produces.
    private static final Map<String, FeatureSchema> BY_ID =
        BY_LOG_TYPE.values().stream()
            .collect(java.util.stream.Collectors.toUnmodifiableMap(FeatureSchema::id, schema -> schema));

    // Non-instantiable: every member is static.
    private FeatureSchemaRegistry() {
    }

    // Throws rather than returning null: an unresolvable schema means the
    // deployment is wrong, and failing at wiring time is cheaper than failing per
    // record once traffic arrives.
    public static FeatureSchema byLogType(LogType logType) {
        FeatureSchema schema = BY_LOG_TYPE.get(logType);
        if (schema == null) {
            throw new IllegalArgumentException("no feature schema registered for log type " + logType);
        }
        return schema;
    }

    public static FeatureSchema byId(String schemaId) {
        FeatureSchema schema = BY_ID.get(schemaId);
        if (schema == null) {
            throw new IllegalArgumentException("no feature schema registered with id " + schemaId);
        }
        return schema;
    }
}
