package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// Inference and training both need "what is schema X, and how many values does it
// have?" without touching Kafka or Flink. This is that lookup, over the static
// schema constants.
class FeatureSchemaRegistryTest {

    // The wiring-time question: given a log type, which schema does it produce?
    @Test
    void resolvesTheSchemaForAnImplementedLogType() {
        FeatureSchema schema = FeatureSchemaRegistry.byLogType(LogType.CONN);

        assertEquals("conn-feature-v1", schema.id());
        assertEquals(20, schema.featureCount());
    }

    // The consumer-side question: an archived row carries a schema id, and training
    // resolves the definitions from it. That is what makes a row self-describing.
    @Test
    void resolvesTheSchemaFromAnArchivedRowsId() {
        FeatureSchema schema = FeatureSchemaRegistry.byId("conn-feature-v1");

        assertSame(ConnFeatureSchemaV1.SCHEMA, schema,
            "byId must return the same frozen instance, not a copy");
    }

    // Both lookups agree, so a row's id resolves to the schema its log type
    // produces. A registry where these could disagree would let a vector be
    // validated against the wrong definitions.
    @Test
    void bothLookupsResolveToTheSameSchema() {
        assertSame(FeatureSchemaRegistry.byLogType(LogType.CONN),
            FeatureSchemaRegistry.byId("conn-feature-v1"));
    }

    // An unresolvable schema is a deployment error, not a runtime condition -- a
    // null return would let a misconfigured job start and fail later, per input,
    // instead of failing at startup.
    @Test
    void throwsOnAnUnknownSchemaIdRatherThanReturningNull() {
        assertThrows(IllegalArgumentException.class,
            () -> FeatureSchemaRegistry.byId("dns-feature-v1"));
    }

    // Every LogType constant must resolve. This is the fail-fast startup check:
    // adding a constant without registering its schema is caught here rather than
    // when the first record of that type arrives in production.
    @Test
    void everyLogTypeConstantHasARegisteredSchema() {
        for (LogType logType : LogType.values()) {
            assertNotNull(FeatureSchemaRegistry.byLogType(logType),
                logType + " has no registered schema");
        }
    }

    // A null key is a caller bug, and it should report as one. Without these
    // checks the map throws a NullPointerException whose message names
    // Object.hashCode -- useless to whoever has to diagnose it, and inconsistent
    // with every other failure in this class.
    @Test
    void rejectsNullArgumentsWithTheSameExceptionAsUnknownKeys() {
        assertThrows(IllegalArgumentException.class, () -> FeatureSchemaRegistry.byLogType(null));
        assertThrows(IllegalArgumentException.class, () -> FeatureSchemaRegistry.byId(null));
    }
}
