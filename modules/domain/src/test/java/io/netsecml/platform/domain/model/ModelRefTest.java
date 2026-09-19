package io.netsecml.platform.domain.model;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// ModelRef travels through Flink job configuration, so its compact constructor
// is the only gate against a malformed model reference reaching the scoring path.
class ModelRefTest {

    // Shared valid values so each test below changes exactly one field.
    private static final String SCHEMA_HASH = "a".repeat(64);
    private static final String MODEL_SHA = "b".repeat(64);
    private static final List<String> CLASSES = List.of("normal", "attack");

    @Test
    void rejectsAHashThatIsNotSixtyFourHexCharacters() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "conn-feature-v1", "not-a-hash", "a".repeat(64),
            0.5f, List.of("normal", "attack"), "probability", 0));
    }

    @Test
    void rejectsAThresholdOutsideZeroToOne() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "conn-feature-v1", "a".repeat(64), "b".repeat(64),
            1.5f, List.of("normal", "attack"), "probability", 0));
    }

    @Test
    void classesAreCopiedSoACallerCannotMutateTheRef() {
        List<String> mutable = new ArrayList<>(List.of("normal", "attack"));
        ModelRef ref = new ModelRef("conn-demo", "v1", "conn-feature-v1", "a".repeat(64),
            "b".repeat(64), 0.5f, mutable, "probability", 0);
        mutable.clear();
        assertEquals(2, ref.classes().size(), "the ref must not share the caller's list");
    }

    // name identifies the bundle in the registry; blank means it can never be looked up.
    @Test
    void rejectsABlankName() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "  ", "v1", "conn-feature-v1", SCHEMA_HASH, MODEL_SHA, 0.5f, CLASSES, "probability", 0));
    }

    // version disambiguates two bundles of the same name; blank collapses that distinction.
    @Test
    void rejectsABlankVersion() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "  ", "conn-feature-v1", SCHEMA_HASH, MODEL_SHA, 0.5f, CLASSES, "probability", 0));
    }

    // schemaId is what ties this model to the feature schema it was trained against;
    // blank leaves the pinning nothing to point at.
    @Test
    void rejectsABlankSchemaId() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "  ", SCHEMA_HASH, MODEL_SHA, 0.5f, CLASSES, "probability", 0));
    }

    // outputName is the ONNX graph output the adapter must read; blank means
    // inference has no tensor name to ask the runtime for.
    @Test
    void rejectsABlankOutputName() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "conn-feature-v1", SCHEMA_HASH, MODEL_SHA, 0.5f, CLASSES, "  ", 0));
    }

    // modelSha is the model file's own content hash, distinct from schemaHash;
    // it gets the same 64-lowercase-hex format check.
    @Test
    void rejectsAModelShaThatIsNotSixtyFourHexCharacters() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "conn-feature-v1", SCHEMA_HASH, "not-a-hash", 0.5f, CLASSES, "probability", 0));
    }

    // An empty classes list leaves positiveClassColumn nothing to label.
    @Test
    void rejectsAnEmptyClassesList() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "conn-feature-v1", SCHEMA_HASH, MODEL_SHA, 0.5f, List.of(), "probability", 0));
    }

    // A negative column cannot address any position in the output tensor.
    @Test
    void rejectsANegativePositiveClassColumn() {
        assertThrows(IllegalArgumentException.class, () -> new ModelRef(
            "conn-demo", "v1", "conn-feature-v1", SCHEMA_HASH, MODEL_SHA, 0.5f, CLASSES, "probability", -1));
    }

    // positiveClassColumn indexes the model's OUTPUT TENSOR, not the classes list --
    // a two-class label list with a single-column probability output legitimately
    // uses column 0, and nothing here should reject a column past classes.size().
    @Test
    void acceptsAPositiveClassColumnBeyondClassesSize() {
        ModelRef ref = new ModelRef("conn-demo", "v1", "conn-feature-v1", SCHEMA_HASH, MODEL_SHA,
            0.5f, List.of("normal"), "probability", 5);
        assertEquals(5, ref.positiveClassColumn());
    }
}
