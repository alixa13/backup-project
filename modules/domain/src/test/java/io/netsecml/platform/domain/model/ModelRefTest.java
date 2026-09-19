package io.netsecml.platform.domain.model;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// ModelRef travels through Flink job configuration, so its compact constructor
// is the only gate against a malformed model reference reaching the scoring path.
class ModelRefTest {

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
}
