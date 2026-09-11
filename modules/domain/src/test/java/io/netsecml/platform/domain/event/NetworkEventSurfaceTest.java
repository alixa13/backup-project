package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// The sealed hierarchy exists so a log type that is not a connection can be
// represented. That only holds if the shared interface demands nothing
// connection-shaped, so this pins its surface.
//
// The 09-04 spec asked for a synthetic second implementation in test sources to
// prove this instead. That cannot compile: a permits clause in main sources
// cannot name a test-source type, because Maven compiles the two source sets
// separately. This is the provable half of that intent -- weaker, because it
// constrains the interface rather than exercising a second shape, and saying so
// plainly matters more than implying the stronger proof is in place.
class NetworkEventSurfaceTest {

    // Exactly the envelope-derived accessors and nothing else. If someone adds a
    // measurements() or connection() method to the interface to save a pattern
    // switch at one call site, every future protocol record inherits an obligation
    // it cannot meaningfully satisfy -- and this fails.
    @Test
    void theInterfaceDeclaresOnlyEnvelopeDerivedAccessors() {
        Set<String> declared = Arrays.stream(NetworkEvent.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(java.util.stream.Collectors.toSet());

        assertEquals(Set.of("envelope", "eventId", "eventTime", "sensor", "logType", "connectionUid"),
            declared, "NetworkEvent must expose only the shared envelope block");
    }

    // permits is the guest list, and it must name only log types with a parser,
    // mapper and feature schema behind them. A record added ahead of its
    // implementation lets code compile against a protocol that does not exist.
    @Test
    void permitsListsOnlyImplementedLogTypes() {
        List<String> permitted = Arrays.stream(NetworkEvent.class.getPermittedSubclasses())
            .map(Class::getSimpleName)
            .toList();

        assertEquals(List.of("ConnEvent"), permitted,
            "only conn has a parser, mapper and schema today; add a record when its protocol lands");
    }

    // Sealing is what makes the compiler flag an unhandled case later. An
    // accidentally non-sealed interface would compile identically today and fail
    // silently the day a second protocol arrives.
    @Test
    void theHierarchyIsActuallySealed() {
        assertTrue(NetworkEvent.class.isSealed(), "NetworkEvent must stay sealed");
    }
}
