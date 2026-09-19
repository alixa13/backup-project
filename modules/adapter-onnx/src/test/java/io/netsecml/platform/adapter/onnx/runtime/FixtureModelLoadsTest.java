package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Proves two things at once: ONNX Runtime resolves and runs offline, and the
// committed fixture is a valid graph with the shape the scorer will assume.
// Everything later in this unit depends on both.
class FixtureModelLoadsTest {

    private static final Path BUNDLE = Path.of("..", "..", "tests", "fixtures", "models", "conn-demo-v1");

    @Test
    void theFixtureModelExposesTwentyInputsAndOneNamedOutput() throws Exception {
        byte[] model = Files.readAllBytes(BUNDLE.resolve("model.onnx"));
        try (OrtEnvironment env = OrtEnvironment.getEnvironment();
             OrtSession session = env.createSession(model, new OrtSession.SessionOptions())) {
            assertEquals(Set.of("features"), session.getInputNames(),
                "the scorer feeds a tensor named 'features'");
            assertTrue(session.getOutputNames().contains("probability"),
                "bundle.json names 'probability' as the output; the graph must actually have it");
        }
    }
}
