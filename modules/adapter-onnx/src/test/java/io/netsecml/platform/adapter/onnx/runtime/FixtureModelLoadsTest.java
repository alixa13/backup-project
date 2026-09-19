package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
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

            // Names alone don't prove the width: a fixture regenerated with the
            // wrong feature count would still pass the assertions above. Read the
            // "features" input's actual tensor shape and check its last
            // dimension is 20 — the shape the scorer's width check will assume.
            NodeInfo featuresInfo = session.getInputInfo().get("features");
            TensorInfo featuresTensorInfo = (TensorInfo) featuresInfo.getInfo();
            long[] shape = featuresTensorInfo.getShape();
            assertEquals(20L, shape[shape.length - 1],
                "the fixture's input tensor must carry exactly 20 features, per conn-feature-schema-v1");
        }
    }
}
