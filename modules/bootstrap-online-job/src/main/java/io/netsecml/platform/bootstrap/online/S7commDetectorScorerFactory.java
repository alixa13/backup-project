package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.onnx.runtime.OnnxReconstructionScorer;
import io.netsecml.platform.adapter.registry.LoadedS7commDetector;
import io.netsecml.platform.adapter.registry.S7commDetectorBundleLoader;
import io.netsecml.platform.port.out.ReconstructionScorer;
import io.netsecml.platform.port.out.ReconstructionScorerFactory;

import java.io.IOException;
import java.nio.file.Path;

// The composition root's ReconstructionScorerFactory: loads and verifies the
// S7comm bundle (adapter-registry-filesystem), then opens its graph
// (adapter-onnx) -- two adapters, so it lives here. Serializable as a path
// string; runs in each subtask's open() on the TaskManager, where models/ is mounted.
public final class S7commDetectorScorerFactory implements ReconstructionScorerFactory {

    private final String bundleDir;

    public S7commDetectorScorerFactory(String bundleDir) {
        this.bundleDir = bundleDir;
    }

    @Override
    public ReconstructionScorer create() {
        try {
            LoadedS7commDetector loaded = S7commDetectorBundleLoader.load(Path.of(bundleDir));
            return new OnnxReconstructionScorer(loaded.model(), loaded.bundle());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the S7comm detector bundle at " + bundleDir, e);
        }
    }
}
