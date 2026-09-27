package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.onnx.runtime.OnnxSequenceScorer;
import io.netsecml.platform.adapter.registry.LoadedSequenceDetector;
import io.netsecml.platform.adapter.registry.SequenceDetectorBundleLoader;
import io.netsecml.platform.port.out.SequenceScorer;
import io.netsecml.platform.port.out.SequenceScorerFactory;

import java.io.IOException;
import java.nio.file.Path;

// The composition root's SequenceScorerFactory: loads and verifies the bundle
// (adapter-registry-filesystem), then opens its graph (adapter-onnx) -- two
// adapters, so it lives here. Serializable as a path string; runs in each
// subtask's open() on the TaskManager, where models/ is mounted.
public final class ModbusDetectorScorerFactory implements SequenceScorerFactory {

    private final String bundleDir;

    public ModbusDetectorScorerFactory(String bundleDir) {
        this.bundleDir = bundleDir;
    }

    @Override
    public SequenceScorer create() {
        try {
            LoadedSequenceDetector loaded = SequenceDetectorBundleLoader.load(Path.of(bundleDir));
            return new OnnxSequenceScorer(loaded.model(), loaded.bundle());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the Modbus detector bundle at " + bundleDir, e);
        }
    }
}
