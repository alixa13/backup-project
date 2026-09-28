package io.netsecml.platform.adapter.registry;

import io.netsecml.platform.domain.model.SequenceDetectorBundle;

import java.util.Arrays;

// A verified bundle and its model bytes, for the ONNX scorer to open.
public record LoadedSequenceDetector(SequenceDetectorBundle bundle, byte[] model) {

    public LoadedSequenceDetector {
        model = Arrays.copyOf(model, model.length);
    }

    @Override
    public byte[] model() {
        return Arrays.copyOf(model, model.length);
    }
}
