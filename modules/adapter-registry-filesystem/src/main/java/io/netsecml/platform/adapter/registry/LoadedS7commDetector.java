package io.netsecml.platform.adapter.registry;

import io.netsecml.platform.domain.model.S7commDetectorBundle;

import java.util.Arrays;

// A verified S7comm bundle and its model bytes, for the ONNX scorer to open.
public record LoadedS7commDetector(S7commDetectorBundle bundle, byte[] model) {

    public LoadedS7commDetector {
        model = Arrays.copyOf(model, model.length);
    }

    @Override
    public byte[] model() {
        return Arrays.copyOf(model, model.length);
    }
}
