package io.netsecml.platform.adapter.registry;

import io.netsecml.platform.domain.model.ModelRef;
import java.util.List;
import java.util.Objects;

// A model bundle as loaded off disk by FilesystemModelRegistry: the domain
// identity (ModelRef), the raw ONNX model bytes -- already SHA-256-verified
// against ModelRef.modelSha before this is ever constructed -- and the golden
// sample vectors carried in bundle.json's sampleVectors (see SampleVector).
public record LoadedModel(ModelRef ref, byte[] onnx, List<SampleVector> samples) {

    public LoadedModel {
        // ref and onnx are structural: a bundle without an identity or a model
        // to run is not a bundle at all.
        Objects.requireNonNull(ref, "ref must not be null");
        Objects.requireNonNull(onnx, "onnx must not be null");
        // Defensive copy in: the caller keeps no handle on our internal array.
        onnx = onnx.clone();
        // List.copyOf both defends against a caller-held mutable list AND
        // rejects a null samples list or any null element within it -- a null
        // sample would otherwise NPE later, inside whatever test or use case
        // iterates the list to score it.
        samples = List.copyOf(samples);
    }

    // Defensive copy out: callers cannot mutate our internal array either.
    @Override
    public byte[] onnx() {
        return onnx.clone();
    }
}
