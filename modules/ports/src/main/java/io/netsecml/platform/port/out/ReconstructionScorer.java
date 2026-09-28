package io.netsecml.platform.port.out;

import io.netsecml.platform.domain.model.S7commDetectorBundle;

// Reconstructs one window of preprocessed vectors with a loaded S7comm
// detector and returns the reconstruction of its LAST row -- the only row the
// detector's score reads (causal_shadow.py). AutoCloseable because the real
// one holds an ONNX Runtime session; close() declares no checked exception, so
// an implementation rethrows its own close failure unchecked.
public interface ReconstructionScorer extends AutoCloseable {

    // The bundle this scorer was built from.
    S7commDetectorBundle bundle();

    // window: bundle().sequenceLength() rows of bundle().featureCount()
    // preprocessed values, oldest first.
    float[] reconstructLast(float[][] window);

    @Override
    void close();
}
