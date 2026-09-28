package io.netsecml.platform.port.out;

import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;

// Scores one window of preprocessed vectors against a loaded sequence
// detector. AutoCloseable because the real one holds an ONNX Runtime session;
// close() declares no checked exception, so an implementation rethrows its
// own close failure unchecked rather than dropping it.
public interface SequenceScorer extends AutoCloseable {

    // The bundle this scorer was built from: preprocessing, thresholds, identity.
    SequenceDetectorBundle bundle();

    // sequence is bundle().sequenceLength() rows of bundle().featureCount()
    // preprocessed values, oldest first.
    DetectorScores score(float[][] sequence);

    @Override
    void close();
}
