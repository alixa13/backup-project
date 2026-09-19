package io.netsecml.platform.port.out;

import io.netsecml.platform.domain.model.ModelRef;

// Scores one feature vector's raw values against a single loaded model bundle.
// AutoCloseable because a real implementation (adapter-onnx) holds a native
// OrtSession that must be released; close() drops the checked
// AutoCloseable.close() throws Exception down to no throws at all, since a
// scorer's own close has nothing checked to report and a use case calling it
// should not have to handle one.
//
// Deliberately narrow -- score(float[]) and ref() are the entire surface a use
// case needs, which is what lets ScoreFeaturesUseCase be tested with a stub and
// no ONNX Runtime on the classpath at all.
public interface ModelScorer extends AutoCloseable {

    // featureValues must already be in the exact order and width of the schema
    // the bundle behind ref() was trained against. Checking that is the
    // caller's job (ScoreFeaturesUseCase), not this method's -- a ModelScorer
    // only holds the model, not the FeatureVector's own schema identity.
    double score(float[] featureValues);

    // The bundle this scorer is bound to: identity, schema binding, decision
    // threshold and output-tensor layout. Called once per score() by
    // ScoreFeaturesUseCase to bind schema identity and threshold onto the
    // Prediction it produces.
    ModelRef ref();

    @Override
    void close();
}
