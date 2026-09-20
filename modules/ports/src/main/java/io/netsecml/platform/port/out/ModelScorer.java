package io.netsecml.platform.port.out;

import io.netsecml.platform.domain.model.ModelRef;

// Scores one feature vector's raw values against a single loaded model bundle.
// AutoCloseable because a real implementation (adapter-onnx) holds a native
// OrtSession that must be released; close() drops the checked
// AutoCloseable.close() throws Exception down to no throws at all, so a use
// case calling it never has to handle a checked exception. That narrowing
// pushes an obligation onto whatever implements this interface: OrtSession's
// own close() declares a checked exception (ai.onnxruntime.OrtException), so
// an implementation must catch its own checked close failure and rethrow it
// unchecked rather than swallow it -- narrowing the signature must not become
// a way to silently drop a real close failure.
//
// Deliberately narrow -- score(float[]) and ref() are the entire surface a use
// case needs, which is what lets ScoreFeaturesUseCase be tested with a stub and
// no ONNX Runtime on the classpath at all.
public interface ModelScorer extends AutoCloseable {

    // featureValues must already be in the exact ORDER of the schema the
    // bundle behind ref() was trained against. The caller (ScoreFeaturesUseCase)
    // guarantees that only indirectly: it binds schema identity by checking
    // ref().schemaId()/schemaHash() against the FeatureVector's own, which is
    // a proxy for order (the schema fixes the order once frozen), not a
    // direct check of the array's contents. WIDTH is a separate obligation and
    // is NOT checked by the caller at all -- neither ScoreFeaturesUseCase nor
    // FeatureVector's constructor compares values.length against any schema's
    // featureCount(). Only an implementation knows the width its own model
    // graph expects, so rejecting a wrong-width array is this method's job,
    // not the caller's.
    double score(float[] featureValues);

    // The bundle this scorer is bound to: identity, schema binding, decision
    // threshold and output-tensor layout. Called once per score() by
    // ScoreFeaturesUseCase to bind schema identity and threshold onto the
    // Prediction it produces.
    ModelRef ref();

    @Override
    void close();
}
