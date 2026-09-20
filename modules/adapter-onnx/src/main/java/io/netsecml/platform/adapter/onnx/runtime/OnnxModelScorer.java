package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import io.netsecml.platform.domain.model.ModelRef;
import io.netsecml.platform.port.out.ModelScorer;

import java.util.Collections;
import java.util.Set;

// Runs one pinned ONNX graph against ModelScorer's contract: score one feature
// vector, report the ref it is bound to, and release its native session when
// done. This is the only class in the platform that actually calls into ONNX
// Runtime -- every use case above it (ScoreFeaturesUseCase) depends only on
// the ModelScorer port and a stub in tests.
public final class OnnxModelScorer implements ModelScorer {

    // OrtEnvironment.getEnvironment() returns one JVM-wide singleton, not an
    // instance this class owns, so it is never closed here regardless of how
    // many OnnxModelScorer instances (and their per-instance OrtSession below)
    // come and go. Measured directly against onnxruntime 1.20.0's bytecode:
    // OrtEnvironment.close() is a no-op there (its body is a bare `return`),
    // so nothing in this JVM currently tears the singleton down even when
    // another caller puts it in a try-with-resources -- but that is this
    // version's behavior, not a documented contract, so this class does not
    // rely on it and still never calls close() on the shared instance itself.
    private static final OrtEnvironment ENVIRONMENT = OrtEnvironment.getEnvironment();

    private final ModelRef ref;
    private final int expectedFeatureCount;
    private final OrtSession session;
    private final String inputName;
    private volatile boolean closed = false;

    public OnnxModelScorer(byte[] onnxModel, ModelRef ref, int expectedFeatureCount) {
        this.ref = ref;
        this.expectedFeatureCount = expectedFeatureCount;
        try {
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            // CPU-only deployment: one thread per subtask, because Flink already runs one
            // subtask per core and a runtime that spawns its own pool oversubscribes them.
            options.setIntraOpNumThreads(1);
            options.setInterOpNumThreads(1);
            this.session = ENVIRONMENT.createSession(onnxModel, options);
        } catch (OrtException e) {
            throw new IllegalStateException("failed to create ONNX Runtime session for model " + ref.name(), e);
        }

        try {
            Set<String> inputNames = session.getInputNames();
            if (inputNames.size() != 1) {
                throw new IllegalStateException("expected exactly one graph input, found " + inputNames);
            }
            this.inputName = inputNames.iterator().next();
            NodeInfo inputInfo = session.getInputInfo().get(inputName);
            TensorInfo tensorInfo = (TensorInfo) inputInfo.getInfo();
            long[] shape = tensorInfo.getShape();
            long actualWidth = shape[shape.length - 1];
            if (actualWidth != expectedFeatureCount) {
                throw new IllegalStateException("graph input '" + inputName + "' has width " + actualWidth
                        + " but schema requires " + expectedFeatureCount);
            }
        } catch (RuntimeException e) {
            // The width check (or the input-count check above it) failed after the
            // session was already created: close it before propagating, or this
            // constructor leaks a native OrtSession on every rejected pairing --
            // exactly the path aGraphWhoseInputWidthDiffersFromTheSchemaIsRejected
            // AtConstruction exercises.
            closeSession();
            throw e;
        } catch (OrtException e) {
            closeSession();
            throw new IllegalStateException("failed to read graph input info for model " + ref.name(), e);
        }
    }

    @Override
    public double score(float[] featureValues) {
        if (featureValues.length != expectedFeatureCount) {
            throw new IllegalArgumentException("expected a feature vector of width " + expectedFeatureCount
                    + " but got " + featureValues.length);
        }
        try (OnnxTensor input = OnnxTensor.createTensor(ENVIRONMENT, new float[][] {featureValues});
             OrtSession.Result result = session.run(Collections.singletonMap(inputName, input))) {
            var outputValue = result.get(ref.outputName());
            if (outputValue.isEmpty()) {
                throw new IllegalStateException("graph has no output named '" + ref.outputName() + "'");
            }
            // The fixture (and every bundle this scorer is built for) is a
            // [1, columns] tensor: one row (the single scored record) and one
            // column per class. positiveClassColumn selects a column of THIS
            // tensor, not an index into ref.classes().
            float[][] rows = (float[][]) outputValue.get().getValue();
            return rows[0][ref.positiveClassColumn()];
        } catch (OrtException e) {
            throw new IllegalStateException("failed to run ONNX Runtime inference for model " + ref.name(), e);
        }
    }

    @Override
    public ModelRef ref() {
        return ref;
    }

    @Override
    public void close() {
        // ModelScorer.close() narrows AutoCloseable.close() to throw nothing at
        // all, but OrtSession.close() throws the checked OrtException -- a
        // scorer's close has nothing a caller could act on beyond knowing native
        // memory may not have been released, so a close failure is surfaced as
        // an unchecked exception rather than silently swallowed. Idempotent:
        // ModelScorer's javadoc gives no guarantee against a second close() (a
        // use case may call it via try-with-resources with no other guard), and
        // calling OrtSession.close() twice is itself undefined, so a local flag
        // makes a repeat call a no-op instead.
        if (closed) {
            return;
        }
        closed = true;
        closeSession();
    }

    private void closeSession() {
        try {
            session.close();
        } catch (OrtException e) {
            throw new IllegalStateException("failed to close ONNX Runtime session for model " + ref.name(), e);
        }
    }
}
