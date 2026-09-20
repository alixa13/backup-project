package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxJavaType;
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
    // Not volatile: idempotence here only covers one thread calling close()
    // twice (try-with-resources followed by an explicit call, say), which a
    // plain field already makes safe. ModelScorerFactory's own contract is
    // one scorer per subtask, never shared across threads, and volatile would
    // not make the check-then-act below atomic anyway -- two racing callers
    // could still both read closed==false and both reach closeSession(). It
    // would only advertise a concurrency guarantee this class does not have.
    private boolean closed = false;

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

            // Output-side pairing checks, mirroring the input-side checks above.
            // A mismatch between ref and the graph it names is invariant across
            // every record -- it is decidable right now, from this session's own
            // metadata -- so it must fail here rather than on the first call to
            // score(). Once a Flink operator is calling this scorer, a per-record
            // exception there is caught, counted, and the record dropped, which
            // turns a bad bundle into a silently empty prediction topic instead
            // of a failed, diagnosable job.
            NodeInfo outputInfo = session.getOutputInfo().get(ref.outputName());
            if (outputInfo == null) {
                throw new IllegalStateException("graph has no output named '" + ref.outputName()
                        + "'; graph outputs are " + session.getOutputNames());
            }
            TensorInfo outputTensorInfo = requireFloatTensorOutput(ref.outputName(), outputInfo);
            long[] outputShape = outputTensorInfo.getShape();
            long outputColumns = outputShape[outputShape.length - 1];
            if (ref.positiveClassColumn() >= outputColumns) {
                throw new IllegalStateException("positiveClassColumn " + ref.positiveClassColumn()
                        + " is out of bounds for output '" + ref.outputName() + "', which has " + outputColumns
                        + " column(s)");
            }
        } catch (IllegalStateException e) {
            // One of the checks above (or the input-count check preceding them)
            // rejected this ref/graph pairing after the session was already
            // created and threw this exact type with its own diagnostic
            // message: close the session before propagating, or this
            // constructor leaks a native OrtSession on every rejected pairing.
            // closeSession() itself can throw IllegalStateException on a native
            // close failure -- if that propagated normally here it would
            // replace the diagnostic an operator actually needs (which width,
            // which output, which column) with a generic close failure, so it
            // is attached as suppressed onto the original exception instead,
            // which is rethrown unchanged.
            closeSessionSuppressingInto(e);
            throw e;
        } catch (RuntimeException e) {
            // Not one of the checks above -- e.g. a graph whose input/output
            // metadata this runtime cannot describe as a NodeInfo, surfacing as
            // a bare NullPointerException from the next line down. Wrap it with
            // the same "which model" context the checks above give, rather than
            // let something this unexpected reach the caller with no diagnostic
            // at all; the session is still closed first for the same leak
            // reason as above.
            IllegalStateException wrapped =
                    new IllegalStateException("unexpected failure validating graph metadata for model "
                            + ref.name(), e);
            closeSessionSuppressingInto(wrapped);
            throw wrapped;
        } catch (OrtException e) {
            IllegalStateException wrapped =
                    new IllegalStateException("failed to read graph input/output info for model " + ref.name(), e);
            closeSessionSuppressingInto(wrapped);
            throw wrapped;
        }
    }

    // Package-visible so the element-type guard can be exercised directly in a
    // test without needing a second .onnx fixture whose graph actually produces
    // a non-tensor or non-float output. TensorInfo.getInfo() returns TensorInfo
    // for a tensor output but something else entirely -- a SequenceInfo, for a
    // scikit-learn zipmap output, which is a sequence of maps -- so the cast is
    // guarded with instanceof rather than left to throw ClassCastException on
    // the first scored record.
    static TensorInfo requireFloatTensorOutput(String outputName, NodeInfo outputInfo) {
        if (!(outputInfo.getInfo() instanceof TensorInfo tensorInfo)) {
            throw new IllegalStateException("graph output '" + outputName + "' is not a tensor (found "
                    + outputInfo.getInfo().getClass().getSimpleName() + "); a scikit-learn export with zipmap "
                    + "enabled produces a sequence of maps instead of a tensor, which this scorer cannot read");
        }
        if (tensorInfo.type != OnnxJavaType.FLOAT) {
            throw new IllegalStateException("graph output '" + outputName + "' has element type " + tensorInfo.type
                    + ", not float");
        }
        return tensorInfo;
    }

    @Override
    public double score(float[] featureValues) {
        if (featureValues.length != expectedFeatureCount) {
            throw new IllegalArgumentException("expected a feature vector of width " + expectedFeatureCount
                    + " but got " + featureValues.length);
        }
        try (OnnxTensor input = OnnxTensor.createTensor(ENVIRONMENT, new float[][] {featureValues});
             OrtSession.Result result = session.run(Collections.singletonMap(inputName, input))) {
            // ref.outputName() naming a real, float-typed tensor output, and
            // ref.positiveClassColumn() being within that tensor's column count,
            // were both verified once at construction (see the constructor's
            // output-side checks) -- a ref/graph mismatch is invariant across
            // every record, not a per-record condition, so nothing here
            // re-derives it. The output tensor's shape is [rows, columns]: one
            // row per scored record (always one here) and some number of
            // columns that has no required relationship to ref.classes().size().
            // positiveClassColumn selects a column of THIS tensor, not an index
            // into ref.classes() -- this fixture's output has a single column
            // even though classes has two entries, which is the normal shape
            // for a P(positive)-only probability output.
            float[][] rows = (float[][]) result.get(ref.outputName()).get().getValue();
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

    // Used only while the constructor is unwinding after rejecting a ref/graph
    // pairing. closeSession() converts a native close failure into its own
    // IllegalStateException; if that were allowed to propagate here it would
    // replace the original exception -- the diagnostic an operator actually
    // needs (which width, which output, which column was wrong) -- with a
    // generic close failure that says nothing about why the constructor was
    // closing the session in the first place. Attach the close failure as
    // suppressed onto the original instead, and let the original win.
    private void closeSessionSuppressingInto(RuntimeException original) {
        try {
            closeSession();
        } catch (RuntimeException closeFailure) {
            original.addSuppressed(closeFailure);
        }
    }
}
