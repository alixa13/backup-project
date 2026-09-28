package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.port.out.ReconstructionScorer;

import java.util.Arrays;
import java.util.Map;

// Runs the S7comm Stage 1 LSTM autoencoder (scoring design section 3.3): one
// window in, the reconstruction of its last row out -- the only row the score
// reads. One session per subtask, one thread each way (CLAUDE.md's CPU rule).
public final class OnnxReconstructionScorer implements ReconstructionScorer {

    private static final OrtEnvironment ENVIRONMENT = OrtEnvironment.getEnvironment();

    private final S7commDetectorBundle bundle;
    private final OrtSession session;
    private boolean closed = false;

    public OnnxReconstructionScorer(byte[] model, S7commDetectorBundle bundle) {
        this.bundle = bundle;
        try {
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            options.setIntraOpNumThreads(1);
            options.setInterOpNumThreads(1);
            this.session = ENVIRONMENT.createSession(model, options);
        } catch (OrtException e) {
            throw new IllegalStateException("failed to open the ONNX graph of " + bundle.bundleId(), e);
        }
        // The graph must have the names and shapes the bundle declares. The
        // delivered graph leaves its output's row count symbolic (-1).
        try {
            requireShape("input", bundle.inputName(), session.getInputInfo().get(bundle.inputName()), false);
            requireShape("output", bundle.outputName(), session.getOutputInfo().get(bundle.outputName()), true);
        } catch (OrtException | RuntimeException e) {
            IllegalStateException failure = e instanceof IllegalStateException ise ? ise
                : new IllegalStateException("failed to read the graph of " + bundle.bundleId(), e);
            try {
                session.close();
            } catch (OrtException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    // A [batch, rows, width] float tensor with this exact name.
    private void requireShape(String role, String name, NodeInfo info, boolean symbolicRowsAllowed) {
        if (info == null || !(info.getInfo() instanceof TensorInfo tensor)) {
            throw new IllegalStateException("the graph has no " + role + " tensor named '" + name + "'");
        }
        long[] shape = tensor.getShape();
        int rows = bundle.sequenceLength();
        boolean rowsFit = shape.length == 3 && (shape[1] == rows || (symbolicRowsAllowed && shape[1] == -1));
        if (!rowsFit || shape[2] != bundle.featureCount()) {
            throw new IllegalStateException("the graph's " + role + " '" + name + "' has shape "
                + Arrays.toString(shape) + ", not [?, " + rows + ", " + bundle.featureCount() + "]");
        }
    }

    @Override
    public S7commDetectorBundle bundle() {
        return bundle;
    }

    @Override
    public float[] reconstructLast(float[][] window) {
        int length = bundle.sequenceLength();
        int width = bundle.featureCount();
        // Exactly one window of the bundle's shape.
        if (window.length != length) {
            throw new IllegalArgumentException("expected " + length + " rows, got " + window.length);
        }
        for (float[] row : window) {
            if (row.length != width) {
                throw new IllegalArgumentException("expected rows of " + width + " values, got " + row.length);
            }
        }
        try (OnnxTensor input = OnnxTensor.createTensor(ENVIRONMENT, new float[][][]{window});
             OrtSession.Result result = session.run(Map.of(bundle.inputName(), input))) {
            OnnxValue value = result.get(bundle.outputName()).orElseThrow(
                () -> new IllegalStateException("the graph returned no output named '" + bundle.outputName() + "'"));
            float[][][] reconstruction = (float[][][]) value.getValue();
            if (reconstruction.length != 1 || reconstruction[0].length != length
                    || reconstruction[0][length - 1].length != width) {
                throw new IllegalStateException("the graph returned a reconstruction that is not [1, " + length
                    + ", " + width + "]");
            }
            return reconstruction[0][length - 1].clone();
        } catch (OrtException e) {
            throw new IllegalStateException("ONNX Runtime failed reconstructing with " + bundle.bundleId(), e);
        }
    }

    // Idempotent; a close failure is rethrown unchecked, never dropped.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            session.close();
        } catch (OrtException e) {
            throw new IllegalStateException("failed to close the ONNX session of " + bundle.bundleId(), e);
        }
    }
}
