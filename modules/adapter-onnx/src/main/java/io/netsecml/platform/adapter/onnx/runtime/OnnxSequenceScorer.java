package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;

import java.util.Map;

// Runs a dual-head sequence detector (spec section 3): one window in, the
// dense head's reconstruction MAE and the temporal head's endpoint MAE out --
// upstream's score_dense and score_temporal, computed in double. One session
// per subtask, one thread each way (CLAUDE.md's CPU rule).
public final class OnnxSequenceScorer implements SequenceScorer {

    private static final OrtEnvironment ENVIRONMENT = OrtEnvironment.getEnvironment();

    private final SequenceDetectorBundle bundle;
    private final OrtSession session;
    private boolean closed = false;

    public OnnxSequenceScorer(byte[] model, SequenceDetectorBundle bundle) {
        this.bundle = bundle;
        try {
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            options.setIntraOpNumThreads(1);
            options.setInterOpNumThreads(1);
            this.session = ENVIRONMENT.createSession(model, options);
        } catch (OrtException e) {
            throw new IllegalStateException("failed to open the ONNX graph of " + bundle.bundleId(), e);
        }
        // The graph must have exactly the names and shapes the bundle declares.
        try {
            int length = bundle.sequenceLength();
            int width = bundle.featureCount();
            requireShape("input", bundle.inputName(), session.getInputInfo().get(bundle.inputName()), length, width);
            requireShape("dense output", bundle.denseOutputName(),
                session.getOutputInfo().get(bundle.denseOutputName()), length, width);
            requireShape("temporal output", bundle.temporalOutputName(),
                session.getOutputInfo().get(bundle.temporalOutputName()), length - 1, width);
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
    private static void requireShape(String role, String name, NodeInfo info, int rows, int width) {
        if (info == null || !(info.getInfo() instanceof TensorInfo tensor)) {
            throw new IllegalStateException("the graph has no " + role + " tensor named '" + name + "'");
        }
        long[] shape = tensor.getShape();
        if (shape.length != 3 || shape[1] != rows || shape[2] != width) {
            throw new IllegalStateException("the graph's " + role + " '" + name + "' has shape "
                + java.util.Arrays.toString(shape) + ", not [?, " + rows + ", " + width + "]");
        }
    }

    @Override
    public SequenceDetectorBundle bundle() {
        return bundle;
    }

    @Override
    public DetectorScores score(float[][] sequence) {
        int length = bundle.sequenceLength();
        int width = bundle.featureCount();
        // Exactly one window of the bundle's shape.
        if (sequence.length != length) {
            throw new IllegalArgumentException("expected " + length + " rows, got " + sequence.length);
        }
        for (float[] row : sequence) {
            if (row.length != width) {
                throw new IllegalArgumentException("expected rows of " + width + " values, got " + row.length);
            }
        }
        try (OnnxTensor input = OnnxTensor.createTensor(ENVIRONMENT, new float[][][]{sequence});
             OrtSession.Result result = session.run(Map.of(bundle.inputName(), input))) {
            float[][] dense = output(result, bundle.denseOutputName())[0];
            float[][] temporal = output(result, bundle.temporalOutputName())[0];
            // score_dense: mean |input - reconstruction| over the whole window.
            double denseSum = 0;
            for (int i = 0; i < length; i++) {
                for (int j = 0; j < width; j++) {
                    denseSum += Math.abs(sequence[i][j] - dense[i][j]);
                }
            }
            // score_temporal: mean |prediction of the last event - the last event|.
            double temporalSum = 0;
            for (int j = 0; j < width; j++) {
                temporalSum += Math.abs(temporal[length - 2][j] - sequence[length - 1][j]);
            }
            return new DetectorScores(denseSum / (length * width), temporalSum / width);
        } catch (OrtException e) {
            throw new IllegalStateException("ONNX Runtime failed scoring with " + bundle.bundleId(), e);
        }
    }

    private static float[][][] output(OrtSession.Result result, String name) throws OrtException {
        OnnxValue value = result.get(name).orElseThrow(
            () -> new IllegalStateException("the graph returned no output named '" + name + "'"));
        return (float[][][]) value.getValue();
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
