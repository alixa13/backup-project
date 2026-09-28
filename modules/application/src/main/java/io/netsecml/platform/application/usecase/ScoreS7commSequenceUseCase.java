package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.port.out.ReconstructionScorer;

import java.time.Clock;

// Scores one S7comm feature vector
// (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md sections
// 3-5): keeps its connection's window of preprocessed vectors, reconstructs the
// window once full, turns the last event's weighted error into a
// group-conditional conformal p-value, and that into a verdict. The window is
// updated in place and handed back, like ModbusScoreWindow.
public final class ScoreS7commSequenceUseCase {

    // The first events after any reset that are never scored (spec amendment A1, item 5):
    // v2 was gated on events past the 64th, and events 16-64 after a reset carry ~12%
    // false alarms.
    public static final int WARMUP_EVENTS = 64;

    private final ReconstructionScorer scorer;
    private final S7commDetectorBundle bundle;
    private final Clock clock;
    private final float[] weights;
    private final float weightSum;
    private final int warmupEvents;

    public ScoreS7commSequenceUseCase(ReconstructionScorer scorer, Clock clock) {
        this(scorer, clock, WARMUP_EVENTS);
    }

    // warmupEvents: how many events after a reset are WARMUP. At least the window's
    // own warm-up (sequenceLength - 1): the first score needs a full window.
    public ScoreS7commSequenceUseCase(ReconstructionScorer scorer, Clock clock, int warmupEvents) {
        this.scorer = scorer;
        this.bundle = scorer.bundle();
        this.clock = clock;
        // The detector must be the one trained on the vectors this job builds.
        if (!S7commFeatureSchemaV1.SCHEMA.id().equals(bundle.schemaId())) {
            throw new IllegalStateException("bundle " + bundle.bundleId() + " was trained on " + bundle.schemaId()
                + ", not " + S7commFeatureSchemaV1.SCHEMA.id());
        }
        // The score's weights, once: they are frozen with the bundle.
        this.weights = bundle.scoreWeights();
        float sum = 0f;
        for (float w : weights) {
            sum += w;
        }
        this.weightSum = sum;
        if (warmupEvents < bundle.sequenceLength() - 1) {
            throw new IllegalArgumentException("warmupEvents " + warmupEvents
                + " is shorter than the window's own warm-up, " + (bundle.sequenceLength() - 1));
        }
        this.warmupEvents = warmupEvents;
    }

    public S7commScoringResult score(FeatureVector vector, boolean freshState, String clientIp, String serverIp,
                                     S7commScoreWindow window) {
        // A window filled under another bundle, or none yet: start empty.
        if (window == null || !window.bundleId().equals(bundle.bundleId())) {
            window = S7commScoreWindow.empty(bundle.bundleId(), bundle.sequenceLength(), bundle.featureCount());
        }
        // The feature engine started this connection from empty state: so does
        // the window (spec section 5, D4).
        if (freshState) {
            window.reset();
        }
        long eventsSinceReset = window.countEvent();
        float[] raw = vector.values();
        S7commScoreGroup group = S7commScoreGroup.of(raw);
        double alpha = bundle.policy().alpha(group);
        // Always finite (the preprocessing imputes and clips), so every vector
        // enters the window (plan ruling P2).
        float[] preprocessed = bundle.preprocessing().apply(raw);
        window.append(preprocessed, vector.qualityFlags());
        // Fewer than a full window, or still inside the warm-up: WARMUP (spec amendment A1, item 5).
        if (!window.isFull() || eventsSinceReset <= warmupEvents) {
            return new S7commScoringResult(prediction(vector, clientIp, serverIp, DetectorVerdict.WARMUP, null,
                null, group, alpha, eventsSinceReset, window.flagsOr(), 0L), window);
        }
        // A full window: reconstruct it and judge the last event.
        long started = System.nanoTime();
        float[] reconstruction = scorer.reconstructLast(window.sequence());
        long micros = (System.nanoTime() - started) / 1_000;
        float score = lastEventScore(reconstruction, preprocessed);
        // A non-finite score cannot be judged (plan ruling P2).
        if (!Float.isFinite(score)) {
            return new S7commScoringResult(prediction(vector, clientIp, serverIp, DetectorVerdict.UNSCORABLE,
                null, null, group, alpha, eventsSinceReset, window.flagsOr(), micros), window);
        }
        double p = bundle.policy().pValue(group, score);
        DetectorVerdict verdict = bundle.policy().anomalous(group, score) ? DetectorVerdict.ANOMALY
            : DetectorVerdict.NORMAL;
        return new S7commScoringResult(prediction(vector, clientIp, serverIp, verdict, score, p, group, alpha,
            eventsSinceReset, window.flagsOr(), micros), window);
    }

    // upstream's causal score (causal_shadow.py) in its own float32
    // arithmetic: sum_j w_j * (reconstruction_j - x_j)^2 / sum_j w_j.
    private float lastEventScore(float[] reconstruction, float[] actual) {
        if (reconstruction.length != actual.length) {
            throw new IllegalStateException("the detector returned " + reconstruction.length + " values for a row of "
                + actual.length);
        }
        float sum = 0f;
        for (int j = 0; j < actual.length; j++) {
            float d = reconstruction[j] - actual[j];
            sum += weights[j] * (d * d);
        }
        return sum / weightSum;
    }

    // The prediction's fields: the vector's identity, the connection's
    // endpoints, the bundle's identity, and this event's outcome.
    private S7commDetectorPrediction prediction(FeatureVector vector, String clientIp, String serverIp,
                                                DetectorVerdict verdict, Float score, Double p,
                                                S7commScoreGroup group, double alpha, long eventsSinceReset,
                                                int flags, long micros) {
        return new S7commDetectorPrediction(
            Prediction.deriveId(vector.eventId(), bundle.name(), bundle.version()),
            vector.eventId(), vector.eventTime(), vector.sensor(), vector.connectionUid(), clientIp, serverIp,
            bundle.name(), bundle.version(), bundle.modelSha(), bundle.schemaId(), bundle.schemaHash(),
            verdict, score, p, group, alpha, eventsSinceReset, flags, micros, clock.instant());
    }
}
