package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;

import java.time.Clock;

// Scores one Modbus feature vector
// (docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md sections
// 4, 5 and 8): keeps its stream's window of preprocessed vectors, runs the
// detector once the window is full, and turns the two scores into a verdict.
// The window is updated in place and handed back, like ModbusEntityState.
public final class ScoreModbusSequenceUseCase {

    private final SequenceScorer scorer;
    private final SequenceDetectorBundle bundle;
    private final Clock clock;
    // prev_event_available is 0 exactly on a segment's first event (spec section 5).
    private final int prevEventAvailable;

    public ScoreModbusSequenceUseCase(SequenceScorer scorer, Clock clock) {
        this.scorer = scorer;
        this.bundle = scorer.bundle();
        this.clock = clock;
        // The detector must be the one trained on the vectors this job builds.
        if (!ModbusFeatureSchemaV1.SCHEMA.id().equals(bundle.schemaId())
                || bundle.featureCount() != ModbusFeatureSchemaV1.SCHEMA.featureCount()) {
            throw new IllegalStateException("bundle " + bundle.bundleId() + " was trained on " + bundle.schemaId()
                + ", not " + ModbusFeatureSchemaV1.SCHEMA.id());
        }
        this.prevEventAvailable = ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .filter(d -> d.name().equals("prev_event_available")).mapToInt(FeatureDefinition::index)
            .findFirst().orElseThrow();
    }

    public ModbusScoringResult score(ModbusEntityKey key, FeatureVector vector, ModbusScoreWindow window) {
        // A window filled under another bundle, or none yet: start empty (spec S5).
        if (window == null || !window.bundleId().equals(bundle.bundleId())) {
            window = ModbusScoreWindow.empty(bundle.bundleId(), bundle.sequenceLength(), bundle.featureCount());
        }
        float[] raw = vector.values();
        // A segment start: no sequence crosses it.
        if (raw[prevEventAvailable] == 0f) {
            window.reset();
        }
        float[] preprocessed = bundle.preprocessing().apply(raw);
        // Non-finite input is never scored and never enters the window (spec S7).
        if (!allFinite(preprocessed)) {
            return new ModbusScoringResult(prediction(key, vector, DetectorVerdict.UNSCORABLE, null,
                DetectorTrigger.NONE, window.size(), vector.qualityFlags() | window.flagsOr(), 0L), window);
        }
        window.append(preprocessed, vector.qualityFlags());
        // Fewer than a full window: WARMUP.
        if (!window.isFull()) {
            return new ModbusScoringResult(prediction(key, vector, DetectorVerdict.WARMUP, null,
                DetectorTrigger.NONE, window.size(), window.flagsOr(), 0L), window);
        }
        // A full window: run the detector and apply the frozen decision rule.
        long started = System.nanoTime();
        DetectorScores scores = scorer.score(window.sequence());
        long micros = (System.nanoTime() - started) / 1_000;
        boolean dense = scores.dense() > bundle.denseThreshold();
        boolean temporal = scores.temporal() > bundle.temporalThreshold();
        DetectorVerdict verdict = dense || temporal ? DetectorVerdict.ANOMALY : DetectorVerdict.NORMAL;
        return new ModbusScoringResult(prediction(key, vector, verdict, scores, DetectorTrigger.of(dense, temporal),
            window.size(), window.flagsOr(), micros), window);
    }

    // The prediction's fields: the vector's identity, the stream key, the
    // bundle's identity and thresholds, and this event's outcome.
    private ModbusDetectorPrediction prediction(ModbusEntityKey key, FeatureVector vector, DetectorVerdict verdict,
                                                DetectorScores scores, DetectorTrigger trigger, int windowEvents,
                                                int flags, long micros) {
        return new ModbusDetectorPrediction(
            Prediction.deriveId(vector.eventId(), bundle.name(), bundle.version()),
            vector.eventId(), vector.eventTime(), vector.sensor(), vector.connectionUid(),
            key.clientIp(), key.serverIp(), key.unitId(),
            bundle.name(), bundle.version(), bundle.modelSha(), bundle.schemaId(), bundle.schemaHash(),
            verdict,
            scores == null ? null : (float) scores.dense(),
            scores == null ? null : (float) scores.temporal(),
            (float) bundle.denseThreshold(), (float) bundle.temporalThreshold(),
            trigger, windowEvents, flags, micros, clock.instant());
    }

    private static boolean allFinite(float[] values) {
        for (float v : values) {
            if (!Float.isFinite(v)) {
                return false;
            }
        }
        return true;
    }
}
