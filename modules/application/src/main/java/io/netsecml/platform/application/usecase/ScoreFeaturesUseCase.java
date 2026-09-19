package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.model.ModelRef;
import io.netsecml.platform.port.out.ModelScorer;
import java.time.Clock;
import java.util.Objects;

// Turns one feature vector into one prediction. This is the ONLY place the
// threshold decision (score >= threshold) is computed anywhere in the
// codebase -- Prediction's own constructor deliberately leaves decision
// unchecked against score/threshold (see its comment) precisely so this rule
// has exactly one home and cannot drift between two.
public final class ScoreFeaturesUseCase {

    private final ModelScorer scorer;

    // Injected so producedAt is deterministic under test, the same reason
    // every BuildFeaturesUseCase implementation in this module takes a Clock
    // rather than calling Clock.systemUTC() itself.
    private final Clock clock;

    public ScoreFeaturesUseCase(ModelScorer scorer, Clock clock) {
        this.scorer = Objects.requireNonNull(scorer, "scorer must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public Prediction score(FeatureVector vector) {
        ModelRef ref = scorer.ref();

        // The bundle names the schema it was trained against. A mismatch means
        // the deployment paired a model with the wrong feature order, which
        // would score confidently and wrongly -- so it throws rather than
        // degrading. Both schemaId and schemaHash are checked, and both the
        // model's expected pair and the vector's actual pair are named in the
        // message, so a same-id-different-hash mismatch (a schema that drifted
        // under an unchanged id) is just as diagnosable from one log line as an
        // outright different schema.
        if (!ref.schemaId().equals(vector.schemaId()) || !ref.schemaHash().equals(vector.schemaHash())) {
            throw new IllegalStateException("model " + ref.name() + ":" + ref.version()
                + " expects schema " + ref.schemaId() + "/" + ref.schemaHash()
                + " but the vector carries " + vector.schemaId() + "/" + vector.schemaHash());
        }

        // Only the scorer call itself is timed -- schema validation above and
        // Prediction construction below are microseconds of pure Java and would
        // only dilute a number meant to reflect model inference cost (e.g. an
        // ONNX Runtime session run).
        long startNanos = System.nanoTime();
        float score = (float) scorer.score(vector.values());
        long inferenceMicros = (System.nanoTime() - startNanos) / 1_000L;

        // Identity: predictionId is re-derived deterministically from
        // (eventId, model name, model version) rather than minted, so replay
        // recomputes the same id. schemaId/schemaHash on the Prediction come
        // from the VECTOR, not the ref -- they already passed the equality
        // check above, and the vector is the thing that was actually scored.
        return new Prediction(
            Prediction.deriveId(vector.eventId(), ref.name(), ref.version()),
            vector.eventId(), vector.eventTime(), ref.name(), ref.version(), ref.modelSha(),
            vector.schemaId(), vector.schemaHash(), score, score >= ref.threshold(), ref.threshold(),
            inferenceMicros, vector.qualityFlags(), clock.instant());
    }
}
