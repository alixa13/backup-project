package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.DnsFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.model.ModelRef;
import io.netsecml.platform.port.out.ModelScorer;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScoreFeaturesUseCaseTest {

    // Fixed so producedAt is assertable, the same reason every other use case
    // test in this module injects a Clock rather than using Clock.systemUTC().
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-09-19T10:00:00Z"), ZoneOffset.UTC);

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // A stub scorer -- the decision rule must be testable with no ONNX Runtime
    // on the classpath, which is the whole reason ModelScorer is this narrow.
    private static ModelScorer stub(double probability, ModelRef ref) {
        return new ModelScorer() {
            @Override public double score(float[] values) { return probability; }
            @Override public ModelRef ref() { return ref; }
            @Override public void close() { }
        };
    }

    // A conn-feature-v1 bundle bound to conn's own frozen schema identity.
    private static ModelRef ref(float threshold) {
        return new ModelRef("conn-demo", "v1", "conn-feature-v1", ConnFeatureSchemaV1.CONTENT_HASH,
            "a".repeat(64), threshold, List.of("benign", "attack"), "probabilities", 0);
    }

    // A conn-feature-v1 vector, 20 values wide, carrying the given qualityFlags.
    private static FeatureVector vector(int qualityFlags) {
        return new FeatureVector("evt-1", Instant.parse("2026-09-19T09:59:00Z"), SENSOR, LogType.CONN, "uid-1",
            "conn-feature-v1", ConnFeatureSchemaV1.CONTENT_HASH, new float[20], qualityFlags,
            Instant.parse("2026-09-19T09:59:01Z"));
    }

    // A vector carrying an arbitrary other schema's identity, so it can never
    // match the conn-feature-v1 ModelRef the tests bind their stub scorers to.
    private static FeatureVector vectorWithSchema(String schemaId, String schemaHash) {
        return new FeatureVector("evt-1", Instant.parse("2026-09-19T09:59:00Z"), SENSOR, LogType.DNS, "uid-1",
            schemaId, schemaHash, new float[24], QualityFlags.NONE, Instant.parse("2026-09-19T09:59:01Z"));
    }

    @Test
    void aScoreAtOrAboveTheThresholdDecidesAttack() {
        ModelRef ref = ref(0.5f);
        Prediction p = new ScoreFeaturesUseCase(stub(0.5, ref), FIXED_CLOCK).score(vector(QualityFlags.NONE));
        assertTrue(p.decision(), "score == threshold must decide attack, not fall through");
        assertEquals(0.5f, p.score(), 1e-6f);
        assertEquals(0.5f, p.threshold(), 1e-6f, "the threshold travels on the prediction, from the bundle");
    }

    @Test
    void aVectorFromADifferentSchemaIsRejected() {
        // A model scoring a schema it was not trained on is a deployment error.
        ModelRef ref = ref(0.5f);
        FeatureVector other = vectorWithSchema("dns-feature-v1", DnsFeatureSchemaV1.CONTENT_HASH);
        assertThrows(IllegalStateException.class,
            () -> new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(other));
    }

    @Test
    void theVectorsQualityFlagsRideOntoThePrediction() {
        // A prediction made on a vector with absent enrichment must stay visibly
        // degraded downstream; dropping the flags would launder that away.
        ModelRef ref = ref(0.5f);
        Prediction p = new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK)
            .score(vector(QualityFlags.CONN_ENRICHMENT_ABSENT));
        assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT, p.qualityFlags());
    }

    @Test
    void theIdentityFieldsComeFromTheVectorAndTheModelRef() {
        ModelRef ref = ref(0.5f);
        Prediction p = new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(vector(QualityFlags.NONE));
        assertEquals(Prediction.deriveId(p.eventId(), "conn-demo", "v1"), p.predictionId());
        assertEquals("conn-feature-v1", p.schemaId());
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, p.schemaHash());
    }

    @Test
    void aScoreBelowTheThresholdDecidesNotAttack() {
        // The counterpart to the >= boundary test above: strictly below must
        // decide false, not just "not >=" by accident of a wrong comparator.
        ModelRef ref = ref(0.5f);
        Prediction p = new ScoreFeaturesUseCase(stub(0.4, ref), FIXED_CLOCK).score(vector(QualityFlags.NONE));
        assertTrue(!p.decision(), "score below threshold must not decide attack");
    }

    @Test
    void schemaMismatchMessageNamesBothTheExpectedAndActualSchema() {
        // The whole point of naming both sides is that a mispaired deployment is
        // diagnosable from the one log line this exception produces.
        ModelRef ref = ref(0.5f);
        FeatureVector other = vectorWithSchema("dns-feature-v1", DnsFeatureSchemaV1.CONTENT_HASH);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(other));
        assertTrue(ex.getMessage().contains("conn-feature-v1"), "must name the model's expected schema id");
        assertTrue(ex.getMessage().contains(ConnFeatureSchemaV1.CONTENT_HASH), "must name the model's expected schema hash");
        assertTrue(ex.getMessage().contains("dns-feature-v1"), "must name the vector's actual schema id");
        assertTrue(ex.getMessage().contains(DnsFeatureSchemaV1.CONTENT_HASH), "must name the vector's actual schema hash");
    }

    @Test
    void aSchemaHashDriftUnderTheSameSchemaIdIsStillRejected() {
        // Proves the check does not short-circuit on schemaId alone: a schema
        // definition that drifted while keeping the same id (a corrupted or
        // stale registry entry) must be caught too, not just an outright
        // different schema.
        ModelRef ref = ref(0.5f);
        FeatureVector driftedHash = vectorWithSchema("conn-feature-v1", "0".repeat(64));
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(driftedHash));
        assertTrue(ex.getMessage().contains(ConnFeatureSchemaV1.CONTENT_HASH), "must name the model's expected schema hash");
        assertTrue(ex.getMessage().contains("0".repeat(64)), "must name the vector's actual (drifted) schema hash");
    }

    @Test
    void producedAtComesFromTheInjectedClock() {
        ModelRef ref = ref(0.5f);
        Prediction p = new ScoreFeaturesUseCase(stub(0.9, ref), FIXED_CLOCK).score(vector(QualityFlags.NONE));
        assertEquals(FIXED_CLOCK.instant(), p.producedAt());
    }
}
