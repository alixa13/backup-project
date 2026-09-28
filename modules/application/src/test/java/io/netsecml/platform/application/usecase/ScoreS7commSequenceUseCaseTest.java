package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.domain.model.S7commConformalPolicy;
import io.netsecml.platform.domain.model.S7commDetectorBundle;
import io.netsecml.platform.domain.model.S7commPreprocessing;
import io.netsecml.platform.port.out.ReconstructionScorer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The use case with a stub detector: the warm-up, resets, the weighted
// last-event score, the group-conditional decision, the flags and the
// bundle-id rule (scoring design sections 3-5).
class ScoreS7commSequenceUseCaseTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);

    // Identity continuous transforms, so a raw continuous value is its preprocessed value.
    private static S7commPreprocessing identity() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("s" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        return new S7commPreprocessing(cs, List.of("is_request_direction", "s7_function_changed"), 20.0,
            List.of("1", "3", "7"), List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
    }

    // RESPONSE: four scores, alpha 0.2. READ_REQUEST: one score, no alpha of its
    // own (the fallback). WRITE_REQUEST: one score, alpha 0.5. OTHER_REQUEST: no
    // scores (pooled, fallback alpha).
    private static S7commConformalPolicy policy() {
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        scores.put(S7commScoreGroup.RESPONSE, new double[]{0.1, 0.2, 0.3, 0.4});
        scores.put(S7commScoreGroup.READ_REQUEST, new double[]{0.05});
        scores.put(S7commScoreGroup.WRITE_REQUEST, new double[]{0.5});
        scores.put(S7commScoreGroup.OTHER_REQUEST, new double[0]);
        return new S7commConformalPolicy(scores,
            Map.of(S7commScoreGroup.RESPONSE, 0.2, S7commScoreGroup.WRITE_REQUEST, 0.5), 0.001);
    }

    private static S7commDetectorBundle bundle(String schemaId) {
        S7commPreprocessing p = identity();
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64), schemaId,
            S7commFeatureSchemaV1.CONTENT_HASH, 16, 21, "input", "reconstruction", p,
            p.scoreWeights(List.of("s7_operation")), policy());
    }

    // A stub detector: it reconstructs the last row with `offset` added to
    // column 0 (weight 1) and 100 added to column 20 (an s7_operation column,
    // weight 0), so the score is offset^2 / 17 whatever column 20's error.
    private static final class StubScorer implements ReconstructionScorer {
        private final S7commDetectorBundle bundle;
        float offset = 0f;
        int calls = 0;

        StubScorer(S7commDetectorBundle bundle) {
            this.bundle = bundle;
        }

        @Override
        public S7commDetectorBundle bundle() {
            return bundle;
        }

        @Override
        public float[] reconstructLast(float[][] window) {
            calls++;
            float[] row = window[window.length - 1].clone();
            row[0] += offset;
            row[20] += 100f;
            return row;
        }

        @Override
        public void close() {
        }
    }

    private final StubScorer scorer = new StubScorer(bundle(S7commFeatureSchemaV1.SCHEMA.id()));
    // The window's own warm-up (15 events: the 16th is the first full window), so these
    // tests read the window's behaviour; the default 64-event warm-up has its own tests
    // (spec amendment A1).
    private final ScoreS7commSequenceUseCase useCase = new ScoreS7commSequenceUseCase(scorer, CLOCK, 15);

    // One s7comm-feature-v1 vector: a request with this operation code, or a
    // response; feature 0 is i / 100.
    private static FeatureVector vector(int i, boolean request, int operation, int flags) {
        float[] v = new float[16];
        v[0] = i / 100f;
        v[12] = request ? 1f : 0f;
        v[14] = request ? 1f : 3f;
        v[15] = operation;
        return new FeatureVector("s:C1:" + i + ":" + (request ? "REQUEST" : "RESPONSE") + ":" + i,
            Instant.ofEpochSecond(1000 + i), new SensorId("s"), LogType.S7COMM, "C1",
            S7commFeatureSchemaV1.SCHEMA.id(), S7commFeatureSchemaV1.CONTENT_HASH, v, flags,
            Instant.ofEpochSecond(1000 + i));
    }

    // `n` response events on one connection, the first fresh; the window after them.
    private S7commScoreWindow feed(int n) {
        S7commScoreWindow window = null;
        for (int i = 0; i < n; i++) {
            window = useCase.score(vector(i, false, 4, 0), i == 0, "10.0.0.5", "10.0.0.9", window).window();
        }
        return window;
    }

    @Test
    void theFirstFifteenEventsWarmUpAndTheSixteenthIsScored() {
        S7commScoreWindow window = null;
        List<S7commDetectorPrediction> out = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            S7commScoringResult r = useCase.score(vector(i, false, 4, 0), i == 0, "10.0.0.5", "10.0.0.9", window);
            window = r.window();
            out.add(r.prediction());
        }
        for (int i = 0; i < 15; i++) {
            assertEquals(DetectorVerdict.WARMUP, out.get(i).verdict(), "event " + i);
            assertNull(out.get(i).score());
            assertEquals(i + 1, out.get(i).eventsSinceReset());
        }
        assertEquals(DetectorVerdict.NORMAL, out.get(15).verdict());
        assertEquals(0f, out.get(15).score());
        assertEquals(1.0, out.get(15).pValue(), "every calibration score is >= 0: p = 5/5");
        assertEquals(16, out.get(15).eventsSinceReset());
        assertEquals(1, scorer.calls, "the detector runs only on a full window");
    }

    // Spec section 5 (D4): the feature engine started the connection again, so
    // the window and its count start again too.
    @Test
    void freshStateStartsTheWindowAndTheCountAgain() {
        S7commScoreWindow window = feed(16);
        S7commScoringResult again = useCase.score(vector(16, false, 4, 0), true, "a", "b", window);
        assertEquals(DetectorVerdict.WARMUP, again.prediction().verdict());
        assertEquals(1, again.prediction().eventsSinceReset());
        S7commScoringResult next = useCase.score(vector(17, false, 4, 0), false, "a", "b", again.window());
        assertEquals(2, next.prediction().eventsSinceReset());
    }

    // causal_shadow.py: only the last event, weighted; column 20 weighs 0.
    @Test
    void theScoreIsTheWeightedErrorOfTheLastEventOnly() {
        S7commScoreWindow window = feed(15);
        scorer.offset = 3f;
        S7commDetectorPrediction p = useCase.score(vector(15, false, 4, 0), false, "a", "b", window).prediction();
        assertEquals(9f / 17f, p.score(), 1e-6f, "offset^2 / 17");
    }

    @Test
    void anAnomalyIsAPValueAtMostItsGroupsAlpha() {
        S7commScoreWindow window = feed(15);
        scorer.offset = 3f;
        S7commDetectorPrediction p = useCase.score(vector(15, false, 4, 0), false, "a", "b", window).prediction();
        assertEquals(S7commScoreGroup.RESPONSE, p.scoreGroup());
        assertEquals(0.2, p.pValue(), 1e-12, "0.53 is above all four RESPONSE scores: 1/5");
        assertEquals(0.2, p.alpha());
        assertEquals(DetectorVerdict.ANOMALY, p.verdict());
    }

    @Test
    void theGroupAndItsAlphaFollowTheVector() {
        assertGroup(true, 4, S7commScoreGroup.READ_REQUEST, 0.001);
        assertGroup(true, 5, S7commScoreGroup.WRITE_REQUEST, 0.5);
        assertGroup(true, 0, S7commScoreGroup.OTHER_REQUEST, 0.001);
        assertGroup(false, 5, S7commScoreGroup.RESPONSE, 0.2);
    }

    private void assertGroup(boolean request, int operation, S7commScoreGroup group, double alpha) {
        S7commDetectorPrediction p = useCase.score(vector(0, request, operation, 0), true, "a", "b", null)
            .prediction();
        assertEquals(group, p.scoreGroup());
        assertEquals(alpha, p.alpha());
    }

    @Test
    void qualityFlagsAreOrdOverTheWindow() {
        S7commScoreWindow window = null;
        S7commDetectorPrediction p = null;
        for (int i = 0; i < 16; i++) {
            S7commScoringResult r = useCase.score(vector(i, false, 4, i == 3 ? 16 : 0), i == 0, "a", "b", window);
            window = r.window();
            p = r.prediction();
        }
        assertEquals(16, p.qualityFlags(), "event 3's S7COMM_OUT_OF_ORDER is still in the window");
        for (int i = 16; i < 20; i++) {
            S7commScoringResult r = useCase.score(vector(i, false, 4, 0), false, "a", "b", window);
            window = r.window();
            p = r.prediction();
        }
        assertEquals(0, p.qualityFlags(), "and gone once it left");
    }

    // A window filled under another bundle is never scored by this one.
    @Test
    void aWindowFromAnotherBundleIsEmptiedFirst() {
        S7commScoreWindow other = S7commScoreWindow.empty("s7comm-stage1-detector/v0", 16, 21);
        for (int i = 0; i < 16; i++) {
            other.append(new float[21], 0);
        }
        S7commDetectorPrediction p = useCase.score(vector(0, false, 4, 0), false, "a", "b", other).prediction();
        assertEquals(DetectorVerdict.WARMUP, p.verdict());
        assertEquals(1, p.eventsSinceReset());
    }

    // Plan ruling P2: a non-finite score is UNSCORABLE, never NORMAL; the row stays.
    @Test
    void aNonFiniteReconstructionIsUnscorable() {
        S7commScoreWindow window = feed(15);
        scorer.offset = Float.NaN;
        S7commScoringResult r = useCase.score(vector(15, false, 4, 0), false, "a", "b", window);
        assertEquals(DetectorVerdict.UNSCORABLE, r.prediction().verdict());
        assertNull(r.prediction().score());
        assertNull(r.prediction().pValue());
        scorer.offset = 0f;
        assertEquals(DetectorVerdict.NORMAL,
            useCase.score(vector(16, false, 4, 0), false, "a", "b", r.window()).prediction().verdict(),
            "the window was kept");
    }

    @Test
    void aBundleForAnotherSchemaIsRefused() {
        assertThrows(IllegalStateException.class,
            () -> new ScoreS7commSequenceUseCase(new StubScorer(bundle("modbus-feature-v1")), CLOCK));
    }

    @Test
    void thePredictionCarriesTheEventsAndTheBundlesIdentity() {
        FeatureVector v = vector(0, true, 4, 0);
        S7commDetectorPrediction p = useCase.score(v, true, "10.0.0.5", "10.0.0.9", null).prediction();
        assertEquals(Prediction.deriveId(v.eventId(), "s7comm-stage1-detector", "v1"), p.predictionId());
        assertEquals(v.eventId(), p.eventId());
        assertEquals("C1", p.connectionUid());
        assertEquals("10.0.0.5", p.clientIp());
        assertEquals("10.0.0.9", p.serverIp());
        assertEquals("b".repeat(64), p.modelSha());
        assertEquals(S7commFeatureSchemaV1.SCHEMA.id(), p.schemaId());
        assertEquals(Instant.parse("2026-09-28T12:00:00Z"), p.producedAt());
    }

    // Amendment A1: by default the first 64 events after a reset are WARMUP, the 65th is scored.
    @Test
    void theDefaultWarmUpIsSixtyFourEvents() {
        ScoreS7commSequenceUseCase byDefault = new ScoreS7commSequenceUseCase(scorer, CLOCK);
        S7commScoreWindow window = null;
        List<S7commDetectorPrediction> out = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            S7commScoringResult r = byDefault.score(vector(i, false, 4, 0), i == 0, "a", "b", window);
            window = r.window();
            out.add(r.prediction());
        }
        for (int i = 0; i < 64; i++) {
            assertEquals(DetectorVerdict.WARMUP, out.get(i).verdict(), "event " + i);
        }
        assertEquals(DetectorVerdict.NORMAL, out.get(64).verdict());
        assertEquals(65, out.get(64).eventsSinceReset());
        assertEquals(1, scorer.calls, "the detector runs only once the warm-up is over");
    }

    @Test
    void aWarmUpShorterThanTheWindowIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new ScoreS7commSequenceUseCase(scorer, CLOCK, 14));
    }
}
