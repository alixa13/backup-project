package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.DnsQType;
import io.netsecml.platform.domain.event.DnsQuery;
import io.netsecml.platform.domain.event.DnsRcode;
import io.netsecml.platform.domain.event.DnsResponse;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import io.netsecml.platform.domain.feature.DnsWindowState;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.QualityFlags;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DnsBuildFeaturesUseCaseTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // FIXED_CLOCK and DNS_EVENT are defined here, rather than found already
    // built, because nothing in the codebase defines them yet.
    private static final Instant FIXED_INSTANT = Instant.parse("2026-08-27T10:03:11.402Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_INSTANT, ZoneOffset.UTC);

    // DNS_EVENT is the plan's NXDOMAIN fixture: an event whose response failed,
    // so it doubles as both "a DNS event" (test 1) and "a failed lookup" (test 2).
    private static final DnsResponse NXDOMAIN_RESPONSE =
        new DnsResponse(DnsRcode.NXDOMAIN, false, true, false, 0, 0L, DnsRcode.NXDOMAIN.code());
    private static final DnsEvent DNS_EVENT = dnsEvent(Instant.ofEpochSecond(60_000), NXDOMAIN_RESPONSE, null);

    // Builds a DnsEvent directly from the domain records, per the ruling that
    // application may not import adapter-kafka to reach for its mappers. name,
    // qtype and transId are fixed because no test here depends on the query
    // shape -- only response, enrichment and eventTime vary across fixtures.
    private static DnsEvent dnsEvent(Instant eventTime, DnsResponse response, ConnSnapshotDelta enrichment) {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(SENSOR, eventTime.toString()), eventTime, SENSOR, LogType.DNS, eventTime.toString());
        DnsQuery query = new DnsQuery("example.com", DnsQType.A, 42, DnsQType.A.code());
        return new DnsEvent(envelope, query, response, "10.0.0.5", true, enrichment);
    }

    // Test 1 (the plan's buildsTwentyFourValuesWithTheCommonTierLeading): width
    // 24, equal to the registered schema's own count so the assertion cannot
    // pass by coincidentally matching a hardcoded 24 on both sides; schemaId is
    // dns-feature-v1; index 0 is the common tier's record_count_5m and this is
    // the first record for the key; index 12 is the first DNS value, and getting
    // the tier order backwards would still produce a vector of the right length,
    // which is why this checks the VALUE at 12, not just the width.
    @Test
    void buildsTwentyFourValuesWithTheCommonTierLeading() {
        FeatureBuildResult<DnsWindowState> result =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(DNS_EVENT, DnsWindowState.empty());

        assertEquals(24, result.vector().values().length);
        assertEquals(FeatureSchemaRegistry.byLogType(LogType.DNS).featureCount(),
            result.vector().values().length);
        assertEquals("dns-feature-v1", result.vector().schemaId());
        assertEquals(1f, result.vector().values()[0]);
        assertEquals(DnsRcode.NXDOMAIN.code(), result.vector().values()[12], 1e-6);
    }

    // Test 2 (the plan's anNxdomainResponseCountsAsAFailureInTheWindow): the
    // rolling window's failed_count_5m must see this NXDOMAIN, or a host doing
    // nothing but failed lookups would read failed_count_5m == 0 forever.
    @Test
    void anNxdomainResponseCountsAsAFailureInTheWindow() {
        FeatureBuildResult<DnsWindowState> first =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(DNS_EVENT, DnsWindowState.empty());

        assertEquals(1f, first.vector().values()[2], "failed_count_5m after one NXDOMAIN");
    }

    // Test 3, the whole reason QualityFlags.DNS_RESPONSE_ABSENT exists: NOERROR's
    // numeric code IS 0, the same value a missing response defaults to, so index
    // 12 cannot tell these two events apart on its own -- only qualityFlags can
    // settle that one question. (The window's failed_count_5m at index 2 also
    // differs between them, because an unanswered query counts as failed in the
    // rolling window (DnsBuildFeaturesUseCase.build's own comment) -- that is a
    // separate, real distinction the vector DOES carry; this test does not claim
    // index 12 and the flag are the only differences anywhere in the vector,
    // only that they are what resolves index 12's own ambiguity.)
    @Test
    void indexTwelveAloneCannotDistinguishUnansweredFromNoerrorButTheResponseAbsentFlagCan() {
        DnsEvent unanswered = dnsEvent(Instant.ofEpochSecond(60_000), null, null);
        DnsEvent answeredNoerror = dnsEvent(Instant.ofEpochSecond(60_000),
            new DnsResponse(DnsRcode.NOERROR, false, false, false, 0, 0L, DnsRcode.NOERROR.code()), null);

        FeatureBuildResult<DnsWindowState> unansweredResult =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(unanswered, DnsWindowState.empty());
        FeatureBuildResult<DnsWindowState> answeredResult =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(answeredNoerror, DnsWindowState.empty());

        // Index 12 alone is identical -- this IS the gap the schema cannot
        // close. (The two vectors are NOT identical overall: index 2 differs,
        // asserted next, because an unanswered query counts as failed in the
        // rolling window. This test's claim is scoped to index 12 only -- do
        // not read it as "the two vectors are identical except for the flag".)
        assertEquals(0f, unansweredResult.vector().values()[12], "dns_rcode defaults to 0 with no response");
        assertEquals(0f, answeredResult.vector().values()[12], "NOERROR's own code is 0");

        // An unanswered query still counts as failed in the rolling window;
        // a genuinely successful lookup does not. This is a real difference
        // between the two vectors, visible in the values themselves -- the flag
        // below is the ONLY thing that distinguishes them specifically at index
        // 12, not the only difference between them anywhere.
        assertEquals(1f, unansweredResult.vector().values()[2], "unanswered query counts as failed");
        assertEquals(0f, answeredResult.vector().values()[2], "answered NOERROR query is not failed");

        // Only the out-of-band flag distinguishes them. Checked with a bitmask,
        // not equality against the whole qualityFlags int, so this test does not
        // silently start asserting CONN_ENRICHMENT_ABSENT's state too (both
        // fixtures pass enrichment == null, which is test 4's concern, not this
        // one's).
        assertEquals(QualityFlags.DNS_RESPONSE_ABSENT,
            unansweredResult.vector().qualityFlags() & QualityFlags.DNS_RESPONSE_ABSENT,
            "unanswered query sets DNS_RESPONSE_ABSENT");
        assertEquals(0,
            answeredResult.vector().qualityFlags() & QualityFlags.DNS_RESPONSE_ABSENT,
            "answered NOERROR query does not set DNS_RESPONSE_ABSENT");
    }

    // Both absence bits from ONE build() call. Tests 3 and 4 each vary a single
    // absence with the other held fixed, so neither ever observes the combined
    // case -- and the two flags are set by two separate, unconditional ifs. A
    // regression that made them mutually exclusive (an `else if`) would leave
    // tests 3 and 4 green while silently dropping one bit whenever both apply,
    // which is the ordinary state for an unanswered query in a connection's
    // first five minutes. The exact-equality asserts are deliberate here: this
    // test IS about the whole int, unlike test 3's bitmask.
    @Test
    void bothAbsenceBitsAreSetTogetherAndNeitherWhenBothArePresent() {
        DnsEvent bothAbsent = dnsEvent(Instant.ofEpochSecond(60_000), null, null);
        DnsEvent bothPresent = dnsEvent(Instant.ofEpochSecond(60_000),
            new DnsResponse(DnsRcode.NOERROR, false, true, false, 1, 300L, DnsRcode.NOERROR.code()),
            new ConnSnapshotDelta(100L, 200L, 3L, 4L, 60L));

        int absentFlags = new DnsBuildFeaturesUseCase(FIXED_CLOCK)
            .build(bothAbsent, DnsWindowState.empty()).vector().qualityFlags();
        int presentFlags = new DnsBuildFeaturesUseCase(FIXED_CLOCK)
            .build(bothPresent, DnsWindowState.empty()).vector().qualityFlags();

        assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT | QualityFlags.DNS_RESPONSE_ABSENT, absentFlags,
            "no response and no conn.log snapshot must set BOTH bits, not whichever is checked first");
        assertEquals(QualityFlags.NONE, presentFlags,
            "a fully observed record carries no absence bits at all");
    }

    // Test 4: enrichment absence/presence must control both the flag and indices
    // 6-10, the same pairing CommonFeatureExtractorTest already proves for conn;
    // this confirms DnsBuildFeaturesUseCase actually wires enrichment through
    // rather than dropping it. response is answered (NOERROR) in both fixtures
    // here so DNS_RESPONSE_ABSENT never fires, isolating CONN_ENRICHMENT_ABSENT.
    @Test
    void enrichmentPresenceControlsTheFlagAndIndicesSixToTen() {
        DnsResponse noerror = new DnsResponse(DnsRcode.NOERROR, false, false, false, 0, 0L, DnsRcode.NOERROR.code());
        DnsEvent withoutEnrichment = dnsEvent(Instant.ofEpochSecond(60_000), noerror, null);

        FeatureBuildResult<DnsWindowState> absentResult =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(withoutEnrichment, DnsWindowState.empty());

        assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT, absentResult.vector().qualityFlags(),
            "response present, enrichment absent: only CONN_ENRICHMENT_ABSENT is set");
        assertEquals(0f, absentResult.vector().values()[11], "conn_enrichment_present is 0 with no snapshot");

        ConnSnapshotDelta delta = new ConnSnapshotDelta(111L, 222L, 3L, 4L, 555L);
        DnsEvent withEnrichment = withoutEnrichment.withEnrichment(delta);

        FeatureBuildResult<DnsWindowState> presentResult =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(withEnrichment, DnsWindowState.empty());

        assertEquals(QualityFlags.NONE, presentResult.vector().qualityFlags(),
            "response and enrichment both present: no flags set");
        assertEquals(1f, presentResult.vector().values()[11], "conn_enrichment_present is 1 with a snapshot");

        float[] values = presentResult.vector().values();
        assertArrayEquals(new float[]{111f, 222f, 3f, 4f, 555f},
            new float[]{values[6], values[7], values[8], values[9], values[10]}, 1e-6f,
            "indices 6-10 carry the delta's five values in order");
    }

    // Test 5: the window state returned from one build() must be usable as the
    // input to the next, exactly as ConnBuildFeaturesUseCaseTest's
    // windowedFeaturesAccumulateAcrossCallsForSameKey proves for conn. A second
    // event 2500ms later is the first ever interval observed, so Welford's mean
    // is exactly that interval -- not an approximation this assertion has to
    // tolerate.
    @Test
    void stateCarriesAcrossCallsForTheSameKey() {
        DnsResponse noerror = new DnsResponse(DnsRcode.NOERROR, false, false, false, 0, 0L, DnsRcode.NOERROR.code());
        Instant firstTime = Instant.ofEpochSecond(60_000);
        DnsEvent first = dnsEvent(firstTime, noerror, null);
        DnsEvent second = dnsEvent(firstTime.plusMillis(2500), noerror, null);

        DnsBuildFeaturesUseCase useCase = new DnsBuildFeaturesUseCase(FIXED_CLOCK);
        FeatureBuildResult<DnsWindowState> r1 = useCase.build(first, DnsWindowState.empty());
        FeatureBuildResult<DnsWindowState> r2 = useCase.build(second, r1.newState());

        assertEquals(2f, r2.vector().values()[0], "record_count_5m after the second event");
        assertEquals(2500f, r2.vector().values()[3], 1e-6f,
            "inter_arrival_mean_ms after a single observed 2500ms interval");
    }

    // Test 6: a mis-registered schema must be rejected in the constructor, not
    // discovered as an ArrayIndexOutOfBoundsException on the first record.
    // assertThrows wraps the constructor call itself -- if it wrapped build()
    // too, a passing test would not distinguish "failed at construction" from
    // "failed while building", which is exactly the distinction 9h requires.
    @Test
    void rejectsAMisRegisteredSchemaAtConstructionBeforeAnyBuildIsAttempted() {
        List<FeatureDefinition> definitions = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            definitions.add(new FeatureDefinition(i, "synthetic_" + i, "count",
                FeatureDefinition.MissingPolicy.DEFAULT_ZERO, "synthetic"));
        }
        FeatureSchema wrongWidth = new FeatureSchema("synthetic-20", "1.0.0", "0".repeat(64), definitions);

        assertThrows(IllegalArgumentException.class,
            () -> new DnsBuildFeaturesUseCase(Clock.systemUTC(), wrongWidth));
    }

    // Mirrors ConnBuildFeaturesUseCaseTest.stampsProducedAtFromTheInjectedClock:
    // producedAt must come from the injected clock, not from the event's own
    // eventTime, and sensor must still propagate from the event untouched.
    @Test
    void stampsProducedAtFromTheInjectedClock() {
        Instant fixed = Instant.parse("2026-08-27T10:03:11.402Z");
        DnsBuildFeaturesUseCase fixedClockUseCase = new DnsBuildFeaturesUseCase(Clock.fixed(fixed, ZoneOffset.UTC));

        FeatureBuildResult<DnsWindowState> result = fixedClockUseCase.build(DNS_EVENT, DnsWindowState.empty());

        assertEquals(fixed, result.vector().producedAt());
        assertEquals(SENSOR, result.vector().sensor(), "sensor must propagate from the event");
    }

    // Test 7, mirroring ConnBuildFeaturesUseCaseTest.truncatesProducedAtToMilliseconds:
    // producedAt becomes a DateTime64(3) row_version, so sub-millisecond
    // precision must be truncated at the source rather than silently dropped
    // somewhere downstream.
    @Test
    void truncatesProducedAtToMilliseconds() {
        Instant subMilli = Instant.parse("2026-08-27T10:03:11.402987654Z");
        DnsBuildFeaturesUseCase fixedClockUseCase =
            new DnsBuildFeaturesUseCase(Clock.fixed(subMilli, ZoneOffset.UTC));

        FeatureBuildResult<DnsWindowState> result =
            fixedClockUseCase.build(DNS_EVENT, DnsWindowState.empty());

        assertEquals(subMilli.truncatedTo(ChronoUnit.MILLIS), result.vector().producedAt());
    }
}
