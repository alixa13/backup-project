package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.CommonFeatureExtractor;
import io.netsecml.platform.application.feature.DnsFeatureExtractor;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.DnsResponse;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import io.netsecml.platform.domain.feature.DnsWindowState;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.domain.feature.RecordTimingState;
import io.netsecml.platform.domain.feature.RollingCounters;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

// Assembles dns-feature-v1's 24 values: the protocol-agnostic common tier
// (indices 0-11) that ConnBuildFeaturesUseCase also produces, then DNS's own
// protocol tier (12-23). Structure mirrors ConnBuildFeaturesUseCase throughout
// -- same Clock injection, same constructor overloads, same
// resolved-once-in-the-constructor schema -- because both classes are the same
// pattern applied to a different log type, not two independently-invented ones.
public final class DnsBuildFeaturesUseCase implements BuildFeaturesUseCase<DnsEvent, DnsWindowState> {
    private final DnsFeatureExtractor dnsFeatureExtractor = new DnsFeatureExtractor();

    // Injected so producedAt is deterministic under test, same rationale as
    // ConnBuildFeaturesUseCase's field of the same name.
    private final Clock clock;

    // Resolved once, in the constructor, rather than per record inside build() --
    // see ConnBuildFeaturesUseCase's field of the same name for the full
    // rationale
    // (docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md
    // section 6.1's "wiring, fail-fast startup checks").
    private final FeatureSchema schema;

    // Kept so a future DnsFeatureProcessFunction.open()'s no-arg construction
    // compiles unchanged, same as ConnBuildFeaturesUseCase's no-arg constructor.
    public DnsBuildFeaturesUseCase() {
        this(Clock.systemUTC());
    }

    // Overload used by callers (tests, and the next task) that need a fixed or
    // fake Clock.
    public DnsBuildFeaturesUseCase(Clock clock) {
        this(clock, FeatureSchemaRegistry.byLogType(LogType.DNS));
    }

    // Package-private: lets a test drive this against a schema other than the
    // registered dns-feature-v1, which is the only way to prove the width check
    // below actually runs rather than trusting the registry to always be right.
    // NOT public -- production has exactly one correct schema for this class,
    // and it is the registered one the public constructors resolve.
    DnsBuildFeaturesUseCase(Clock clock, FeatureSchema schema) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");

        // Unlike conn, DNS's layout is fully determined: build() always writes
        // exactly CommonFeatureTierV1.FEATURE_COUNT common values followed by
        // exactly DnsFeatureExtractor.FEATURE_COUNT protocol values -- there is
        // no "wider schema pads the rest with zeros" case for this class to
        // support. So if the registered schema's width disagrees with that sum,
        // every future build() call would throw ArrayIndexOutOfBoundsException
        // trying to arraycopy 24 values into a too-narrow (or leave a too-wide
        // one partially zeroed) array. Checking once here, at construction,
        // turns a per-record failure mode into a fail-fast startup check -- the
        // same failure-mode hoist this project already did once elsewhere.
        int expectedWidth = CommonFeatureTierV1.FEATURE_COUNT + DnsFeatureExtractor.FEATURE_COUNT;
        if (schema.featureCount() != expectedWidth) {
            throw new IllegalArgumentException(
                "schema '" + schema.id() + "' declares " + schema.featureCount() + " features, but common tier ("
                    + CommonFeatureTierV1.FEATURE_COUNT + ") + dns protocol tier (" + DnsFeatureExtractor.FEATURE_COUNT
                    + ") = " + expectedWidth + " is what DnsBuildFeaturesUseCase writes");
        }
    }

    @Override
    public FeatureBuildResult<DnsWindowState> build(DnsEvent event, DnsWindowState currentState) {
        DnsResponse response = event.response();

        // An unanswered query counts as failed in the rolling window, same
        // as conn's S0 (an attempt that got no reply). Capture that sees only
        // the query side of DNS traffic -- a resolver whose replies never reach
        // this sensor -- will inflate failed_count_5m exactly as it inflates
        // conn's S0 count for a one-sided TCP capture. That is a known
        // limitation of a passive capture point, not a bug in this fold.
        boolean failed = response == null || response.rcode().isFailure();

        // Folds this event into the bounded rolling window and the inter-arrival
        // timing. Both .record() and .observe() return NEW state; the caller
        // (DnsFeatureProcessFunction) is responsible for storing newState back
        // into keyed state.
        long bucketMinute = event.eventTime().getEpochSecond() / 60;

        // bytes are always 0L -- dns.log carries no byte counts, so this
        // window's byte_sum_5m for DNS stays 0 unless a later unit adds one.
        // Unlike conn, there is no measurement to sum here.
        RollingCounters newCounters = currentState.counters().record(bucketMinute, 0L, failed);
        RecordTimingState newTiming = currentState.timing().observe(event.eventTime());
        DnsWindowState newState = new DnsWindowState(newCounters, newTiming);

        // The common tier reads the state AFTER folding this event in -- same
        // as conn's indices 17-19 -- so this record's own contribution is
        // reflected in its own vector, not only in the next one.
        float[] commonTier = CommonFeatureExtractor.extract(newCounters, newTiming, event.isOrig(), event.enrichment());
        float[] protocolTier = dnsFeatureExtractor.extractProtocolTier(event);

        // Common tier then protocol tier, at the width the schema declares
        // (validated once above, so this arraycopy pair can never run off the
        // end of `values`). DnsFeatureSchemaV1Test.theFirstTwelveFeaturesAreTheCommonTierInOrder
        // is what pins this order as contract.
        float[] values = new float[schema.featureCount()];
        System.arraycopy(commonTier, 0, values, 0, CommonFeatureTierV1.FEATURE_COUNT);
        System.arraycopy(protocolTier, 0, values, CommonFeatureTierV1.FEATURE_COUNT, DnsFeatureExtractor.FEATURE_COUNT);

        // The two provenance bits, OR'd together and independent of one
        // another. Neither is exceptional -- an idle connection has no conn.log
        // snapshot yet, and dns.log genuinely records queries nothing answered
        // -- but both must be visible outside the frozen values array, because
        // the array alone cannot distinguish either absence from a real zero.
        int qualityFlags = QualityFlags.NONE;
        if (event.enrichment() == null) {
            qualityFlags |= QualityFlags.CONN_ENRICHMENT_ABSENT;
        }
        if (response == null) {
            qualityFlags |= QualityFlags.DNS_RESPONSE_ABSENT;
        }

        // logType and connectionUid pass straight through from the event,
        // unchanged. producedAt is truncated to milliseconds because it lands in
        // a DateTime64(3) row_version; finer precision would not round-trip.
        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            event.sensor(),
            event.logType(),
            event.connectionUid(),
            schema.id(),
            schema.contentHash(),
            values,
            qualityFlags,
            clock.instant().truncatedTo(ChronoUnit.MILLIS));

        return new FeatureBuildResult<>(vector, newState);
    }
}
