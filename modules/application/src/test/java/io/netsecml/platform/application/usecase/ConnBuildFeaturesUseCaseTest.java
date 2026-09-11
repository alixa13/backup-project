package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.ConnWindowState;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import static org.junit.jupiter.api.Assertions.*;

class ConnBuildFeaturesUseCaseTest {
    private final ConnBuildFeaturesUseCase useCase = new ConnBuildFeaturesUseCase();

    // Returns ConnEvent, not NetworkEvent: build() now takes ConnEvent directly,
    // so every call site needs the narrow type. The body is unchanged -- it
    // already constructed a ConnEvent, it just used to be widened on return.
    private ConnEvent event(Instant eventTime, long originBytes, long responseBytes, boolean failed) {
        SensorId sensor = new SensorId("sensor-eu-1");
        ConnectionState state = failed ? ConnectionState.S0 : ConnectionState.SF;
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, state);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, originBytes, responseBytes, 5, 5, 0);
        // LogType.CONN and a non-blank uid are required positional components now;
        // this use case test only exercises feature-building math, so a fixed
        // constant and the same timestamp-derived id used for eventId are enough.
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(sensor, eventTime.toString()), eventTime, sensor, LogType.CONN, eventTime.toString());
        return new ConnEvent(envelope, tuple, measurements, new ConnectionLocality(null, null));
    }

    // The FREEZE guard: conn's schema id, hash and width are literals here on
    // purpose. A wrong value in the registry would still pass the provenance
    // test below (both sides would move together) -- this test is what catches
    // that a registry edit silently changed the frozen conn-feature-v1 contract.
    @Test
    void producesTwentyValueVectorWithFrozenSchemaIdentity() {
        ConnEvent e = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult<ConnWindowState> result = useCase.build(e, ConnWindowState.empty());
        assertEquals(20, result.vector().values().length);
        assertEquals("conn-feature-v1", result.vector().schemaId());
        assertEquals("f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b", result.vector().schemaHash());
        assertEquals(e.eventId().value(), result.vector().eventId());
    }

    @Test
    void windowedFeaturesAccumulateAcrossCallsForSameKey() {
        ConnWindowState state = ConnWindowState.empty();

        ConnEvent first = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult<ConnWindowState> r1 = useCase.build(first, state);
        assertEquals(1f, r1.vector().values()[17], "source_connections_5m after first event");
        assertEquals(300f, r1.vector().values()[18], "source_bytes_5m after first event (total_bytes=100+200)");
        assertEquals(0f, r1.vector().values()[19], "source_failed_connections_5m, first event was not failed");

        ConnEvent second = event(Instant.ofEpochSecond(60_030), 50, 50, true);
        FeatureBuildResult<ConnWindowState> r2 = useCase.build(second, r1.newState());
        assertEquals(2f, r2.vector().values()[17], "source_connections_5m after second event, same minute bucket");
        assertEquals(400f, r2.vector().values()[18], "300 + total_bytes(50+50)=100 = 400");
        assertEquals(1f, r2.vector().values()[19], "second event was failed (S0)");
    }

    // An injected Clock is what makes producedAt assertable. Without it the field
    // would only ever be testable as "not null", which asserts nothing useful.
    @Test
    void stampsProducedAtFromTheInjectedClock() {
        Instant fixed = Instant.parse("2026-08-27T10:03:11.402Z");
        ConnBuildFeaturesUseCase fixedClockUseCase =
            new ConnBuildFeaturesUseCase(Clock.fixed(fixed, ZoneOffset.UTC));

        ConnEvent e = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult<ConnWindowState> result = fixedClockUseCase.build(e, ConnWindowState.empty());

        assertEquals(fixed, result.vector().producedAt());
        assertEquals(e.sensor(), result.vector().sensor(), "sensor must propagate from the event");
    }

    // producedAt becomes a DateTime64(3) row_version. Sub-millisecond precision
    // would not survive the round trip, so it is truncated at the source.
    @Test
    void truncatesProducedAtToMilliseconds() {
        Instant subMilli = Instant.parse("2026-08-27T10:03:11.402987654Z");
        ConnBuildFeaturesUseCase fixedClockUseCase =
            new ConnBuildFeaturesUseCase(Clock.fixed(subMilli, ZoneOffset.UTC));

        FeatureBuildResult<ConnWindowState> result = fixedClockUseCase.build(
            event(Instant.ofEpochSecond(60_000), 100, 200, false), ConnWindowState.empty());

        assertEquals(subMilli.truncatedTo(ChronoUnit.MILLIS), result.vector().producedAt());
    }

    // The width comes from the schema, not a literal. For conn the schema reports
    // 20, so this asserts the same number the old hardcoded array produced -- the
    // point is where the number comes from, not what it is.
    @Test
    void vectorLengthComesFromTheRegisteredSchema() {
        FeatureBuildResult<ConnWindowState> result =
            useCase.build(event(Instant.ofEpochSecond(60_000), 100, 200, false), ConnWindowState.empty());

        assertEquals(FeatureSchemaRegistry.byLogType(LogType.CONN).featureCount(),
            result.vector().values().length);
        assertEquals(20, result.vector().values().length,
            "conn's frozen schema is 20 wide and must stay so");
    }

    // The schema id and content hash on the vector must come from the registry
    // too, so a vector can never claim a schema its values do not match.
    @Test
    void vectorCarriesTheRegisteredSchemasIdentity() {
        FeatureBuildResult<ConnWindowState> result =
            useCase.build(event(Instant.ofEpochSecond(60_000), 100, 200, false), ConnWindowState.empty());

        FeatureSchema schema = FeatureSchemaRegistry.byLogType(LogType.CONN);
        assertEquals(schema.id(), result.vector().schemaId());
        assertEquals(schema.contentHash(), result.vector().schemaHash());
    }
}
