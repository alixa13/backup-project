package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.domain.event.LogType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// The chain loop's structure, asserted without Docker.
//
// getStreamGraph() builds the job graph in memory, so the topology can be
// inspected with no Kafka and no ClickHouse. That matters: the container tests
// that would otherwise cover this are the slowest in the project and one of them
// cannot run on a small machine at all.
class ArchiveJobTopologyTest {

    private static StreamExecutionEnvironment buildJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));
        return env;
    }

    // Two log types in, two source-to-sink chains out. This is what must scale to
    // six without the method growing.
    @Test
    void buildsOneChainPerRegisteredLogType() {
        Set<String> uids = operatorUids();

        assertTrue(uids.contains("feature-vector-source"), "feature vector chain must be present");
        assertTrue(uids.contains("dlq-source"), "dlq chain must be present");
    }

    // Every operator needs a stable uid or Flink derives one from the topology
    // hash, and any later edit silently discards checkpoint state instead of
    // failing loudly. The loop must not drop these.
    @Test
    void everyOperatorCarriesAnExplicitUid() {
        Set<String> uids = operatorUids();

        assertTrue(uids.containsAll(Set.of(
            "feature-vector-source", "feature-vector-row", "feature-vectors-clickhouse-sink",
            "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            "expected the six pre-existing operator uids, found: " + uids);
    }

    // everyOperatorCarriesAnExplicitUid above and operatorUidsAreUniqueAcrossChains
    // below both SKIP any node whose getTransformationUID() is null -- so a
    // seventh chain wired without .uid() would pass every test in this class
    // except this one. This is the test that actually fails on a missing uid.
    //
    // Verified rather than assumed: this job's ClickHouseBatchSink is a plain
    // Flink Sink V2 (implements Sink<T> directly, not TwoPhaseCommittingSink), so
    // sinkTo() expands to exactly one Writer StreamNode per chain -- there is no
    // separate, uid-less Committer/GlobalCommitter node for a two-phase-commit
    // sink to legitimately lack a uid for. Printing every node's id/uid/operator
    // name for this exact topology confirmed all six nodes (two sources, two
    // maps, two sink writers) carry the uid this job assigns them and nothing
    // else exists in the graph. If a future chain's sink ever becomes a
    // TwoPhaseCommittingSink, this assertion would need to scope down to the
    // source/map/writer node types and say so here -- it must not be weakened to
    // vacuity or deleted.
    @Test
    void everyStreamNodeCarriesANonNullUid() {
        StreamExecutionEnvironment env = buildJob();

        for (StreamNode node : env.getStreamGraph(false).getStreamNodes()) {
            assertNotNull(node.getTransformationUID(),
                "stream node " + node.getId() + " (" + node.getOperatorName() + ") has no uid -- "
                + "every operator in this job must carry an explicit .uid()");
        }
    }

    // Uids must be unique across chains. Two operators sharing one is how a
    // six-log-type loop silently collides state, and it is exactly the failure a
    // hand-written topology cannot have but a generated one can.
    @Test
    void operatorUidsAreUniqueAcrossChains() {
        StreamExecutionEnvironment env = buildJob();

        Set<String> seen = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            String uid = node.getTransformationUID();
            if (uid != null) {
                assertTrue(seen.add(uid), "duplicate operator uid across chains: " + uid);
            }
        });
    }

    // The parameterised overload
    // docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
    // §6.3 requires: the set of log types is a list, so a third log type is a
    // third list entry, wired identically to the two the 5-argument overload
    // builds. This is what proves adding a log type is one entry, not two new
    // String parameters on build() itself.
    @Test
    void parameterisedBuildWiresOneChainPerListEntry() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", List.of(
            new ArchiveJob.LogTypeChain<>("netsec.conn.feature-vector.v1",
                new FeatureVectorRowMapFunction("netsec.conn.feature-vector.v1"), "feature_vectors",
                "feature-vector-source", "feature-vector-row", "feature-vectors-clickhouse-sink"),
            new ArchiveJob.LogTypeChain<>("netsec.conn.dlq.v1",
                new InvalidEventRowMapFunction("netsec.conn.dlq.v1", LogType.CONN), "invalid_events",
                "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink"),
            // LogType.CONN here is a placeholder: there is no LogType.HTTP (only
            // CONN exists -- see LogType's own comment), so this chain's
            // "http"-named topic and uids are labels exercising uid uniqueness
            // across a third chain, not a second real log type. This test's
            // subject is topology wiring -- N chains produce N*3 distinct uids --
            // not log-type semantics. Unit 3 should switch this to a real second
            // log type (e.g. DNS) once one lands.
            new ArchiveJob.LogTypeChain<>("netsec.http.dlq.v1",
                new InvalidEventRowMapFunction("netsec.http.dlq.v1", LogType.CONN), "invalid_events",
                "http-dlq-source", "http-invalid-event-row", "http-invalid-events-clickhouse-sink")),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));

        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });

        assertTrue(uids.containsAll(Set.of("http-dlq-source", "http-invalid-event-row",
            "http-invalid-events-clickhouse-sink")), "a third registered chain must produce its own three uids");
        assertEquals(9, uids.size(), "three chains of three operators each, no collisions");
    }

    // The conn DLQ chain keeps its historical uids. They are checkpoint state
    // identity: a job restoring from an existing checkpoint looks them up by
    // exactly these strings, so renaming them to match a new per-protocol pattern
    // would silently discard that operator's state instead of failing.
    @Test
    void theConnDlqChainKeepsItsHistoricalUids() {
        Set<String> uids = operatorUids();

        assertTrue(uids.containsAll(Set.of(
            "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            "the conn DLQ chain's original uids must survive, found: " + uids);
    }

    // dlqChain is the single place that knows the conn uids, so this pins that it
    // reproduces them rather than generating the per-protocol pattern for CONN.
    //
    // NOTE the limit of this test: LogType has only CONN today, so dlqChain's
    // non-CONN branch -- the per-protocol uid prefix -- is UNEXERCISED until Unit 3
    // adds DNS. Do not rename this to suggest it covers a second log type; it
    // cannot, and a test whose name overstates its reach is worse than none.
    @Test
    void dlqChainReproducesTheHistoricalConnUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", List.of(
            ArchiveJob.dlqChain(LogType.CONN, "netsec.conn.dlq.v1")),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));

        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });

        assertTrue(uids.containsAll(Set.of(
            "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            "dlqChain(CONN, ...) must reproduce all three historical conn uids, found: " + uids);
    }

    private static Set<String> operatorUids() {
        StreamExecutionEnvironment env = buildJob();
        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });
        return uids;
    }

    // The branch dlqChainReproducesTheHistoricalConnUids's own comment recorded
    // as untestable while LogType had only one constant: two log types (conn,
    // dns) times two chain kinds (feature vector, DLQ), built through
    // connAndDnsChains() -- the two-protocol chain list, whose first four
    // entries connDnsAndModbusChains (below, and the method ArchiveJob.main()
    // actually calls today) reuses byte-for-byte -- so this test pins the
    // exact list that method's own first four entries wire rather than a
    // hand-copied duplicate that could silently drift from it. This is what
    // proves the per-protocol prefix pattern actually produces twelve
    // non-colliding uids rather than only being exercised for DLQ alone.
    @Test
    void fourChainsAcrossTwoLogTypesProduceTwelveDistinctUidsAndConnStaysUnchanged() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092",
            ArchiveJob.connAndDnsChains("netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
                "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1"),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));

        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });

        // Four chains of three operators each. Using the SET's size (rather
        // than only containsAll below) is what actually proves uniqueness: if
        // any two of the twelve nodes shared a uid, this count would be 11 or
        // fewer, whereas containsAll alone would still pass.
        assertEquals(12, uids.size(), "four chains of three operators each, no collisions, found: " + uids);

        // Conn's six historical uids, unchanged -- the same six
        // everyOperatorCarriesAnExplicitUid and theConnDlqChainKeepsItsHistoricalUids
        // assert for the two-chain build above, now proven to survive sitting
        // alongside a second log type's chains too.
        assertTrue(uids.containsAll(Set.of(
            "feature-vector-source", "feature-vector-row", "feature-vectors-clickhouse-sink",
            "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            "conn's six historical uids must be present and unchanged, found: " + uids);

        // dns's six, following featureVectorChain's and dlqChain's shared
        // "<wirename>-" prefix pattern.
        assertTrue(uids.containsAll(Set.of(
            "dns-feature-vector-source", "dns-feature-vector-row", "dns-feature-vectors-clickhouse-sink",
            "dns-dlq-source", "dns-invalid-event-row", "dns-invalid-events-clickhouse-sink")),
            "dns's six uids must follow the shared prefix pattern, found: " + uids);
    }

    // A third protocol's six-chain list (connDnsAndModbusChains, alongside
    // connAndDnsChains rather than widening it -- see that method's own
    // signature comment) must produce eighteen non-colliding uids: conn's
    // six historical ones, dns's six under the shared prefix pattern, and
    // modbus's own six under the identical pattern. Using the SET's size, not
    // only containsAll, is what actually proves the eighteen are pairwise
    // distinct -- in Flink 2.2.1 a uid collision does NOT silently merge two
    // chains' checkpoint state: StreamGraphHasherV2 throws "Hash collision on
    // user-specified ID" while building the JobGraph, before a job ever
    // submits (both this test and its two-protocol sibling above would catch
    // it here, without needing a real cluster). The real risk a uid collision
    // does not protect against is RENAMING a uid between versions: the
    // renamed operator's savepoint state no longer maps to it, so a restore
    // fails, or, under allowNonRestoredState, that operator quietly starts
    // empty (see CLAUDE.md's own note on this).
    @Test
    void sixChainsProduceEighteenDistinctUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", ArchiveJob.connDnsAndModbusChains(
            "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
            "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1",
            "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1"),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));

        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });

        assertEquals(18, uids.size(), "eighteen non-colliding uids expected, found: " + uids);

        assertTrue(uids.containsAll(Set.of(
            "modbus-feature-vector-source", "modbus-feature-vector-row", "modbus-feature-vectors-clickhouse-sink",
            "modbus-dlq-source", "modbus-invalid-event-row", "modbus-invalid-events-clickhouse-sink")),
            "modbus's six uids must follow the shared prefix pattern, found: " + uids);
    }

    // A fourth protocol's eight-chain list (connDnsModbusAndS7commChains) must
    // produce twenty-four non-colliding uids: the eighteen of the six-chain
    // list, plus s7comm's six under the same prefix pattern.
    @Test
    void eightChainsProduceTwentyFourDistinctUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", ArchiveJob.connDnsModbusAndS7commChains(
            "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
            "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1",
            "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1",
            "netsec.s7comm.feature-vector.v1", "netsec.s7comm.dlq.v1"),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));

        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });

        assertEquals(24, uids.size(), "twenty-four non-colliding uids expected, found: " + uids);
        assertTrue(uids.containsAll(Set.of(
            "s7comm-feature-vector-source", "s7comm-feature-vector-row", "s7comm-feature-vectors-clickhouse-sink",
            "s7comm-dlq-source", "s7comm-invalid-event-row", "s7comm-invalid-events-clickhouse-sink")),
            "s7comm's six uids must follow the shared prefix pattern, found: " + uids);
    }
}
