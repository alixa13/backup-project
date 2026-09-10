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

    // The parameterised overload Spec §6.3 requires: the set of log types is a
    // list, so a third log type is a third list entry, wired identically to the
    // two the 5-argument overload builds. This is what proves adding a log type
    // is one entry, not two new String parameters on build() itself.
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
}
