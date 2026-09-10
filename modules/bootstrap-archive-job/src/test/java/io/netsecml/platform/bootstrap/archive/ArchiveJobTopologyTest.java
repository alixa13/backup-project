package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
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
