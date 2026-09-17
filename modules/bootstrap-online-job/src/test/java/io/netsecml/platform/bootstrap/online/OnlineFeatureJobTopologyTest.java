package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// The two-protocol topology's structure, asserted without Docker or Kafka --
// mirrors ArchiveJobTopologyTest's own env.getStreamGraph() inspection, for the
// same reason: this is the only way to cover operator uids (checkpoint state
// identity) without paying for the containers this module's own E2E test needs.
class OnlineFeatureJobTopologyTest {

    private static StreamExecutionEnvironment buildJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OnlineFeatureJob.build(env, "localhost:9092",
            new OnlineFeatureJob.ProtocolTopics("conn", "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("dns", "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1"),
            new SensorId("sensor-eu-1"));
        return env;
    }

    // Task 12 binding ruling 12b's table, verbatim. Conn's five are marked
    // separately below because they must be BYTE-IDENTICAL to what shipped
    // before this task -- a changed uid here silently discards that operator's
    // checkpoint state on restore.
    private static final Set<String> CONN_UIDS = Set.of(
        "conn-raw-source", "parse-map-validate", "conn-feature-extraction",
        "feature-vector-sink", "dlq-sink");

    private static final Set<String> DNS_UIDS = Set.of(
        "dns-raw-source", "dns-parse-map-validate", "conn-snapshot-extract",
        "dns-conn-enrichment", "dns-feature-extraction", "dns-feature-vector-sink", "dns-dlq-sink");

    // conn's five plus dns's seven (dns-parse, the join, dns-feature-extraction,
    // dns's two sinks, PLUS conn-snapshot-extract -- the extra branch dns's join
    // needs off the conn chain) is twelve, matching ruling 12b's table exactly.
    private static Set<String> allTwelveUids() {
        Set<String> all = new HashSet<>(CONN_UIDS);
        all.addAll(DNS_UIDS);
        return all;
    }

    // All twelve of ruling 12b's uids must exist somewhere in the graph.
    @Test
    void allTwelveOperatorUidsArePresent() {
        Set<String> uids = operatorUids();
        assertTrue(uids.containsAll(allTwelveUids()),
            "expected all twelve uids from ruling 12b, found: " + uids);
    }

    // Conn's five predate this task and are checkpoint state identity for a
    // pipeline that (per CLAUDE.md's implementation-state notes) has never
    // actually restored a checkpoint in production -- but the online job's own
    // uid comment is explicit that the cost of an uid change is paid in full
    // regardless of whether a checkpoint has been exercised yet. This is the
    // dedicated check ArchiveJobTopologyTest's own
    // theConnDlqChainKeepsItsHistoricalUids test makes for its job; this is
    // this job's copy of that same guarantee.
    @Test
    void connsFiveHistoricalUidsAreByteIdentical() {
        Set<String> uids = operatorUids();
        assertTrue(uids.containsAll(CONN_UIDS),
            "conn's five historical uids must survive unchanged, found: " + uids);
    }

    // Every operator and sink-writer uid across the WHOLE topology must be
    // pairwise distinct -- two operators sharing one is how Flink collides
    // checkpoint state across them. Set.add returning false is the signal; this
    // walks every StreamNode (not just the twelve this job assigns explicitly)
    // so it also catches a uid this job assigns colliding with one Flink
    // generates for a two-phase-commit sink's committer (see the comment on
    // noStreamNodeLacksAUid below for what that generated uid looks like).
    @Test
    void everyOperatorUidIsUniqueAcrossTheTopology() {
        StreamExecutionEnvironment env = buildJob();
        Set<String> seen = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            String uid = node.getTransformationUID();
            if (uid != null) {
                assertTrue(seen.add(uid), "duplicate operator uid across the topology: " + uid);
            }
        });
    }

    // KafkaSink is a TwoPhaseCommittingStatefulSink (confirmed by inspecting the
    // class, not assumed), so sinkTo() was a candidate to expand into a Writer
    // node AND a separate Committer node the way ClickHouseBatchSink in the
    // archive job never does -- ArchiveJobTopologyTest's own comment records
    // verifying that ITS sink does NOT split; this is this job's verification
    // that ITS sink DOES.
    //
    // VERIFIED, not assumed: printing every node's id/operator-name/uid for
    // this exact topology (env.getStreamGraph(false).getStreamNodes(), sensor
    // "sensor-diagnostic", both protocols wired) showed sixteen StreamNodes,
    // not twelve -- each of the four sinkTo() calls (feature-vector-sink,
    // dlq-sink, dns-feature-vector-sink, dns-dlq-sink) produced TWO nodes:
    //   - a "<uid>: Writer" node whose getTransformationUID() is EXACTLY the
    //     uid this class passed to .uid(...), e.g. "feature-vector-sink"; and
    //   - a "<uid>: Committer" node whose getTransformationUID() is Flink's own
    //     "Sink Committer: <uid>" string -- DERIVED from, not equal to, the uid
    //     this class assigned, exactly as ruling 12b warned it might be.
    // Both nodes carried a non-null uid in every case observed -- Flink derives
    // the committer's uid automatically rather than leaving it null -- so the
    // assertion below holds for this topology as built today. If a future sink
    // here were ever NOT a TwoPhaseCommittingSink (or Flink's committer-naming
    // scheme changed to leave it null), this test is what would catch a node
    // silently missing a uid; it must not be weakened to vacuity or deleted.
    @Test
    void noStreamNodeLacksAUid() {
        StreamExecutionEnvironment env = buildJob();
        for (StreamNode node : env.getStreamGraph(false).getStreamNodes()) {
            assertNotNull(node.getTransformationUID(),
                "stream node " + node.getId() + " (" + node.getOperatorName() + ") has no uid -- "
                + "every operator in this job must carry an explicit .uid()");
        }
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
