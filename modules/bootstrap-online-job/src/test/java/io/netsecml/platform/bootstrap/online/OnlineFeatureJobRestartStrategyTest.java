package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.bootstrap.online.OnlineFeatureJob.ProtocolTopics;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.time.Duration;
import java.util.stream.StreamSupport;
import static org.junit.jupiter.api.Assertions.*;

// The restart strategy main() gives this job, loaded the way Flink's scheduler
// loads it (from the submitted job graph's configuration), and fed the failures
// a TaskManager loss can produce. No cluster is needed.
//
// Server test, 2026-09-25: one TaskManager restart failed the archive job
// outright, because each of its independent pipelines (pipelined regions)
// reported its own failure and Flink 2.2.1's failure-rate strategy counts every
// one (see ArchiveJobRestartStrategyTest). This job came through that restart,
// but only just: it had the same strategy -- 3 failures per 10 minutes allowed,
// the 4th fails the job -- and three regions (conn+dns, joined; modbus; s7comm),
// so one TaskManager loss spent the whole allowance and any failure in the next
// 10 minutes would have failed it.
class OnlineFeatureJobRestartStrategyTest {

    // deploy/lib/tune.sh's parallelism on a host with 16 cores or more, which
    // the server has.
    private static final int SERVER_PARALLELISM = 4;

    // The job graph main() submits: its four protocols, its restart strategy.
    private static JobGraph mainJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SERVER_PARALLELISM);
        env.configure(OnlineFeatureJob.restartStrategy());
        OnlineFeatureJob.build(env, "localhost:9092",
            new ProtocolTopics("conn", "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1"),
            new ProtocolTopics("dns", "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1"),
            new ProtocolTopics("netsec.modbus.raw.v1", "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1"),
            new ProtocolTopics("netsec.s7comm.raw.v1", "netsec.s7comm.feature-vector.v1", "netsec.s7comm.dlq.v1"),
            new SensorId("sensor-test"), Duration.ofMinutes(60));
        return env.getStreamGraph().getJobGraph();
    }

    // Flink's own strategy for a job configuration; checkpointing is on, as in
    // main(). With no restart strategy configured this would be Flink's default.
    private static RestartBackoffTimeStrategy strategyFor(Configuration jobConfiguration) {
        return RestartBackoffTimeStrategyFactoryLoader
            .createRestartBackoffTimeStrategyFactory(jobConfiguration, new Configuration(), true)
            .create();
    }

    private static long pipelinedRegions(JobGraph graph) {
        return StreamSupport.stream(
            DefaultLogicalTopology.fromJobGraph(graph).getAllPipelinedRegions().spliterator(), false).count();
    }

    // Two TaskManager losses, one restart apart, each failing every region at
    // once, must still leave the job a restart. The backoffs are shrunk to
    // 200 ms here, only so that "one restart apart" takes a fraction of a
    // second: each loss's region failures still arrive together, well inside it.
    @Test
    void twoTaskManagerLossesInARowDoNotExhaustTheRestartStrategy() throws InterruptedException {
        JobGraph graph = mainJobGraph();
        long regions = pipelinedRegions(graph);
        assertTrue(regions >= 3, "the job must have at least the old allowance of 3 regions to test this, has " + regions);
        Configuration config = graph.getJobConfiguration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofMillis(200));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMillis(200));
        RestartBackoffTimeStrategy strategy = strategyFor(config);

        for (int loss = 1; loss <= 2; loss++) {
            for (long i = 0; i < regions; i++) {
                strategy.notifyFailure(new IOException("TaskManager lost"));
            }
            assertTrue(strategy.canRestart(),
                "TaskManager loss " + loss + " (" + regions + " simultaneous region failures) exhausted " + strategy);
            // The restart happens; the next loss comes after it.
            Thread.sleep(300);
        }
    }

    // A job that fails again after every restart -- saved state that no longer
    // fits, or an outage that never ends -- must still be given up on in the
    // end, so it turns FAILED and the supervisor's check for resubmissions from
    // one restore point (deploy/flink/submit-jobs.sh) gets to run. The backoffs
    // are shrunk to 1 ms here, only so that ten separate failures take
    // milliseconds rather than a quarter of an hour.
    @Test
    void aJobThatKeepsFailingIsGivenUpAfterTenRestarts() throws InterruptedException {
        Configuration config = mainJobGraph().getJobConfiguration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofMillis(1));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMillis(1));
        RestartBackoffTimeStrategy strategy = strategyFor(config);

        int restarts = 0;
        while (restarts < 100) {
            // Past the backoff, so each failure is a new one, not merged.
            Thread.sleep(5);
            strategy.notifyFailure(new IOException("Kafka unreachable"));
            if (!strategy.canRestart()) {
                break;
            }
            restarts++;
        }

        assertEquals(10, restarts, "restarts granted before the job is failed, by " + strategy);
    }
}
