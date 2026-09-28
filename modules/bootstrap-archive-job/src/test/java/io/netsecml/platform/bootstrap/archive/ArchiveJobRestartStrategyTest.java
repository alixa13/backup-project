package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
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
// a TaskManager loss really produces. No cluster is needed.
//
// Server test, 2026-09-25: one 'docker restart' of the TaskManager failed this
// job outright -- "Recovery is suppressed by FailureRateRestartBackoffTimeStrategy
// (... maxFailuresPerInterval=3)" -- and only the supervisor's resubmission a
// minute later brought it back. A dying TaskManager takes all its tasks down at
// once, and every independent pipeline (pipelined region) among them reports its
// own failure. This job's chains share nothing, so each chain is its own region:
// 8 of them, at the server's parallelism as at any other. Flink 2.2.1's
// failure-rate strategy counts every one (its notifyFailure merges nothing) and
// allowed 3 per 10 minutes, so the 4th failed the job.
class ArchiveJobRestartStrategyTest {

    // deploy/lib/tune.sh's parallelism on a host with 16 cores or more, which
    // the server has.
    private static final int SERVER_PARALLELISM = 4;

    // The job graph main() submits: its eight chains, its restart strategy.
    private static JobGraph mainJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(SERVER_PARALLELISM);
        env.configure(ArchiveJob.restartStrategy());
        ArchiveJob.build(env, "localhost:9092", ArchiveJob.connDnsModbusAndS7commChains(
            "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
            "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1",
            "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1",
            "netsec.s7comm.feature-vector.v1", "netsec.s7comm.dlq.v1"),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));
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

    // One failure from every region at once -- a TaskManager dying under the
    // whole job -- must still leave the job a restart.
    @Test
    void oneTaskManagerLossDoesNotExhaustTheRestartStrategy() {
        JobGraph graph = mainJobGraph();
        long regions = pipelinedRegions(graph);
        assertTrue(regions > 3, "the job must have more regions than the 3 failures the old strategy allowed, has " + regions);
        RestartBackoffTimeStrategy strategy = strategyFor(graph.getJobConfiguration());

        for (long i = 0; i < regions; i++) {
            strategy.notifyFailure(new IOException("TaskManager lost"));
        }

        assertTrue(strategy.canRestart(),
            "one TaskManager loss (" + regions + " simultaneous region failures) exhausted " + strategy);
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
            strategy.notifyFailure(new IOException("ClickHouse unreachable"));
            if (!strategy.canRestart()) {
                break;
            }
            restarts++;
        }

        assertEquals(10, restarts, "restarts granted before the job is failed, by " + strategy);
    }
}
