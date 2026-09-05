package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.flink.process.ConnFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.ParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.RejectedRecord;
import io.netsecml.platform.adapter.flink.process.SourceKeySelector;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

public final class OnlineFeatureJob {

    public static void build(StreamExecutionEnvironment env, String bootstrapServers, String inputTopic,
                              String featureVectorTopic, String dlqTopic, SensorId sensor) {
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("conn-online-job")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new RawBytesDeserializationSchema())
            .build();

        DataStream<byte[]> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "conn-raw-source");

        SingleOutputStreamOperator<NetworkEvent> parsed = rawStream
            .process(new ParseMapValidateFunction(sensor))
            .name("parse-map-validate");

        DataStream<FeatureVector> featureVectors = parsed
            .keyBy(new SourceKeySelector())
            .process(new ConnFeatureProcessFunction())
            .name("conn-feature-extraction");

        FeatureVectorSerializer featureSerializer = new FeatureVectorSerializer();
        RejectedRecordSerializer rejectedSerializer = new RejectedRecordSerializer();

        KafkaSink<FeatureVector> featureSink = KafkaSink.<FeatureVector>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<FeatureVector>builder()
                .setTopic(featureVectorTopic)
                .setValueSerializationSchema(vector -> featureSerializer.serialize(featureVectorTopic, vector))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        featureVectors.sinkTo(featureSink).name("feature-vector-sink");

        DataStream<RejectedRecord> rejected = parsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        KafkaSink<RejectedRecord> dlqSink = KafkaSink.<RejectedRecord>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedRecord>builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(r -> rejectedSerializer.serialize(dlqTopic,
                    // stage comes from the domain's ReasonCode, so the archive
                    // adapter never has to re-derive it from the reason name.
                    new RejectedRecordPayload(r.rawPayload(), r.eventId(), r.reason().stage().name(),
                        r.reason().name(), r.detail(), r.receivedAt())))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        rejected.sinkTo(dlqSink).name("dlq-sink");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink 2.x removed StreamExecutionEnvironment.setRestartStrategy, so the restart
        // strategy is configuration-only now. Applied before enableCheckpointing below so
        // the explicit checkpoint settings are the last word regardless of what configure()
        // reads out of this Configuration. Without this the default with checkpointing on
        // would restart forever; failure-rate stops hot-looping a broken deployment.
        Configuration restartConfig = new Configuration();
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY,
            RestartStrategyOptions.RestartStrategyType.FAILURE_RATE.getMainValue());
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3);
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(10));
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(10));
        env.configure(restartConfig);

        // This job is stateful -- the keyed rolling buckets in ConnFeatureProcessFunction
        // only survive a failure if there are checkpoints to restore them from. With
        // checkpointing off the default strategy is no-restart, so any transient error
        // kills the job outright and loses that state; with it on, an in-run failure
        // restores state and source offsets together. The at-least-once Kafka sinks also
        // flush on the checkpoint barrier, so their durability is tied to this too.
        // Values are FINAL_ARCHITECTURE.md's initial settings -- benchmark, do not canonize.
        env.enableCheckpointing(30_000L);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(10_000L);
        checkpoints.setCheckpointTimeout(120_000L);
        checkpoints.setMaxConcurrentCheckpoints(1);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("CONN_INPUT_TOPIC", "conn");
        String featureTopic = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1");
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1");
        String sensorId = System.getenv().getOrDefault("SENSOR_ID", "sensor-default");

        build(env, bootstrapServers, inputTopic, featureTopic, dlqTopic, new SensorId(sensorId));
        env.execute("conn-online-feature-job");
    }
}
