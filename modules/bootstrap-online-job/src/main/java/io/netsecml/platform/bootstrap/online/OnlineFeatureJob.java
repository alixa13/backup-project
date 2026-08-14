package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.flink.process.ConnFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.ParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.RejectedRecord;
import io.netsecml.platform.adapter.flink.process.SourceKeySelector;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class OnlineFeatureJob {

    private static final DeserializationSchema<byte[]> RAW_BYTES = new DeserializationSchema<>() {
        @Override
        public byte[] deserialize(byte[] message) {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(byte[].class);
        }
    };

    public static void build(StreamExecutionEnvironment env, String bootstrapServers, String inputTopic,
                              String featureVectorTopic, String dlqTopic, SensorId sensor) {
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("conn-online-job")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(RAW_BYTES)
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
                    new RejectedRecordPayload(r.rawPayload(), r.reason().name(), r.detail())))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        rejected.sinkTo(dlqSink).name("dlq-sink");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("CONN_INPUT_TOPIC", "conn");
        String featureTopic = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1");
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1");
        String sensorId = System.getenv().getOrDefault("SENSOR_ID", "sensor-default");

        build(env, bootstrapServers, inputTopic, featureTopic, dlqTopic, new SensorId(sensorId));
        env.execute("conn-online-feature-job");
    }
}
