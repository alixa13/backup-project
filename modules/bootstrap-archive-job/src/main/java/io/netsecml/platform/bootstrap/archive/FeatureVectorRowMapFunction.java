package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.FeatureVectorRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// Kafka bytes to a feature_vectors row.
//
// This lives in the bootstrap module rather than in either adapter because it is
// the only place allowed to know about both: adapter-kafka owns the JSON, and
// adapter-clickhouse owns the row. Neither imports the other.
public final class FeatureVectorRowMapFunction extends RichMapFunction<byte[], FeatureVectorRow> {

    // Only used for error messages from the Kafka Deserializer contract.
    private final String topic;

    // Built in open(), so nothing non-serializable travels through the job graph
    // and nothing is allocated per record.
    private transient FeatureVectorDeserializer deserializer;
    private transient FeatureVectorRowMapper mapper;

    public FeatureVectorRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new FeatureVectorDeserializer();
        mapper = new FeatureVectorRowMapper();
    }

    @Override
    public FeatureVectorRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
