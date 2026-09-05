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

    // Only the topic name crosses the constructor; the deserializer and mapper
    // are built later, in open().
    public FeatureVectorRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new FeatureVectorDeserializer();
        mapper = new FeatureVectorRowMapper();
    }

    // KNOWN LIMITATION: an undeserializable message makes deserializer.deserialize
    // throw IllegalArgumentException, which escapes this map with no DLQ or side
    // output to catch it. Combined with the archive job's failure-rate restart
    // strategy (3 failures / 10 min), one poison message on this internal topic
    // retries until the rate limit trips and then permanently stalls the job --
    // the offset in front of it never advances. The exposure is narrow: only the
    // online job ever writes to featureVectorTopic, so a malformed message here
    // means a serializer bug or a bad deploy, not untrusted external input -- but
    // that is lower likelihood, not zero. A side output mirroring the online
    // job's DLQ path is the follow-up fix; it is out of scope for this branch.
    // See docs/clickhouse.md's "Poison records" section.
    @Override
    public FeatureVectorRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
