package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.InvalidEventRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.adapter.kafka.sink.RejectedEventDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// Kafka bytes to an invalid_events row, via the neutral domain RejectedEvent.
public final class InvalidEventRowMapFunction extends RichMapFunction<byte[], InvalidEventRow> {

    // Only used for error messages from the Kafka Deserializer contract.
    private final String topic;

    // Built in open(), so nothing non-serializable travels through the job graph
    // and nothing is allocated per record.
    private transient RejectedEventDeserializer deserializer;
    private transient InvalidEventRowMapper mapper;

    public InvalidEventRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new RejectedEventDeserializer();
        mapper = new InvalidEventRowMapper();
    }

    @Override
    public InvalidEventRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
