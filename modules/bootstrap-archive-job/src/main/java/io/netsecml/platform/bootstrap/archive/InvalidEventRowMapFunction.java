package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.InvalidEventRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.adapter.kafka.sink.RejectedEventDeserializer;
import io.netsecml.platform.domain.event.LogType;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// Kafka bytes to an invalid_events row, via the neutral domain RejectedEvent.
public final class InvalidEventRowMapFunction extends RichMapFunction<byte[], InvalidEventRow> {

    // Only used for error messages from the Kafka Deserializer contract.
    private final String topic;

    // The log type bound to this function's topic at wiring time. An enum is
    // Serializable, so it travels with the function to the TaskManagers; the
    // mapper it configures is still built in open().
    private final LogType logType;

    // Built in open(), so nothing non-serializable travels through the job graph
    // and nothing is allocated per record.
    private transient RejectedEventDeserializer deserializer;
    private transient InvalidEventRowMapper mapper;

    // The topic name and the log type it carries cross the constructor; the
    // deserializer and mapper are built later, in open().
    public InvalidEventRowMapFunction(String topic, LogType logType) {
        if (logType == null) {
            throw new IllegalArgumentException("logType must not be null");
        }
        this.topic = topic;
        this.logType = logType;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new RejectedEventDeserializer();
        mapper = new InvalidEventRowMapper(logType);
    }

    // KNOWN LIMITATION: same as FeatureVectorRowMapFunction.map -- an
    // undeserializable message here throws IllegalArgumentException with no DLQ or
    // side output, and one poison message on the DLQ topic can permanently stall
    // this job under the failure-rate restart strategy. Only the online job writes
    // to dlqTopic, so this means a serializer bug or bad deploy rather than
    // untrusted input, but that lowers the likelihood without removing it. See
    // docs/clickhouse.md's "Poison records" section for the follow-up fix.
    @Override
    public InvalidEventRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
