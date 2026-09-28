package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.S7commDetectorPredictionRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.S7commDetectorPredictionRow;
import io.netsecml.platform.adapter.kafka.sink.S7commDetectorPredictionDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// One prediction message from netsec.s7comm.prediction.v1 as one
// s7comm_detector_predictions row, as ModbusDetectorPredictionRowMapFunction does.
public final class S7commDetectorPredictionRowMapFunction
        extends RichMapFunction<byte[], S7commDetectorPredictionRow> {

    private final String topic;
    private transient S7commDetectorPredictionDeserializer deserializer;
    private transient S7commDetectorPredictionRowMapper mapper;

    public S7commDetectorPredictionRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new S7commDetectorPredictionDeserializer();
        mapper = new S7commDetectorPredictionRowMapper();
    }

    @Override
    public S7commDetectorPredictionRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
