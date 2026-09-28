package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.ModbusDetectorPredictionRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.ModbusDetectorPredictionRow;
import io.netsecml.platform.adapter.kafka.sink.ModbusDetectorPredictionDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// One prediction message from netsec.modbus.prediction.v1 as one
// modbus_detector_predictions row, as FeatureVectorRowMapFunction does for vectors.
public final class ModbusDetectorPredictionRowMapFunction
        extends RichMapFunction<byte[], ModbusDetectorPredictionRow> {

    private final String topic;
    private transient ModbusDetectorPredictionDeserializer deserializer;
    private transient ModbusDetectorPredictionRowMapper mapper;

    public ModbusDetectorPredictionRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new ModbusDetectorPredictionDeserializer();
        mapper = new ModbusDetectorPredictionRowMapper();
    }

    @Override
    public ModbusDetectorPredictionRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
