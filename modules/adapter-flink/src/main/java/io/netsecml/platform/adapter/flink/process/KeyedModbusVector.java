package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;

// A Modbus feature vector with the stream key it was built under: what
// modbus-features hands modbus-score (spec section 4). The vector itself
// carries no client, server or unit.
public record KeyedModbusVector(ModbusEntityKey key, FeatureVector vector) {
}
