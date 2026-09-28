package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.ModbusEntityKey;
import org.apache.flink.api.java.functions.KeySelector;

// modbus-score is keyed by the same stream key as modbus-features.
public final class KeyedModbusVectorKeySelector implements KeySelector<KeyedModbusVector, ModbusEntityKey> {
    @Override
    public ModbusEntityKey getKey(KeyedModbusVector value) {
        return value.key();
    }
}
