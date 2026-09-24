package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import org.apache.flink.api.java.functions.KeySelector;

// Unlike SourceKeySelector, which is shared across conn and dns and keys off
// NetworkEvent, this selector is modbus's own: the modbus chain is a
// separate pipeline end to end (own topics, own DLQ, own keyed state -- see
// SourceKeySelector's and DnsFeatureProcessFunction's own ModbusEvent arms),
// so it is typed directly on ModbusEvent rather than narrowing NetworkEvent
// the way SourceKeySelector's switch does.
//
// The client/server orientation normalization itself lives in
// ModbusEntityKey.of, not here -- this selector only calls it, so the
// normalization formula exists in exactly one place.
public final class ModbusEntityKeySelector implements KeySelector<ModbusEvent, ModbusEntityKey> {
    @Override
    public ModbusEntityKey getKey(ModbusEvent event) {
        return ModbusEntityKey.of(event);
    }
}
