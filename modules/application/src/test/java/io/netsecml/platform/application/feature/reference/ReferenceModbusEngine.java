package io.netsecml.platform.application.feature.reference;

import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.QualityFlags;

// The decide-segment / reset / advance / extract / flag sequence of
// ModbusBuildFeaturesUseCase.build as it stood at commit 1d0878e, driving the
// verbatim ReferenceModbusEntityState and ReferenceModbusFeatureExtractor
// beside it. Together the three are the parity oracle the production engine
// is tested against: same event stream in, the same 42 float values and the
// same quality flags out, bit for bit. Only the vector's values and flags are
// replayed -- id, schema and producedAt come from the event and the registry,
// never from the causal state, so they have nothing to diverge on.
public final class ReferenceModbusEngine {

    private final ReferenceModbusFeatureExtractor extractor = new ReferenceModbusFeatureExtractor();

    // One event's output and the state to feed the next event for this key.
    public record Step(float[] values, int qualityFlags, ReferenceModbusEntityState newState) {
    }

    public Step step(ModbusEvent event, ReferenceModbusEntityState currentState) {
        // Steps 1-6 of ModbusBuildFeaturesUseCase.build at 1d0878e, in order.
        double ts = event.tsSeconds();
        boolean newSegment = currentState.startsNewSegment(ts);
        Double lastTs = currentState.lastTs();
        boolean outOfOrder = lastTs != null && ts - lastTs < 0.0;
        ReferenceModbusEntityState before = newSegment ? currentState.resetForNewSegment() : currentState;
        ReferenceModbusEntityState after = before.afterEvent(ts, event.functionCode(), event.transactionId(),
            event.direction(), event.address(), event.quantity());
        float[] values = extractor.extract(event, before, after, newSegment);
        int qualityFlags = outOfOrder ? QualityFlags.MODBUS_OUT_OF_ORDER : QualityFlags.NONE;
        return new Step(values, qualityFlags, after);
    }
}
