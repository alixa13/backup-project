package io.netsecml.platform.port.out;

import java.io.Serializable;

// Creates one ModelScorer per Flink subtask, from inside that subtask's own
// open() -- never built once centrally and shared, since a real ModelScorer
// (an ONNX Runtime session) is not guaranteed safe to call from more than one
// subtask at a time. Serializable so the factory itself (bundle path, model
// config) can travel inside a captured operator field to every TaskManager,
// the same reason ModelRef and SensorId are Serializable.
public interface ModelScorerFactory extends Serializable {
    ModelScorer create();
}
