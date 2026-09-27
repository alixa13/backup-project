package io.netsecml.platform.port.out;

import java.io.Serializable;

// Creates one SequenceScorer per Flink subtask, inside that subtask's open():
// an ONNX Runtime session is not shared across subtasks. Serializable so the
// factory (a bundle path) travels with the operator to every TaskManager.
public interface SequenceScorerFactory extends Serializable {
    SequenceScorer create();
}
