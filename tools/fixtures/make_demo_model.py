"""Generates the demo scoring bundle used by the Java tests.

Hand-built rather than trained: the model is sigmoid(x.W + b) with fixed
weights, so the expected score for any vector is computable in closed form.
That is what lets the Java golden-vector test detect a runtime that computes
something subtly different, which a trained model could not.
"""
import hashlib
import json
import pathlib
import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

OUT = pathlib.Path("tests/fixtures/models/conn-demo-v1")
CONTRACT_PATH = pathlib.Path("contracts/features/conn-feature-schema-v1.json")

# Read the schema id, feature count and feature order from the committed
# contract rather than retyping them: a contract change (e.g. a new frozen
# schema version with a different width) then desyncs the graph from the
# contract loudly, via the assertions below, instead of silently producing a
# fixture whose weight matrix no longer matches featureOrder.
contract = json.loads(CONTRACT_PATH.read_text())
SCHEMA_ID = contract["id"]
FEATURES = contract["featureCount"]
feature_order = [f["name"] for f in contract["features"]]
assert len(feature_order) == FEATURES, (
    f"contract lists {len(feature_order)} feature names but featureCount={FEATURES}"
)

# Deterministic, spread across positive and negative so no vector saturates.
W = np.array([[0.1 * (i - 10) / 10] for i in range(FEATURES)], dtype=np.float32)
B = np.array([-0.5], dtype=np.float32)
assert W.shape[0] == FEATURES, f"weight matrix has {W.shape[0]} rows, contract says {FEATURES}"

graph = helper.make_graph(
    [
        helper.make_node("MatMul", ["features", "W"], ["logit_raw"]),
        helper.make_node("Add", ["logit_raw", "B"], ["logit"]),
        helper.make_node("Sigmoid", ["logit"], ["probability"]),
    ],
    "conn_demo_logreg",
    [helper.make_tensor_value_info("features", TensorProto.FLOAT, [1, FEATURES])],
    [helper.make_tensor_value_info("probability", TensorProto.FLOAT, [1, 1])],
    [numpy_helper.from_array(W, name="W"), numpy_helper.from_array(B, name="B")],
)
model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
# The installed onnx package defaults new models to its own current IR version
# (14 here), but the pinned ONNX Runtime Java (1.20.0) only loads up to IR
# version 10. Opset 13 is valid under IR version 10 (which covers opsets up
# to 21), so pin the IR version down explicitly rather than upgrading the
# runtime or downgrading the onnx package.
# If you upgrade onnxruntime.version later, re-check ITS reported ceiling
# (the OrtException message names it) before assuming 10 still holds — do
# not just carry this literal forward.
model.ir_version = 10
onnx.checker.check_model(model)
payload = model.SerializeToString()

OUT.mkdir(parents=True, exist_ok=True)
(OUT / "model.onnx").write_bytes(payload)

def score(x):
    return float(1.0 / (1.0 + np.exp(-(np.array(x, dtype=np.float32) @ W + B)[0])))

samples = [
    [0.0] * FEATURES,
    [1.0] * FEATURES,
    [float(i) for i in range(FEATURES)],
    # Brackets the 0.5 decision threshold: only the highest-weight feature
    # (index 19, W[19]=0.09, the largest-magnitude positive weight) is
    # nonzero, set to 5.5 — the smallest single-feature nudge that lands the
    # logit close to 0. Score ~0.4988, inside [0.45, 0.55], unlike the three
    # samples above which sit at 0.3775, 0.3543 and 0.9945 and never test the
    # scorer's behavior near its threshold.
    [0.0] * (FEATURES - 1) + [5.5],
]

# The schema hash must equal what ConnFeatureSchemaV1 carries. Read that class's
# test first and compute it the same way over the committed contract file.
schema_hash = hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()

bundle = {
    "name": "conn-demo",
    "version": "v1",
    "schemaId": SCHEMA_ID,
    "schemaHash": schema_hash,
    "featureOrder": feature_order,
    "classes": ["normal", "attack"],
    "threshold": 0.5,
    "modelSha": hashlib.sha256(payload).hexdigest(),
    "outputName": "probability",
    # The COLUMN of the "probability" output tensor carrying P(positive
    # class) — NOT an index into `classes` above. 0 here because this graph's
    # output has a single column; a scikit-learn model exported with
    # zipmap=False would instead use 1 (column 1 of a two-column
    # [P(normal), P(attack)] output). Do not read this as pointing at
    # classes[0] ("normal") — the model's high scores mean attack.
    "positiveClassColumn": 0,
    "metrics": {},
    "trainedAt": "2026-09-19T00:00:00Z",
    "provenance": "hand-built fixture, not trained on any data",
    "sampleVectors": [{"values": s, "expectedScore": score(s)} for s in samples],
}
(OUT / "bundle.json").write_text(json.dumps(bundle, indent=2) + "\n")
print("wrote", OUT, "modelSha", bundle["modelSha"])
