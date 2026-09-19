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
FEATURES = 20

# Deterministic, spread across positive and negative so no vector saturates.
W = np.array([[0.1 * (i - 10) / 10] for i in range(FEATURES)], dtype=np.float32)
B = np.array([-0.5], dtype=np.float32)

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
]

# The schema hash must equal what ConnFeatureSchemaV1 carries. Read that class's
# test first and compute it the same way over the committed contract file.
schema_hash = hashlib.sha256(
    pathlib.Path("contracts/features/conn-feature-schema-v1.json").read_bytes()
).hexdigest()
feature_order = [f["name"] for f in json.loads(
    pathlib.Path("contracts/features/conn-feature-schema-v1.json").read_text())["features"]]

bundle = {
    "name": "conn-demo",
    "version": "v1",
    "schemaId": "conn-feature-v1",
    "schemaHash": schema_hash,
    "featureOrder": feature_order,
    "classes": ["normal", "attack"],
    "threshold": 0.5,
    "modelSha": hashlib.sha256(payload).hexdigest(),
    "outputName": "probability",
    "positiveClassIndex": 0,
    "metrics": {},
    "trainedAt": "2026-09-19T00:00:00Z",
    "provenance": "hand-built fixture, not trained on any data",
    "sampleVectors": [{"values": s, "expectedScore": score(s)} for s in samples],
}
(OUT / "bundle.json").write_text(json.dumps(bundle, indent=2) + "\n")
print("wrote", OUT, "modelSha", bundle["modelSha"])
