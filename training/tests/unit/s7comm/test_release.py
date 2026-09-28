"""The ONNX graph's signature and parity, and a release every consumer can check file by file."""
import json

import numpy as np
import onnxruntime as ort
import pandas as pd
import pytest
import torch
from numpy.lib.stride_tricks import sliding_window_view

from netsec_ml.s7comm import calibrate as C
from netsec_ml.s7comm import preprocessing as P
from netsec_ml.s7comm import release as R
from netsec_ml.s7comm.data import CONTINUOUS, FEATURES
from netsec_ml.s7comm.model import LSTMAutoencoder, column_weights


def small():
    """A fitted preprocessor, an untrained small model, its windows and a policy: enough to release."""
    rng = np.random.default_rng(0)
    frame = pd.DataFrame({name: rng.uniform(0, 1, 300) for name in CONTINUOUS})
    frame["is_request_direction"] = np.arange(300) % 2
    frame["s7_function_changed"] = 0.0
    frame["s7_rosctr"] = np.where(np.arange(300) % 2 == 1, "1", "3")
    frame["s7_operation"] = "READ_VAR"
    pre = P.fit(frame[list(FEATURES)])
    X = P.transform(pre, frame)
    torch.manual_seed(0)
    model = LSTMAutoencoder(X.shape[1], 8, 4)
    windows = sliding_window_view(np.arange(len(X)), 16).astype(np.int64)
    policy = C.calibrate(np.linspace(0, 1, 50), np.array(["RESPONSE"] * 25 + ["READ_REQUEST"] * 25),
                         {"RESPONSE": 0.001, "READ_REQUEST": 0.001}, 0.001, 10)
    return pre, X, model, windows, policy


def release(tmp_path):
    """A release of small() under tmp_path, as the pipeline writes one."""
    pre, X, model, windows, policy = small()
    onnx_path = tmp_path / "model.onnx"
    R.export_onnx(model, onnx_path, 16, X.shape[1])
    names = list(pre.get_feature_names_out())
    return R.write_release(
        tmp_path / "out", "test_r1", model=model, model_config={"hidden_size": 8, "latent_size": 4},
        pre=pre, policy=policy, score_weights=column_weights(names, ["s7_operation"], 0.0),
        parity=R.onnx_parity(model, onnx_path, X, windows, batches=(1, 8, 32)),
        documents={"outputs/evaluation_summary.json": {"gates": {"G1": True}}},
        summary={"release_note": "test"})


def test_the_onnx_graph_has_the_delivered_signature(tmp_path):
    _, X, model, _, _ = small()
    R.export_onnx(model, tmp_path / "m.onnx", 16, X.shape[1])
    session = ort.InferenceSession(str(tmp_path / "m.onnx"), providers=["CPUExecutionProvider"])
    (inp,), (out,) = session.get_inputs(), session.get_outputs()
    assert (inp.name, inp.shape[1:], inp.type) == ("input", [16, X.shape[1]], "tensor(float)")
    assert isinstance(inp.shape[0], str)  # the batch axis is dynamic
    assert out.name == "reconstruction"


def test_onnx_matches_pytorch_within_the_gate(tmp_path):
    _, X, model, windows, _ = small()
    R.export_onnx(model, tmp_path / "m.onnx", 16, X.shape[1])
    parity = R.onnx_parity(model, tmp_path / "m.onnx", X, windows, batches=(1, 8, 32))
    assert [p["batch"] for p in parity] == [1, 8, 32]
    assert max(p["max_abs_difference"] for p in parity) <= 1e-4


def test_parity_needs_enough_windows(tmp_path):
    _, X, model, windows, _ = small()
    R.export_onnx(model, tmp_path / "m.onnx", 16, X.shape[1])
    with pytest.raises(ValueError, match="256"):
        R.onnx_parity(model, tmp_path / "m.onnx", X, windows[:100], batches=(1, 256))


def test_a_release_pins_every_file_and_verifies(tmp_path):
    rel = release(tmp_path)
    assert rel == tmp_path / "out" / "models" / "stage1_anomaly" / "test_r1"
    manifest = R.verify_release(rel)
    art = R.ARTIFACTS
    assert set(manifest["files"]) == {
        f"{art}/{R.ONNX_NAME}", f"{art}/lstm_autoencoder.pt", f"{art}/preprocessor.joblib",
        f"{art}/preprocessor_contract.json", f"{art}/causal_online_conformal_calibration_scores.npz",
        f"{art}/causal_online_shadow_policy.json", f"{art}/shadow_deployment_manifest.json",
        "outputs/evaluation_summary.json"}
    assert manifest["release_note"] == "test"


def test_the_calibration_and_policy_read_as_the_java_loader_expects(tmp_path):
    rel = release(tmp_path) / R.ARTIFACTS
    with np.load(rel / "causal_online_conformal_calibration_scores.npz") as npz:
        assert sorted(npz.files) == sorted(C.GROUPS)
        assert all(npz[g].dtype == np.float64 for g in C.GROUPS)
        assert len(npz["RESPONSE"]) == 25 and len(npz["OTHER_REQUEST"]) == 0
    policy = json.loads((rel / "causal_online_shadow_policy.json").read_text())
    assert policy["score_semantics"] == {"sequence_length": 16, "stride": 1, "scored_timestep": "LAST_ONLY",
                                         "future_context": False, "minimum_events_before_score": 16}
    assert policy["alpha_by_group"] == {"RESPONSE": 0.001, "READ_REQUEST": 0.001}
    assert policy["fallback_alpha"] == 0.001
    contract = json.loads((rel / "preprocessor_contract.json").read_text())
    shadow = json.loads((rel / "shadow_deployment_manifest.json").read_text())
    assert shadow["input_dimension"] == contract["transformed_dimension"]
    assert shadow["onnx_path"] == f"{R.ARTIFACTS}/{R.ONNX_NAME}"


def test_the_checkpoint_loads_into_the_delivered_architecture(tmp_path):
    ckpt = torch.load(release(tmp_path) / R.ARTIFACTS / "lstm_autoencoder.pt", weights_only=True)
    cfg = ckpt["model_config"]
    model = LSTMAutoencoder(ckpt["input_size"], cfg["hidden_size"], cfg["latent_size"])
    model.load_state_dict(ckpt["state_dict"])


def test_a_changed_file_fails_verification_naming_it(tmp_path):
    rel = release(tmp_path)
    with open(rel / R.ARTIFACTS / "causal_online_shadow_policy.json", "a", encoding="utf-8") as f:
        f.write(" ")
    with pytest.raises(ValueError, match="causal_online_shadow_policy.json"):
        R.verify_release(rel)


def test_a_release_is_never_overwritten(tmp_path):
    release(tmp_path)
    with pytest.raises(FileExistsError):
        release(tmp_path)
