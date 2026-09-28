"""The release (spec sections 3 and 6): the ONNX graph with the delivered signature, the parity gate
G3, and a directory in the delivered layout (ruling A4) whose FROZEN_MANIFEST.json pins every file
by SHA-256. The Java loader of the scoring plan reads the five scoring files from it."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import onnxruntime as ort
import torch

from netsec_ml.s7comm.calibrate import GROUPS
from netsec_ml.s7comm.preprocessing import write_contract

ARTIFACTS = "artifacts/model"
ONNX_NAME = "s7comm_lstm_autoencoder.onnx"
SEQUENCE = 16


def export_onnx(model, path, sequence, width) -> None:
    """input [batch, sequence, width] -> reconstruction, batch dynamic: the delivered signature,
    through torch 2.5.1's TorchScript exporter (ruling A3)."""
    model.eval()
    torch.onnx.export(model, torch.zeros(1, sequence, width), str(path),
                      input_names=["input"], output_names=["reconstruction"],
                      dynamic_axes={"input": {0: "batch"}, "reconstruction": {0: "batch"}},
                      opset_version=17)


def onnx_parity(model, onnx_path, X, windows, batches=(1, 8, 32, 256)) -> list[dict]:
    """G3's evidence: max and mean |ONNX - PyTorch| over the reconstruction of the first `batch`
    windows, at each batch size."""
    if len(windows) < max(batches):
        raise ValueError(f"parity needs at least {max(batches)} windows, got {len(windows)}")
    session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    model.eval()
    out = []
    for b in batches:
        x = np.ascontiguousarray(X[windows[:b]], dtype=np.float32)
        with torch.no_grad():
            expected = model(torch.from_numpy(x)).numpy()
        diff = np.abs(session.run(None, {"input": x})[0] - expected)
        out.append({"batch": int(b), "max_abs_difference": float(diff.max()),
                    "mean_abs_difference": float(diff.mean())})
    return out


def sha256(path) -> str:
    """A file's SHA-256, 64 lowercase hex."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _json(path: Path, value) -> None:
    """Pretty JSON, UTF-8, parents created."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_release(root, release_id, *, model, model_config, pre, policy, score_weights, parity,
                  documents, summary) -> Path:
    """Write the release under <root>/models/stage1_anomaly/<release_id>/ and return it. An existing
    release is never overwritten: a new model is a new release_id."""
    rel = Path(root) / "models" / "stage1_anomaly" / release_id
    if rel.exists():
        raise FileExistsError(f"{rel} exists: a release is frozen, give the new one its own release_id")
    art = rel / ARTIFACTS
    art.mkdir(parents=True)
    names = [str(n) for n in pre.get_feature_names_out()]

    # The graph and the checkpoint, in the delivered checkpoint shape (reliability.load_model).
    export_onnx(model, art / ONNX_NAME, SEQUENCE, len(names))
    torch.save({"input_size": len(names), "state_dict": model.state_dict(),
                "model_config": {"num_layers": 1, "dropout": 0.0, **model_config}}, art / "lstm_autoencoder.pt")

    # The preprocessing, as the pickled pipeline and as the JSON contract Java reads.
    joblib.dump(pre, art / "preprocessor.joblib")
    contract = write_contract(pre, art / "preprocessor_contract.json")

    # Every group's calibration scores (float64, empty when none: the Java loader wants all four),
    # compressed like the delivered file, and the policy that reads them.
    np.savez_compressed(art / "causal_online_conformal_calibration_scores.npz",
                        **{g: np.asarray(policy.calibration.get(g, []), dtype=np.float64) for g in GROUPS})
    _json(art / "causal_online_shadow_policy.json", {
        "schema_version": "s7-causal-online-shadow-policy-v2",
        "status": "SHADOW_ONLY_NOT_PRODUCTION_VALIDATED",
        "score_semantics": {"sequence_length": SEQUENCE, "stride": 1, "scored_timestep": "LAST_ONLY",
                            "future_context": False, "minimum_events_before_score": SEQUENCE},
        "alpha_by_group": policy.alpha_by_group,
        "fallback_alpha": policy.fallback_alpha,
        "calibration_groups": [
            {"group": g, "calibration_n": int(len(policy.calibration.get(g, []))),
             "alpha": policy.alpha_by_group.get(g),
             "minimum_possible_pvalue": 1.0 / (len(policy.calibration[g]) + 1) if len(policy.calibration.get(g, [])) else None}
            for g in GROUPS],
    })
    _json(art / "shadow_deployment_manifest.json", {
        "status": "SHADOW_ONLY_NOT_PRODUCTION_VALIDATED",
        "onnx_path": f"{ARTIFACTS}/{ONNX_NAME}",
        "sequence_length": SEQUENCE,
        "input_dimension": contract["transformed_dimension"],
        "preprocessor_contract": f"{ARTIFACTS}/preprocessor_contract.json",
        "evaluation_mode": "CAUSAL_LAST_TIMESTEP",
        "conformal_calibration_scores": f"{ARTIFACTS}/causal_online_conformal_calibration_scores.npz",
        "shadow_policy": f"{ARTIFACTS}/causal_online_shadow_policy.json",
        "identity_features_excluded_from_event_score": ["s7_operation"],
        "identity_score_weight": 0.0,
        "score_weights": [float(w) for w in score_weights],
        "alpha_by_group": policy.alpha_by_group,
        "transformed_feature_order": names,
        "onnx_parity": parity,
        "onnx_max_abs_difference": max(p["max_abs_difference"] for p in parity),
    })

    # The run's own records (evaluation, selection, datasets), then the manifest over everything.
    for relative, value in documents.items():
        _json(rel / relative, value)
    files = sorted(p for p in rel.rglob("*") if p.is_file())
    _json(rel / "FROZEN_MANIFEST.json", {
        "schema_version": "s7-frozen-model-release-v2",
        "release_id": release_id,
        "release_status": "FROZEN_INTERNAL_SHADOW_MODEL",
        "production_validated": False,
        "model": {"algorithm": "LSTM_AUTOENCODER", "training_objective": "CAUSAL_LAST_TIMESTEP_RECONSTRUCTION",
                  "sequence_length": SEQUENCE, "raw_feature_count": 16,
                  "transformed_dimension": contract["transformed_dimension"]},
        **summary,
        "files": {p.relative_to(rel).as_posix(): {"sha256": sha256(p), "bytes": p.stat().st_size} for p in files},
    })
    return rel


def verify_release(release_dir) -> dict:
    """Every file FROZEN_MANIFEST.json lists exists with its recorded SHA-256; returns the manifest."""
    rel = Path(release_dir)
    manifest = json.loads((rel / "FROZEN_MANIFEST.json").read_text(encoding="utf-8"))
    for relative, pin in manifest["files"].items():
        path = rel / relative
        if not path.is_file():
            raise FileNotFoundError(f"{relative} is listed in FROZEN_MANIFEST.json but missing")
        if sha256(path) != pin["sha256"]:
            raise ValueError(f"{relative} does not match FROZEN_MANIFEST.json's SHA-256")
    return manifest
