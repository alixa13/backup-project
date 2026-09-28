"""The whole pipeline on synthetic exports: a release when the gates hold, none when G1 fails."""
import json

import yaml

from netsec_ml.s7comm import pipeline
from netsec_ml.s7comm.release import verify_release
from tests.unit.s7comm.synthetic import write_polling


def config(tmp_path, threshold):
    features = tmp_path / "features"
    features.mkdir(exist_ok=True)
    for capture, client, events, seed in (("a", "10.0.0.1", 900, 1), ("b", "10.0.0.2", 900, 2),
                                          ("c", "10.0.0.3", 300, 3), ("d", "10.0.0.4", 200, 4)):
        write_polling(features / f"{capture}.csv", capture, client, events, seed)
    cfg = {
        "release_id": "test_r1", "features_dir": str(features),
        "sources": {"a": {"captures": ["a"], "role": "normal"}, "b": {"captures": ["b"], "role": "normal"},
                    "c": {"captures": ["c"], "role": "unseen"}, "d": {"captures": ["d"], "role": "report"}},
        "split": {"train": 0.70, "validation": 0.15, "gap_events": 64, "gap_fraction": 0.01},
        "model": {"hidden": 8, "latent": 4, "sequence": 16, "train_stride": 4},
        "training": {"batch": 64, "lr": 0.01, "weight_decay": 1.0e-5, "clip": 1.0, "max_epochs": 2,
                     "patience": 2, "seed": 1, "threads": 1},
        "candidates": [{"name": "only", "identity_weight": 0.5, "balance": "equal", "write_fraction": 0.1}],
        "calibration": {"alpha": {"RESPONSE": 0.001, "READ_REQUEST": 0.001}, "fallback_alpha": 0.001,
                        "min_calibration": 50},
        "gates": {"normal_rate_past_64": threshold, "onnx_parity": 1.0e-4},
        "parity_batches": [1, 8, 32],
    }
    path = tmp_path / f"config-{threshold}.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return path


def test_passing_gates_write_a_verified_release(tmp_path):
    out = tmp_path / "run"
    assert pipeline.main(["--config", str(config(tmp_path, 0.0)), "--out", str(out)]) == 0
    summary = json.loads((out / "evaluation_summary.json").read_text())
    assert summary["gates"]["G1"]["passed"] and summary["gates"]["G3"]["passed"]
    assert [r["source"] for r in summary["g1"]] == ["a", "b"]
    assert [r["source"] for r in summary["unseen"]] == ["c"]
    assert [r["source"] for r in summary["report"]] == ["d"]
    assert set(summary["leave_one_source_out"]["only"]) == {"a", "b"}
    manifest = verify_release(out / "release" / "models" / "stage1_anomaly" / "test_r1")
    assert "outputs/evaluation_summary.json" in manifest["files"]
    assert (out / "MODEL_CARD.md").read_text().startswith("# S7comm Stage 1 detector")


def test_a_failing_gate_writes_no_release_and_names_the_failure(tmp_path, capsys):
    out = tmp_path / "run"
    assert pipeline.main(["--config", str(config(tmp_path, 1.01)), "--out", str(out)]) == 1
    summary = json.loads((out / "evaluation_summary.json").read_text())
    assert not summary["gates"]["G1"]["passed"]
    assert not (out / "release").exists()
    assert "G1 failed" in capsys.readouterr().err


def test_an_existing_output_directory_is_refused(tmp_path):
    (tmp_path / "run").mkdir()
    assert pipeline.main(["--config", str(config(tmp_path, 0.0)), "--out", str(tmp_path / "run")]) == 2
