"""One training run (spec sections 6 and 7): load the exports, split by time, choose a candidate on
validation and leave-one-source-out runs over the training sources, train the final model, calibrate,
score every source once, check G1 and G3, and release only when both hold.

    python -m netsec_ml.s7comm.pipeline --config configs/s7comm-v2.yaml --out /data/run-1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import onnxruntime as ort
import torch

from netsec_ml.s7comm import calibrate as C
from netsec_ml.s7comm import data as D
from netsec_ml.s7comm import evaluate as E
from netsec_ml.s7comm import preprocessing as P
from netsec_ml.s7comm import release as R
from netsec_ml.s7comm.model import column_weights
from netsec_ml.s7comm.release import sha256
from netsec_ml.s7comm.sequences import connection_rows, sampling_weights, window_rows
from netsec_ml.s7comm.train import TrainConfig, train_model


@dataclass
class Fit:
    """One trained candidate: its preprocessing, inputs, model, policy and every row's score."""
    pre: object
    X: np.ndarray
    model: object
    history: list
    policy: C.Policy
    scores: np.ndarray
    score_weights: np.ndarray


def fit(data, part, connections, groups, sources, candidate, cfg) -> Fit:
    """Train `candidate` on the train part of `sources`, calibrate on their validation part, and
    score every row of every claimed connection (the online rule, stride 1)."""
    train = (part == "train").to_numpy() & data["source"].isin(sources).to_numpy()
    val = (part == "validation").to_numpy() & data["source"].isin(sources).to_numpy()
    pre = P.fit(data[train])
    X = P.transform(pre, data)
    names = [str(n) for n in pre.get_feature_names_out()]
    loss_w = column_weights(names, ["s7_operation"], candidate["identity_weight"])
    score_w = column_weights(names, ["s7_operation"], 0.0)

    # Training windows lie wholly inside one part (Review Focus 1); sample weights per the recipe.
    m, t = cfg["model"], cfg["training"]
    train_w = window_rows(connections, m["sequence"], m["train_stride"], train)
    val_w = window_rows(connections, m["sequence"], 1, val)
    last = train_w[:, -1]
    is_write = (data["is_request_direction"].to_numpy() == 1) & (data["operation_code"].to_numpy() == 5)
    weights = sampling_weights(data["source"].to_numpy()[last], is_write[last], candidate["balance"],
                               candidate["write_fraction"])
    tc = TrainConfig(hidden=m["hidden"], latent=m["latent"], batch=t["batch"], lr=t["lr"],
                     weight_decay=t["weight_decay"], clip=t["clip"], max_epochs=t["max_epochs"],
                     patience=t["patience"], seed=t["seed"])
    model, history = train_model(X, train_w, weights, val_w, loss_w, score_w, tc)

    # Every row's score (each connection whole, as online), then the conformal policy.
    scores = C.stream_scores(model, X, connections, score_w)
    cal = cfg["calibration"]
    policy = C.calibrate(scores[val], groups[val], cal["alpha"], cal["fallback_alpha"], cal["min_calibration"])
    return Fit(pre, X, model, history, policy, scores, score_w)


def select(data, part, connections, groups, normal, cfg):
    """Each candidate's leave-one-source-out NORMAL rate past the 64th event, on each held-out
    source's train and validation rows (never its test rows); the best mean wins, ties to the
    first in config order."""
    held_rows = part.isin(["train", "validation"]).to_numpy()
    results = []
    for candidate in cfg["candidates"]:
        loso = {}
        for held in normal:
            f = fit(data, part, connections, groups, [s for s in normal if s != held], candidate, cfg)
            verdicts = C.decide(f.policy, f.scores, groups)
            mask = held_rows & (data["source"] == held).to_numpy()
            table = E.rate_table(data, verdicts, mask)
            loso[held] = float(table["normal_rate_past_64"].iloc[0]) if len(table) else float("nan")
        results.append({"name": candidate["name"], "leave_one_source_out": loso,
                        "objective": float(np.nanmean(list(loso.values()))) if loso else float("nan")})
    best = max(range(len(results)), key=lambda i: (np.nan_to_num(results[i]["objective"], nan=-1.0), -i))
    return cfg["candidates"][best], results


def inference_cost(onnx_path, X, windows, n=1000) -> float:
    """Microseconds per window, ONNX Runtime, batch 1, one thread: the online job's setting."""
    options = ort.SessionOptions()
    options.intra_op_num_threads = options.inter_op_num_threads = 1
    session = ort.InferenceSession(str(onnx_path), options, providers=["CPUExecutionProvider"])
    batch = [np.ascontiguousarray(X[w][None], dtype=np.float32) for w in windows[:n]]
    start = time.perf_counter()
    for x in batch:
        session.run(None, {"input": x})
    return 1e6 * (time.perf_counter() - start) / max(len(batch), 1)


def run(cfg, out: Path) -> int:
    """The run itself; returns 0 with a release, 1 when a gate fails."""
    torch.set_num_threads(int(cfg["training"]["threads"]))
    sources = D.sources_of(cfg)
    data = D.load_sources(cfg, sources)
    data = data[data["source"] != ""].reset_index(drop=True)  # unclaimed connections are not used
    part = D.split_by_time(data, cfg["split"])
    connections = connection_rows(data)
    groups = C.score_groups(data["is_request_direction"].to_numpy(), data["operation_code"].to_numpy())
    normal = [s.name for s in sources if s.role == "normal"]

    # Choose on training sources only, then train the final model on all of them.
    candidate, candidates = select(data, part, connections, groups, normal, cfg)
    print(f"chosen candidate: {candidate['name']}", flush=True)
    final = fit(data, part, connections, groups, normal, candidate, cfg)
    verdicts = C.decide(final.policy, final.scores, groups)

    # G1 on held-out normal rows; everything else reported.
    role, late = data["role"].to_numpy(), data["event_index"].to_numpy() >= E.PAST
    gated = ((role == "normal") & (part == "test").to_numpy()) | (role == "normal-test")
    g1 = E.rate_table(data, verdicts, gated)
    g1_failures = E.gate_g1(g1, cfg["gates"]["normal_rate_past_64"])

    # G3: ONNX against PyTorch on validation windows.
    val_w = window_rows(connections, 16, 1, (part == "validation").to_numpy())
    onnx_path = out / "candidate.onnx"
    R.export_onnx(final.model, onnx_path, 16, final.X.shape[1])
    parity = R.onnx_parity(final.model, onnx_path, final.X, val_w, tuple(cfg.get("parity_batches", (1, 8, 32, 256))))
    max_diff = max(p["max_abs_difference"] for p in parity)
    g3_ok = max_diff <= cfg["gates"]["onnx_parity"]

    summary = {
        "release_id": cfg["release_id"],
        "gates": {"G1": {"passed": not g1_failures, "threshold": cfg["gates"]["normal_rate_past_64"],
                         "failures": g1_failures},
                  "G3": {"passed": g3_ok, "threshold": cfg["gates"]["onnx_parity"], "max_abs_difference": max_diff}},
        "g1": g1.to_dict("records"),
        "unseen": E.rate_table(data, verdicts, role == "unseen").to_dict("records"),
        "report": E.rate_table(data, verdicts, role == "report").to_dict("records"),
        "groups": E.group_table(groups, verdicts, gated, late).to_dict("records"),
        "leave_one_source_out": {c["name"]: c["leave_one_source_out"] for c in candidates},
        "selection": {"chosen": candidate["name"], "candidates": candidates},
        "training_history": final.history,
        "parity": parity,
        "inference_us_per_window": inference_cost(onnx_path, final.X, val_w),
        "sources": {s.name: {"role": s.role, "captures": list(s.captures), "clients": list(s.clients),
                             "rows": {str(k): int(v) for k, v in part[data["source"] == s.name].value_counts().items()}}
                    for s in sources},
        "exports": {c: sha256(Path(cfg["features_dir"]) / f"{c}.csv") for c in sorted(data["capture"].unique())},
    }
    (out / "evaluation_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    card = E.render_card(summary)
    (out / "MODEL_CARD.md").write_text(card, encoding="utf-8")
    print(card, flush=True)
    onnx_path.unlink()

    # Release only when both gates hold (spec section 7: a failure goes to the owner first).
    if g1_failures or not g3_ok:
        for f in g1_failures:
            print(f"G1 failed: {f}", file=sys.stderr)
        if not g3_ok:
            print(f"G3 failed: ONNX differs from PyTorch by {max_diff:.3g}", file=sys.stderr)
        return 1
    rel = R.write_release(
        out / "release", cfg["release_id"], model=final.model,
        model_config={"hidden_size": cfg["model"]["hidden"], "latent_size": cfg["model"]["latent"]},
        pre=final.pre, policy=final.policy, score_weights=final.score_weights, parity=parity,
        documents={"outputs/evaluation_summary.json": summary, "config/run_config.json": cfg},
        summary={"selection": {"selected_candidate": candidate, "attack_used_for_selection": False,
                               "test_used_for_selection": False},
                 "internal_metrics": {"g1": summary["g1"], "onnx_max_abs_difference": max_diff}})
    print(f"release written: {rel}", flush=True)
    return 0


def main(argv=None) -> int:
    """CLI entry point; 2 on a usage error or an existing output directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    out = Path(args.out)
    if out.exists():
        print(f"{out} exists: every run gets a new output directory", file=sys.stderr)
        return 2
    out.mkdir(parents=True)
    return run(D.load_config(args.config), out)


if __name__ == "__main__":
    sys.exit(main())
