#!/usr/bin/env python3
"""Generate the S7comm Stage 1 scoring oracle for detector v2 (scoring spec amendment A1).

Builds deterministic S7 traffic exactly as icsnpp-s7comm writes it (both
endpoint pairs, hex-string function codes). Each record goes, as written, to
upstream's own feature builder (the independent reference for the features;
no S2). The generator then runs detector v2's released files: its fitted
preprocessor (preprocessor.joblib, fed the categories exactly as decoded; no
S1), its ONNX graph in Python ONNX Runtime, its calibration scores and policy,
and upstream's own operation_groups and conformal_pvalues, exec'd from
debiased.py with its torch import skipped. The features handed to the
preprocessor are first rounded to float32, exactly as the Java vector carries
them. The first 64 events after a reset are WARMUP (amendment A1, item 5).

Refuses to run unless every pinned file's SHA-256 matches the one this fixture
was designed against. Refuses to write a fixture that misses a verdict it must
cover, or that holds a verdict which could flip within
S7commDetectorOracleTest's score tolerance (plan ruling P3).

Usage (from the repository root; training/.venv holds numpy, pandas,
scikit-learn, joblib and onnxruntime, and training/src holds the netsec_ml
classes v2's preprocessor.joblib unpickles):
    PYTHONPATH=training/src training/.venv/bin/python tests/fixtures/s7comm/generate_detector_oracle.py \
        --delivery models/S7 --release models/S7v2/models/stage1_anomaly/v2_multisource_r1 \
        --out tests/fixtures/s7comm/detector_oracle_v1.jsonl
The output is deterministic.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

import joblib
import numpy as np
import onnxruntime as ort
import pandas as pd

# Upstream's code (the feature reference and the decision functions): delivery file
# -> (its place in the throwaway package, or None when only read) and its SHA-256.
PINS = {
    "src/s7zeek/domain/events.py": (
        "s7zeek/domain/events.py", "60413956423e0ffa770e0fdaea8c3529d58068e6e9b45cdf9a18d3abbb7c72ae"),
    "src/s7zeek/features/s7_parser.py": (
        "s7zeek/features/s7_parser.py", "8ef6bbdead97a4207d8cc74b98494fa4dd87dd85b3d572514cbe0aa6cda377fe"),
    "src/s7zeek/features/customer_icsnpp_enriched_builder.py": (
        "s7zeek/features/customer_icsnpp_enriched_builder.py",
        "3aa48d969095e45c0084f713f8a34c9a8092674dc5f0cad9cec2998c3ffb3c99"),
    "src/s7zeek/features/customer_icsnpp_time_normalized_builder.py": (
        "s7zeek/features/customer_icsnpp_time_normalized_builder.py",
        "65a03719c1986705d2819ebbd27d352cd66f7fb6ba8dc06115cbcc06180d7fb4"),
    "src/s7zeek/adapters/kafka_source.py": (
        "s7zeek/adapters/kafka_source.py", "bedae0d6427a86dac97b1a0af1c3bbcfbc3301ac8741575590dc28a331d39aca"),
    "src/s7zeek/modeling/debiased.py": (
        None, "faf09cf290dbf534e2d24b5c3aba9bb2ddee9f9b0e28a3b84611244896f162b1"),
    "src/s7zeek/inference/causal_shadow.py": (
        None, "2cded3d7d4384486903ab3bc23feaf86d0d4032387744c633801afd0fa2567e3"),
}
# Detector v2's scoring files (release v2_multisource_r1), pinned from its FROZEN_MANIFEST.json.
ARTIFACTS = "artifacts/model"
RELEASE_PINS = {
    f"{ARTIFACTS}/preprocessor.joblib": "079aa3dca67a3b43177cb42859889f894f45d5ef8983cdda2dbefcee57162c28",
    f"{ARTIFACTS}/s7comm_lstm_autoencoder.onnx": "59992382178b1f367e6f189e41f69ceaee0b96d69f228b8a3d631bc5c2571002",
    f"{ARTIFACTS}/causal_online_shadow_policy.json": "227ccc3841af889cfc55e59e152332428771478e8c1d207c502dd745aca68f73",
    f"{ARTIFACTS}/causal_online_conformal_calibration_scores.npz":
        "b36a62590478e7c1eb48c1d2cedd6490ed72ec0e321a9bbc77ab9bd165a2a3ed",
}
FEATURES = [
    "s7_outstanding_requests", "s7_outstanding_mean_16", "s7_response_match_rate_16",
    "s7_same_function_run_length", "s7_same_direction_run_length", "s7_request_ratio_16",
    "s7_direction_change_rate_16", "s7_function_change_rate_16", "s7_function_entropy_16",
    "s7_function_transition_entropy_16", "s7_rosctr_change_rate_16",
    "s7_pdu_reference_unique_ratio_32", "is_request_direction", "s7_function_changed",
    "s7_rosctr", "s7_operation",
]
NUMERIC = FEATURES[:14]
SEQUENCE = 16
# The first events after a reset that are never scored (scoring spec amendment A1, item 5).
WARMUP_EVENTS = 64

# The endpoints every synthetic connection uses; the live check overrides them.
CLIENT = ("10.0.20.5", 50110)
SERVER = ("10.0.20.9", 102)

# A request/response kind: (request ROSCTR, request code, request name, response ROSCTR, response code).
READ = (1, 0x04, "Read Variable", 3, 0x04)
WRITE = (1, 0x05, "Write Variable", 3, 0x05)
SETUP = (1, 0xF0, "Setup Communication", 3, 0xF0)
CPU_FUNCTIONS = (7, 0x44, "Request: CPU Functions", 7, 0x84)  # user data, codes as ICSNPP writes them
PLC_STOP = (1, 0x29, "PLC Stop", 3, 0x29)
ACK_READ = (1, 0x04, "Read Variable", 2, None)  # answered by a bare ACK (ROSCTR 2), never seen in training


def tolerance(score: float) -> float:
    """S7commDetectorOracleTest's score tolerance (plan ruling P3)."""
    return 1e-9 + 1e-4 * abs(score)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_package(delivery: Path) -> Path:
    """Copy the pinned upstream modules into a throwaway importable package; verify every pin."""
    root = Path(tempfile.mkdtemp(prefix="s7detector-"))
    for package in ("s7zeek", "s7zeek/domain", "s7zeek/features", "s7zeek/adapters"):
        (root / package).mkdir(parents=True, exist_ok=True)
        (root / package / "__init__.py").write_text("")
    for name, (target, expected) in PINS.items():
        actual = sha256(delivery / name)
        if actual != expected:
            sys.exit(f"{delivery / name}: sha256 {actual} != expected {expected}; "
                     "the delivery changed -- review it before regenerating this fixture")
        if target is not None:
            shutil.copyfile(delivery / name, root / target)
    return root


def verify_release(release: Path) -> None:
    """Every scoring file of the release is the one this fixture was generated from."""
    for name, expected in RELEASE_PINS.items():
        actual = sha256(release / name)
        if actual != expected:
            sys.exit(f"{release / name}: sha256 {actual} != expected {expected}; "
                     "the release changed -- a new model is a new fixture")


def decision_functions(delivery: Path) -> dict:
    """operation_groups, conformal_pvalues and GROUP_NAMES, exec'd from debiased.py itself."""
    tree = ast.parse((delivery / "src/s7zeek/modeling/debiased.py").read_text())
    keep = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name in ("operation_groups", "conformal_pvalues"))
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", "").startswith("GROUP") for t in n.targets))]
    namespace = {"np": np}
    exec(compile(ast.Module(body=keep, type_ignores=[]), "debiased.py", "exec"), namespace)
    return namespace


def record(uid: str, ts: float, request: bool, pdu: int, kind: tuple) -> dict:
    """One ICSNPP s7comm.log record: both endpoint pairs, the code as a hex string."""
    rosctr, code, name = (kind[0], kind[1], kind[2]) if request else (kind[3], kind[4], None)
    sender, receiver = (CLIENT, SERVER) if request else (SERVER, CLIENT)
    r = {"ts": round(ts, 6), "uid": uid, "id_orig_h": CLIENT[0], "id_orig_p": CLIENT[1],
         "id_resp_h": SERVER[0], "id_resp_p": SERVER[1], "is_orig": request,
         "source_h": sender[0], "source_p": sender[1], "destination_h": receiver[0], "destination_p": receiver[1],
         "rosctr_code": rosctr, "pdu_reference": pdu}
    if code is not None:
        r["function_code"] = f"0x{code:02x}"
    if name is not None:
        r["function_name"] = name
    return r


def pipelined(uid: str, requests: int, kind_of, t0: float) -> list[dict]:
    """The training capture's shape: two requests (PDU references 7 and 8) in flight before their responses."""
    out, ts = [], t0
    for i in range(0, requests, 2):
        a, b = kind_of(i), kind_of(i + 1)
        out += [record(uid, ts, True, 7, a), record(uid, ts + 0.001, True, 8, b),
                record(uid, ts + 0.004, False, 7, a), record(uid, ts + 0.005, False, 8, b)]
        ts += 0.1
    return out


def alternating(uid: str, requests: int, kind: tuple, t0: float) -> list[dict]:
    """Strict request/response alternation with a new PDU reference each time: common, and far from training."""
    out, ts = [], t0
    for i in range(requests):
        out += [record(uid, ts, True, 100 + i, kind), record(uid, ts + 0.004, False, 100 + i, kind)]
        ts += 0.1
    return out


def flood(uid: str, requests: int, t0: float) -> list[dict]:
    """Reads that are never answered."""
    return [record(uid, t0 + i * 0.001, True, 1000 + i, READ) for i in range(requests)]


# (name, records, the index before which the connection's state is dropped -- a TTL expiry -- or None)
STREAMS = [
    ("pipelined-read", pipelined("CS7ORA", 120, lambda i: SETUP if i == 0 else READ, 1_790_000_000.0), None),
    ("pipelined-userdata",
     pipelined("CS7ORB", 120, lambda i: CPU_FUNCTIONS if i % 20 == 19 else READ, 1_790_001_000.0), None),
    ("pipelined-write", pipelined("CS7ORC", 80, lambda i: WRITE if i % 20 == 19 else READ, 1_790_002_000.0), None),
    ("pipelined-ack-and-stop",
     pipelined("CS7ORD", 80, lambda i: PLC_STOP if i == 41 else (ACK_READ if i % 10 == 9 else READ),
               1_790_003_000.0), None),
    ("alternating-read", alternating("CS7ORE", 40, READ, 1_790_004_000.0), None),
    ("alternating-userdata", alternating("CS7ORF", 50, CPU_FUNCTIONS, 1_790_005_000.0), None),
    ("unanswered-flood", flood("CS7ORG", 100, 1_790_006_000.0), None),
    ("reset", pipelined("CS7ORH", 60, lambda i: READ, 1_790_007_000.0), 40),
]


class Reference:
    """The reference runtime: upstream's builder on the records as written, detector v2's fitted
    preprocessor, ONNX graph, calibration and policy, and upstream's conformal decision."""

    def __init__(self, delivery: Path, release: Path):
        self.package = build_package(delivery)
        sys.path.insert(0, str(self.package))
        from s7zeek.adapters.kafka_source import normalize_zeek_message
        from s7zeek.features.customer_icsnpp_time_normalized_builder import (
            CustomerICSNPPTimeNormalizedFeatureBuilder,
        )
        self.normalize = normalize_zeek_message
        self.builder_type = CustomerICSNPPTimeNormalizedFeatureBuilder
        self.decide = decision_functions(delivery)
        verify_release(release)
        release = release / ARTIFACTS
        self.pre = joblib.load(release / "preprocessor.joblib")
        if list(self.pre.feature_names_in_) != FEATURES:
            sys.exit("the release's preprocessor's features are not s7comm-feature-v1's")
        # One thread each way, so the graph's arithmetic is the same run to run.
        options = ort.SessionOptions()
        options.intra_op_num_threads = 1
        options.inter_op_num_threads = 1
        self.session = ort.InferenceSession(str(release / "s7comm_lstm_autoencoder.onnx"), options,
                                            providers=["CPUExecutionProvider"])
        self.policy = json.loads((release / "causal_online_shadow_policy.json").read_text())
        self.calibration = np.load(release / "causal_online_conformal_calibration_scores.npz")
        self.groups = self.decide["GROUP_NAMES"]
        self.pooled = np.concatenate([self.calibration[g] for g in self.groups.values()
                                      if len(self.calibration[g])])
        # causal_shadow.py's weights: 0 on the s7_operation columns, 1 elsewhere.
        self.weights = np.array([0.0 if n.startswith("categorical__s7_operation_") else 1.0
                                 for n in self.pre.get_feature_names_out()], np.float32)

    def close(self):
        shutil.rmtree(self.package)

    def reference(self, group: str):
        """apply_group_conformal_policy: a group with no scores is judged against all of them, fallback alpha."""
        own = self.calibration[group]
        if len(own) == 0:
            return self.pooled, float(self.policy["fallback_alpha"])
        return own, float(self.policy["alpha_by_group"].get(group, self.policy["fallback_alpha"]))

    def p_value(self, calibration, score: float) -> float:
        return float(self.decide["conformal_pvalues"](calibration, np.array([score], np.float64))[0])

    def run(self, records: list[dict], reset_at: int | None = None) -> list[dict]:
        """One connection's records -> one oracle line per record."""
        builder, window, since, out = self.builder_type(), [], 0, []
        for index, raw in enumerate(records):
            reset = index == reset_at
            if reset:  # a TTL expiry: the feature state and the window start empty
                builder, window, since = self.builder_type(), [], 0
            event = self.normalize(dict(raw))
            row = builder.process_event(event)
            numeric = [float(np.float32(row[f])) for f in NUMERIC]  # what the Java vector carries
            frame = pd.DataFrame([numeric + [row["s7_rosctr"], row["s7_operation"]]], columns=FEATURES)
            x = np.asarray(self.pre.transform(frame), np.float32)[0]
            window.append(x)
            del window[:-SEQUENCE]
            since += 1
            code = event.function_code
            group = self.groups[int(self.decide["operation_groups"](
                [row["is_request_direction"]], [int(code == 4)], [int(code == 5)])[0])]
            calibration, alpha = self.reference(group)
            line = {"reset_before": reset, "raw": json.dumps(raw, sort_keys=True), "values": numeric,
                    "rosctr": row["s7_rosctr"], "operation": row["s7_operation"],
                    "preprocessed": [float(v) for v in x], "group": group, "alpha": alpha,
                    "events_since_reset": since}
            if len(window) < SEQUENCE or since <= WARMUP_EVENTS:
                line.update(verdict="WARMUP", score=None, p=None, p_low=None, p_high=None)
            else:
                seq = np.stack(window)[None]
                recon = self.session.run(None, {"input": seq})[0]
                # causal_shadow.py's score, in its float32 arithmetic.
                score = float(((((recon[:, -1, :] - seq[:, -1, :]) ** 2) * self.weights).sum(axis=1)
                               / np.float32(self.weights.sum()))[0])
                p = self.p_value(calibration, score)
                line.update(verdict="ANOMALY" if np.isfinite(p) and p <= alpha else "NORMAL", score=score, p=p,
                            p_low=self.p_value(calibration, score + tolerance(score)),
                            p_high=self.p_value(calibration, score - tolerance(score)))
            out.append(line)
        return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivery", required=True, type=Path)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    reference = Reference(args.delivery, args.release)
    lines = [json.dumps({"meta": {
        "generator": "tests/fixtures/s7comm/generate_detector_oracle.py",
        "python": sys.version.split()[0],
        "onnxruntime": ort.__version__,
        "delivery_sha256": {name: sha for name, (_, sha) in PINS.items()},
        "release": "s7comm-stage1-detector/v2 (v2_multisource_r1)",
        "release_sha256": RELEASE_PINS,
        "features": FEATURES,
        "warmup_events": WARMUP_EVENTS,
        "input_amendments": [],
    }}, sort_keys=True)]
    coverage = {g: {"NORMAL": 0, "ANOMALY": 0} for g in reference.groups.values()}
    ambiguous = []
    try:
        for stream, records, reset_at in STREAMS:
            for index, line in enumerate(reference.run(records, reset_at)):
                line["stream"] = stream
                if line["verdict"] in ("NORMAL", "ANOMALY"):
                    coverage[line["group"]][line["verdict"]] += 1
                    if (line["p_low"] <= line["alpha"]) != (line["p_high"] <= line["alpha"]):
                        ambiguous.append(f"{stream} event {index}")
                lines.append(json.dumps(line, sort_keys=True))
    finally:
        reference.close()

    # What the fixture must cover: both verdicts where the detector can give them. Detector v2
    # never saw an OTHER request past a connection's warm-up (its training holds 21, nearly all a
    # connection's first event), so it flags every one; OTHER_REQUEST's pooled-reference path is
    # still covered, through its ANOMALY lines' p-values.
    for group, counts in coverage.items():
        print(f"{group:14s} NORMAL {counts['NORMAL']:4d}  ANOMALY {counts['ANOMALY']:4d}")
    required = [(g, "NORMAL") for g in ("RESPONSE", "READ_REQUEST")] \
        + [(g, "ANOMALY") for g in coverage]
    missing = [f"{g} {v}" for g, v in required if coverage[g][v] == 0]
    if ambiguous:
        sys.exit("verdicts that could flip within the Java test's score tolerance: " + ", ".join(ambiguous[:10])
                 + " -- change the streams")
    if missing:
        sys.exit("the streams produce no " + ", ".join(missing) + " -- change the streams")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {len(lines) - 1} events from {len(STREAMS)} streams to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
