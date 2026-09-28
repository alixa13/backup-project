#!/usr/bin/env python3
"""Generate the Modbus detector oracle by running the model team's own code.

Builds seeded synthetic Modbus traffic as icsnpp-modbus v1.0.0 writes it --
`values` strings, responses without address or quantity -- then does what
upstream's capture adapter did before training (spec section 2.1: values as
numeric arrays; a response's address, quantity and `matched` from its pending
request), runs upstream's own 07b engine for the 42 features, applies the
frozen preprocessing contract, and runs the delivered ONNX graph in Python
ONNX Runtime for both scores. Refuses to run unless each upstream file's
SHA-256 matches the one this fixture was designed against.

Usage (from the repository root, in a venv with pandas, pyarrow, numpy, onnxruntime):
    python3 tests/fixtures/modbus/generate_detector_oracle.py \
        --delivery models/modbus/stage1_anomaly_detector \
        --out tests/fixtures/modbus/detector_oracle_v1.jsonl
The output is deterministic.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import random
import sys
from pathlib import Path

import numpy as np
import onnxruntime as ort
import pandas as pd

PINS = {
    "src/07b_materialize_feature_engine_v1.py": "e2abcfa3",
    "contracts/modbus_feature_contract_v1/FEATURE_CONTRACT_V1.json": "620c9d00",
    "contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json": "9ecff68f",
    "models/dual_head_model_fp32.onnx": "b5f28fec",
}
L, T_DENSE, T_TEMPORAL = 20, 0.2483385056257248, 0.4121147692203522
FUNCS = {"READ_COILS": 1, "READ_HOLDING_REGISTERS": 3, "WRITE_SINGLE_REGISTER": 6}


def check_pins(delivery: Path) -> None:
    # Each upstream file must be the exact one this fixture was designed on.
    for rel, prefix in PINS.items():
        digest = hashlib.sha256((delivery / rel).read_bytes()).hexdigest()
        if not digest.startswith(prefix):
            raise SystemExit(f"{rel}: SHA-256 {digest[:8]} is not the pinned {prefix}")


def traffic(seed: int) -> list[dict]:
    # Three streams (units 1, 2, 3) of polling; unit 1 runs 30 events, then a
    # 20 s gap (a segment break), then 22 more; unit 2 runs 24; unit 3 runs 10.
    rng = random.Random(seed)
    records, ts, tid = [], 1_790_000_000.0, 0
    plan = [(1, 15), (2, 12), (3, 5), (1, 0), (1, 11)]
    for unit, pairs in plan:
        if pairs == 0:
            ts += 20.0
            continue
        for _ in range(pairs):
            tid = (tid + 1) % 65536
            func = rng.choice(list(FUNCS))
            address, quantity = rng.choice([0, 100, 200]), rng.choice([1, 2, 4])
            request = {"ts": round(ts, 6), "uid": f"CORACLE{unit}", "id_orig_h": "10.0.0.5", "id_orig_p": 50200,
                       "id_resp_h": "10.0.0.9", "id_resp_p": 502, "is_orig": True, "source_h": "10.0.0.5",
                       "source_p": 50200, "destination_h": "10.0.0.9", "destination_p": 502, "tid": tid,
                       "unit": unit, "func": func, "request_response": "REQUEST", "address": address,
                       "quantity": quantity}
            if func == "WRITE_SINGLE_REGISTER":
                request["values"] = str(rng.randrange(65536))
            ts += rng.uniform(0.002, 0.02)
            response = {"ts": round(ts, 6), "uid": f"CORACLE{unit}", "id_orig_h": "10.0.0.5", "id_orig_p": 50200,
                        "id_resp_h": "10.0.0.9", "id_resp_p": 502, "is_orig": False, "source_h": "10.0.0.9",
                        "source_p": 502, "destination_h": "10.0.0.5", "destination_p": 50200, "tid": tid,
                        "unit": unit, "func": func, "request_response": "RESPONSE"}
            # The quantity (and, for a write, the address) Zeek writes on each
            # response, as on real icsnpp-modbus v1.0.0 output (CIC Modbus 2023
            # captures): a coil read's is the bits returned -- 8 per byte --
            # not the quantity asked for; a register read's and a write's are
            # the request's. No draw from rng, so the traffic itself is unchanged.
            if func == "READ_COILS":
                response["quantity"] = 8 * ((quantity + 7) // 8)
                response["values"] = ",".join(rng.choice("TF") for _ in range(quantity))
            elif func == "READ_HOLDING_REGISTERS":
                response["quantity"] = quantity
                response["values"] = ",".join(str(rng.randrange(65536)) for _ in range(quantity))
            else:
                response["address"], response["quantity"] = address, 1
                response["values"] = request["values"]
            records += [request, response]
            ts += rng.uniform(0.1, 0.5)
    return records


def values_of(text: str | None) -> list[float]:
    # Spec F1: T/F -> 1/0, decimals -> numbers.
    if not text:
        return []
    return [1.0 if t == "T" else 0.0 if t == "F" else float(t) for t in text.split(",")]


def canonical(records: list[dict]) -> pd.DataFrame:
    # Upstream's adapter shape: arrays, and a response's address (when it has
    # none), quantity (always) and matched from its pending request (spec F2),
    # per stream and segment.
    rows, pending, last_ts = [], {}, {}
    for i, r in enumerate(records):
        key = (r["unit"],)
        if key in last_ts and r["ts"] - last_ts[key] > 15.0:
            pending[key] = {}
        last_ts[key] = r["ts"]
        stream = pending.setdefault(key, {})
        is_req = r["request_response"] == "REQUEST"
        address, quantity, matched = r.get("address"), r.get("quantity"), False
        if is_req:
            stream[r["tid"]] = (address, quantity)
        elif r["tid"] in stream:
            req_address, req_quantity = stream.pop(r["tid"])
            address = address if address is not None else req_address
            # F2, amended 2026-09-28: the request's quantity wins over the response's own.
            quantity = req_quantity if req_quantity is not None else quantity
            matched = True
        rows.append({"capture_key": "oracle", "capture_event_index": i, "ts": r["ts"],
                     "src_ip": r["source_h"], "dst_ip": r["destination_h"],
                     "direction": "request" if is_req else "response", "transaction_id": r["tid"],
                     "unit_id": r["unit"], "function_code": FUNCS[r["func"]], "address": address,
                     "quantity": quantity, "matched": matched,
                     "request_values": json.dumps(values_of(r.get("values")) if is_req else []),
                     "response_values": json.dumps([] if is_req else values_of(r.get("values")))})
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivery", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    check_pins(args.delivery)

    # Upstream's own engine, imported from the delivery as it is.
    spec = importlib.util.spec_from_file_location("engine07b", args.delivery / "src/07b_materialize_feature_engine_v1.py")
    engine = importlib.util.module_from_spec(spec)
    # Registered before it runs: its dataclasses look their module up in sys.modules.
    sys.modules[spec.name] = engine
    spec.loader.exec_module(engine)
    names = json.loads((args.delivery / "contracts/modbus_feature_contract_v1/FEATURE_CONTRACT_V1.json")
                       .read_text())["feature_order"]
    prep = json.loads((args.delivery / "contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json")
                      .read_text())
    params = {p["feature"]: p for p in prep["parameters"]}
    session = ort.InferenceSession(str(args.delivery / "models/dual_head_model_fp32.onnx"),
                                   providers=["CPUExecutionProvider"])

    records = traffic(seed=20260926)
    output, _qa = engine.process_capture(canonical(records), names)
    matrix = output[names].to_numpy(dtype=np.float32)

    def preprocess(v: np.ndarray) -> np.ndarray:
        # The contract's transform_definitions, in double, carried as float32.
        out = np.zeros(len(names))
        for i, n in enumerate(names):
            p, x = params[n], float(v[i])
            mask = 1.0 if not p["mask_feature"] else float(v[names.index(p["mask_feature"])])
            pol = p["policy"]
            if pol.startswith("CONDITIONAL") and mask != 1.0:
                out[i] = 0.0
            elif pol.startswith("PASSTHROUGH"):
                out[i] = x
            elif pol in ("GLOBAL_STANDARD", "CONDITIONAL_STANDARD"):
                out[i] = (x - p["mean"]) / p["std"]
            elif pol in ("GLOBAL_LOG1P_ONLY", "CONDITIONAL_LOG1P_ONLY"):
                out[i] = np.log1p(x)
            else:
                out[i] = (np.log1p(x) - p["mean"]) / p["std"]
        return out.astype(np.float32)

    windows: dict[str, list[np.ndarray]] = {}
    with args.out.open("w") as f:
        for i, record in enumerate(records):
            key = f"{output['client_ip'].iloc[i]}|{output['server_ip'].iloc[i]}|{output['unit_id'].iloc[i]}"
            vector = matrix[i]
            pre = preprocess(vector)
            # A segment start (prev_event_available == 0) empties the window.
            window = windows.setdefault(key, [])
            if vector[names.index("prev_event_available")] == 0.0:
                window.clear()
            window.append(pre)
            del window[:-L]
            verdict, dense, temporal = "WARMUP", None, None
            if len(window) == L:
                x = np.asarray([window], dtype=np.float32)
                d, t = session.run(None, {"sequence_20x42": x})
                dense = float(np.mean(np.abs(x - d)))
                temporal = float(np.mean(np.abs(t[0, -1] - x[0, -1])))
                verdict = "ANOMALY" if dense > T_DENSE or temporal > T_TEMPORAL else "NORMAL"
            f.write(json.dumps({"raw": record, "key": key, "vector": [float(v) for v in vector],
                                "preprocessed": [float(v) for v in pre], "verdict": verdict,
                                "dense": dense, "temporal": temporal}, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
