#!/usr/bin/env python3
"""Generate the S7comm upstream-oracle fixture by running the model team's own code.

The frozen S7comm feature builders live in two-models-info/S7___/ (untracked,
never committed). This script copies the five files the feature path needs into
a throwaway `s7zeek` package, refuses to run unless each file's SHA-256 matches
the one this fixture was designed against, and runs
CustomerICSNPPTimeNormalizedFeatureBuilder.process_event over seeded synthetic
event streams. Each output line holds one raw ICSNPP s7comm.log record exactly
as this platform's parser will read it, plus upstream's 16 frozen features for
it: the 14 numeric values (float64) and the 2 category strings.

Usage (from the repository root):
    python3 tests/fixtures/s7comm/generate_upstream_oracle.py \
        --upstream two-models-info/S7___ \
        --out tests/fixtures/s7comm/upstream_oracle_v1.jsonl

The output is deterministic: running it twice produces identical bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import sys
import tempfile
from pathlib import Path

# Upstream file -> where it lives inside the throwaway package, and the SHA-256
# of the exact file this fixture was generated from.
UPSTREAM_FILES = {
    "events.py": ("s7zeek/domain/events.py",
                  "60413956423e0ffa770e0fdaea8c3529d58068e6e9b45cdf9a18d3abbb7c72ae"),
    "s7_parser.py": ("s7zeek/features/s7_parser.py",
                     "8ef6bbdead97a4207d8cc74b98494fa4dd87dd85b3d572514cbe0aa6cda377fe"),
    "customer_icsnpp_enriched_builder.py": (
        "s7zeek/features/customer_icsnpp_enriched_builder.py",
        "3aa48d969095e45c0084f713f8a34c9a8092674dc5f0cad9cec2998c3ffb3c99"),
    "customer_icsnpp_time_normalized_builder.py": (
        "s7zeek/features/customer_icsnpp_time_normalized_builder.py",
        "65a03719c1986705d2819ebbd27d352cd66f7fb6ba8dc06115cbcc06180d7fb4"),
    "kafka_source.py": ("s7zeek/adapters/kafka_source.py",
                        "bedae0d6427a86dac97b1a0af1c3bbcfbc3301ac8741575590dc28a331d39aca"),
}

# STAGE1_RAW_FEATURES, from 90_attack_type_mode_router_v2.py: 12 continuous,
# 2 binary, 2 categorical.
FEATURES = [
    "s7_outstanding_requests", "s7_outstanding_mean_16", "s7_response_match_rate_16",
    "s7_same_function_run_length", "s7_same_direction_run_length", "s7_request_ratio_16",
    "s7_direction_change_rate_16", "s7_function_change_rate_16", "s7_function_entropy_16",
    "s7_function_transition_entropy_16", "s7_rosctr_change_rate_16",
    "s7_pdu_reference_unique_ratio_32", "is_request_direction", "s7_function_changed",
    "s7_rosctr", "s7_operation",
]
NUMERIC = FEATURES[:14]

KNOWN_CODES = [0x04, 0x05, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x28, 0x29, 0xF0]
UNKNOWN_CODES = [0x00, 0x01, 0x12, 0x2A, 0xFF, 0x100]
NAMES_WITHOUT_CODE = ["read_var", "PLC_STOP", "Setup_Communication", "Function_0x12", "Something Else"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_package(upstream: Path) -> Path:
    """Copy the upstream files into a throwaway importable package; verify each hash."""
    root = Path(tempfile.mkdtemp(prefix="s7oracle-"))
    for package in ("s7zeek", "s7zeek/domain", "s7zeek/features", "s7zeek/adapters"):
        (root / package).mkdir(parents=True, exist_ok=True)
        (root / package / "__init__.py").write_text("")
    for name, (target, expected) in UPSTREAM_FILES.items():
        source = upstream / name
        actual = sha256(source)
        if actual != expected:
            sys.exit(f"{source}: sha256 {actual} != expected {expected}; "
                     "the upstream code changed -- review it before regenerating this fixture")
        shutil.copyfile(source, root / target)
    return root


class Connection:
    """One S7 connection (one Zeek uid): a client talking to a PLC."""

    def __init__(self, uid: str, index: int, server_port: int = 102, client_port: int | None = None):
        self.uid = uid
        self.client = (f"10.0.{index}.5", client_port if client_port is not None else 49152 + index)
        self.server = (f"10.0.9.{index + 1}", server_port)
        self.next_pdu = 1
        self.outstanding: list[tuple[int, int | None]] = []  # (pdu, function) of unanswered requests


def render(rng: random.Random, conn: Connection, ts: float, is_request: bool, pdu: int,
           rosctr: int | None, function: int | None, name: str | None, shape: str) -> tuple[dict, dict]:
    """Return (raw record for this platform's parser, equivalent record for upstream's adapter)."""
    sender, receiver = (conn.client, conn.server) if is_request else (conn.server, conn.client)
    is_orig = is_request  # the client originated every connection in these streams
    raw: dict = {"ts": ts, "uid": conn.uid}

    if shape == "per_packet":
        raw.update({"source_h": sender[0], "source_p": sender[1],
                    "destination_h": receiver[0], "destination_p": receiver[1]})
        if rng.random() < 0.3:  # also carry the connection pair: per-packet must still win
            raw.update({"id_orig_h": conn.client[0], "id_orig_p": conn.client[1],
                        "id_resp_h": conn.server[0], "id_resp_p": conn.server[1],
                        "is_orig": is_orig})
    elif shape == "id_underscore":
        raw.update({"id_orig_h": conn.client[0], "id_orig_p": conn.client[1],
                    "id_resp_h": conn.server[0], "id_resp_p": conn.server[1]})
    elif shape == "id_dotted":
        raw.update({"id.orig_h": conn.client[0], "id.orig_p": conn.client[1],
                    "id.resp_h": conn.server[0], "id.resp_p": conn.server[1]})
    elif shape == "id_nested":
        raw["id"] = {"orig_h": conn.client[0], "orig_p": conn.client[1],
                     "resp_h": conn.server[0], "resp_p": conn.server[1]}
    else:
        raise ValueError(shape)
    if shape != "per_packet":
        raw["is_orig"] = rng.choice([is_orig, is_orig, "true" if is_orig else "false", 1 if is_orig else 0,
                                     "orig" if is_orig else "resp"])

    # pdu_reference, sometimes under one of upstream's aliases.
    raw[rng.choice(["pdu_reference", "pdu_reference", "pdu_ref", "pdu_ref_num"])] = pdu
    if rosctr is not None:
        raw[rng.choice(["rosctr_code", "rosctr_code", "rosctr"])] = rng.choice([rosctr, str(rosctr)])
    if function is not None:
        key = rng.choice(["function_code", "function_code", "function"])
        raw[key] = rng.choice([function, f"0x{function:02X}", str(function)])
    if name is not None:
        raw["function_name"] = name

    upstream = json.loads(json.dumps(raw))
    if shape == "id_underscore":  # upstream only knows the dotted spelling; same meaning
        for side in ("orig_h", "orig_p", "resp_h", "resp_p"):
            upstream[f"id.{side}"] = upstream.pop(f"id_{side}")
    if shape == "per_packet":  # upstream would ignore these; drop them so its input is its own shape
        for side in ("orig_h", "orig_p", "resp_h", "resp_p"):
            upstream.pop(f"id_{side}", None)
        upstream.pop("is_orig", None)
    return raw, upstream


def random_stream(rng: random.Random, connections: list[Connection], length: int) -> list[tuple]:
    """Mixed traffic: matched and unmatched responses, reused PDUs, name-only and missing codes."""
    events = []
    clock = {c.uid: 1_790_000_000.0 + i * 100 for i, c in enumerate(connections)}
    for _ in range(length):
        conn = rng.choice(connections)
        step = rng.choice([0.0005, 0.001, 0.01, 0.05, 0.2, 1.5])
        if rng.random() < 0.02:
            step = -0.3  # an out-of-order record: upstream's features never read time
        clock[conn.uid] = round(clock[conn.uid] + step, 6)
        shape = rng.choice(["per_packet", "id_underscore", "id_dotted", "id_nested"])
        if conn.outstanding and rng.random() < 0.45:
            # A response: usually to an outstanding request, sometimes to nothing.
            if rng.random() < 0.8:
                pdu, function = conn.outstanding.pop(rng.randrange(len(conn.outstanding)))
            else:
                pdu, function = rng.randrange(0, 65536), rng.choice(KNOWN_CODES)
            rosctr = rng.choice([3, 3, 3, 2, 7, None])
            events.append((conn, clock[conn.uid], False, pdu, rosctr, function, None, shape))
        else:
            if conn.outstanding and rng.random() < 0.1:
                pdu = rng.choice(conn.outstanding)[0]  # re-using a still-outstanding reference
            else:
                pdu = conn.next_pdu
                conn.next_pdu = (conn.next_pdu + rng.choice([1, 1, 1, 2, 7])) % 65536
            roll = rng.random()
            if roll < 0.75:
                function, name = rng.choice(KNOWN_CODES[:4] * 3 + KNOWN_CODES + UNKNOWN_CODES), None
            elif roll < 0.85:
                function, name = None, rng.choice(NAMES_WITHOUT_CODE + [""])
            elif roll < 0.9:
                function, name = None, None
            else:
                function, name = rng.choice(KNOWN_CODES), "ignored_when_a_code_is_present"
            rosctr = rng.choice([1, 1, 1, 7, None])
            conn.outstanding.append((pdu, function))
            events.append((conn, clock[conn.uid], True, pdu, rosctr, function, name, shape))
    return events


def catalogue_stream(connection: Connection) -> list[tuple]:
    """Every named and several unnamed function codes, each as a request/response pair."""
    events, ts = [], 1_790_100_000.0
    for function in KNOWN_CODES + UNKNOWN_CODES:
        pdu = connection.next_pdu
        connection.next_pdu += 1
        events.append((connection, ts, True, pdu, 1, function, None, "id_underscore"))
        events.append((connection, round(ts + 0.004, 6), False, pdu, 3, function, None, "id_underscore"))
        ts += 0.1
    for name in NAMES_WITHOUT_CODE + ["", None]:
        events.append((connection, ts, True, connection.next_pdu, 1, None, name, "per_packet"))
        connection.next_pdu += 1
        ts += 0.1
    return events


def flood_stream(connection: Connection) -> list[tuple]:
    """An unanswered request flood, then late responses to some of it."""
    events, ts = [], 1_790_200_000.0
    for i in range(160):
        events.append((connection, round(ts + i * 0.001, 6), True, i, 1, 0x04, None, "id_dotted"))
    for i in range(0, 160, 4):
        events.append((connection, round(ts + 1 + i * 0.001, 6), False, i, 3, 0x04, None, "id_dotted"))
    return events


def port_stream(connection: Connection, label: str) -> list[tuple]:
    """Traffic where neither or both endpoints use port 102."""
    events, ts = [], 1_790_300_000.0
    for i in range(20):
        events.append((connection, round(ts + i * 0.01, 6), i % 2 == 0, i // 2, 1 if i % 2 == 0 else 3,
                       0x04 if i % 3 else 0x05, None, "per_packet" if label == "neither" else "id_nested"))
    return events


def entropy_stream(rng: random.Random, connection: Connection) -> list[tuple]:
    """Requests whose function codes cycle through many different count distributions."""
    events, ts = [], 1_790_400_000.0
    palette = KNOWN_CODES + UNKNOWN_CODES
    for block in range(20):
        width = 1 + block % 8
        codes = rng.sample(palette, width)
        for i in range(12):
            function = codes[min(int(rng.expovariate(0.6)), width - 1)] if block % 2 else codes[i % width]
            events.append((connection, round(ts, 6), True, connection.next_pdu, 1, function, None, "id_nested"))
            connection.next_pdu += 1
            ts += 0.01
    return events


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    package_root = build_package(args.upstream)
    sys.path.insert(0, str(package_root))
    from s7zeek.adapters.kafka_source import normalize_zeek_message
    from s7zeek.features.customer_icsnpp_time_normalized_builder import (
        CustomerICSNPPTimeNormalizedFeatureBuilder,
    )

    streams: list[tuple[str, list[tuple]]] = []
    for seed in range(1, 9):
        rng = random.Random(seed)
        connections = [Connection(f"CS7S{seed:02d}U{k}", seed * 10 + k) for k in range(3)]
        streams.append((f"random-{seed}", random_stream(rng, connections, 200)))
    streams.append(("catalogue", catalogue_stream(Connection("CS7CATALOG", 90))))
    streams.append(("flood", flood_stream(Connection("CS7FLOOD", 91))))
    streams.append(("ports-neither-102", port_stream(Connection("CS7PORTN", 92, server_port=103), "neither")))
    streams.append(("ports-both-102", port_stream(Connection("CS7PORTB", 93, client_port=102), "both")))
    streams.append(("entropy", entropy_stream(random.Random(77), Connection("CS7ENTROPY", 94))))

    lines = [json.dumps({"meta": {
        "generator": "tests/fixtures/s7comm/generate_upstream_oracle.py",
        "python": sys.version.split()[0],
        "upstream_sha256": {name: sha for name, (_, sha) in UPSTREAM_FILES.items()},
        "features": FEATURES,
    }}, sort_keys=True)]
    shape_rng = random.Random(4242)
    for stream_name, events in streams:
        # One builder per stream: upstream state is keyed by uid, exactly as the
        # Java side keys it per (sensor, uid), and uids never repeat across streams.
        builder = CustomerICSNPPTimeNormalizedFeatureBuilder()
        for conn, ts, is_request, pdu, rosctr, function, name, shape in events:
            raw, upstream = render(shape_rng, conn, ts, is_request, pdu, rosctr, function, name, shape)
            row = builder.process_event(normalize_zeek_message(upstream))
            lines.append(json.dumps({
                "stream": stream_name,
                "raw": json.dumps(raw, sort_keys=True),
                "values": [float(row[f]) for f in NUMERIC],
                "rosctr": row["s7_rosctr"],
                "operation": row["s7_operation"],
            }, sort_keys=True))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    shutil.rmtree(package_root)
    print(f"wrote {len(lines) - 1} records from {len(streams)} streams to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
