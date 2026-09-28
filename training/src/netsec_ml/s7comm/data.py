"""S7comm training data: the rows S7commFeatureExport writes, claimed by logical sources and
split by time (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md sections 4-6).
Nothing here computes a feature: every value comes from the platform's own Java code."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

# s7comm-feature-v1, in contract order.
FEATURES = (
    "s7_outstanding_requests", "s7_outstanding_mean_16", "s7_response_match_rate_16",
    "s7_same_function_run_length", "s7_same_direction_run_length", "s7_request_ratio_16",
    "s7_direction_change_rate_16", "s7_function_change_rate_16", "s7_function_entropy_16",
    "s7_function_transition_entropy_16", "s7_rosctr_change_rate_16",
    "s7_pdu_reference_unique_ratio_32", "is_request_direction", "s7_function_changed",
    "s7_rosctr", "s7_operation",
)
CONTINUOUS = FEATURES[:12]
BINARY = FEATURES[12:14]
CATEGORICAL = FEATURES[14:]

# S7commFeatureExport.COLUMNS, in order.
EXPORT_COLUMNS = (
    "capture", "uid", "ts", "event_id", "client_ip", "server_ip", "is_request", "function_code",
    "fresh", "rosctr", "operation", *(f"f{i}" for i in range(16)), "quality_flags",
)

# normal: trained, calibrated and tested; normal-test and unseen: tested only (unseen clients are
# the generalisation evidence); report: scored and reported, never gated.
ROLES = ("normal", "normal-test", "unseen", "report")


@dataclass(frozen=True)
class Source:
    """A logical source: some captures' records (only some clients' when `clients` is set)."""
    name: str
    captures: tuple[str, ...]
    clients: tuple[str, ...]
    role: str


def load_config(path) -> dict:
    """The run configuration (training/configs/s7comm-v2.yaml)."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def sources_of(config: dict) -> list[Source]:
    """The configured sources, in config order; an unknown role is refused."""
    out = []
    for name, spec in config["sources"].items():
        if spec["role"] not in ROLES:
            raise ValueError(f"source {name}: role {spec['role']!r} is not one of {ROLES}")
        out.append(Source(name, tuple(spec["captures"]), tuple(spec.get("clients") or ()), spec["role"]))
    return out


def load_export(path) -> pd.DataFrame:
    """One capture's export: the 14 numeric features under their contract names, the two
    categories as the strings the Java decoder wrote, and the operation code (ruling A5)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"feature export {path} is missing: run training/s7comm/acquire.sh first")
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    if tuple(raw.columns) != EXPORT_COLUMNS:
        raise ValueError(f"{path}: columns are not S7commFeatureExport's: {list(raw.columns)}")
    df = pd.DataFrame({
        "capture": raw["capture"], "uid": raw["uid"], "ts": raw["ts"].astype("float64"),
        "event_id": raw["event_id"], "client_ip": raw["client_ip"], "server_ip": raw["server_ip"],
        "fresh": raw["fresh"].astype("int64"), "quality_flags": raw["quality_flags"].astype("int64"),
    })
    for i, name in enumerate(FEATURES[:14]):
        df[name] = raw[f"f{i}"].astype("float64")
    df["s7_rosctr"] = raw["rosctr"]
    df["s7_operation"] = raw["operation"]
    df["operation_code"] = raw["f15"].astype("float64")
    return df


def load_sources(config: dict, sources: list[Source]) -> pd.DataFrame:
    """Every capture any source names, each row tagged with its source ('' when none claims it),
    its role, and its 0-based event index within its connection (export order)."""
    captures = sorted({c for s in sources for c in s.captures})
    data = pd.concat([load_export(Path(config["features_dir"]) / f"{c}.csv") for c in captures],
                     ignore_index=True)
    data["event_index"] = data.groupby(["capture", "uid"]).cumcount()
    data["source"] = ""
    for s in sources:  # the first source in config order that claims a row keeps it
        claim = (data["source"] == "") & data["capture"].isin(s.captures)
        if s.clients:
            claim &= data["client_ip"].isin(s.clients)
        data.loc[claim, "source"] = s.name
    data["role"] = data["source"].map({s.name: s.role for s in sources}).fillna("")
    return data


def split_by_time(data: pd.DataFrame, split: dict) -> pd.Series:
    """train / validation / test / gap per row. Each normal source is ordered by timestamp and cut
    70/15/15 (as configured) with a gap of min(gap_events, gap_fraction x size) rows between parts;
    every other role is test only; unclaimed rows stay ''."""
    part = pd.Series("", index=data.index, dtype=object)
    for _, rows in data[data["role"] == "normal"].groupby("source"):
        order = rows.sort_values(["ts", "capture", "event_index"], kind="stable").index
        n = len(order)
        gap = min(int(split["gap_events"]), int(split["gap_fraction"] * n))
        a = int(n * split["train"])
        b = int(n * (split["train"] + split["validation"]))
        part.loc[order[:a]] = "train"
        part.loc[order[a:a + gap]] = "gap"
        part.loc[order[a + gap:b]] = "validation"
        part.loc[order[b:b + gap]] = "gap"
        part.loc[order[b + gap:]] = "test"
    part[data["role"].isin(["normal-test", "unseen", "report"])] = "test"
    return part
