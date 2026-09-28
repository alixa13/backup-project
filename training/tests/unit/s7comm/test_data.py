"""Loading S7commFeatureExport rows, claiming them for sources, and splitting by time."""
import numpy as np
import pytest

from netsec_ml.s7comm import data as D

HEADER = ",".join(D.EXPORT_COLUMNS)


def row(capture, uid, ts, client="10.0.0.1", request=1, rosctr="1", operation="READ_VAR", code="4", fresh=0):
    """One export row as S7commFeatureExport writes it; values zero except the ones the
    categories and direction imply."""
    values = [0.0] * 16
    values[12] = float(request)
    values[14] = float(rosctr) if rosctr.isdigit() else -1.0
    values[15] = float(code) if code else -1.0
    fields = [capture, uid, f"{ts:.6f}", f"s:{uid}:{ts}", client, "10.0.0.9", str(request), code, str(fresh),
              rosctr, operation, *[repr(v) for v in values], "0"]
    return ",".join(fields)


def write(directory, capture, rows):
    (directory / f"{capture}.csv").write_text(HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def config(directory, sources):
    return {"features_dir": str(directory), "sources": sources,
            "split": {"train": 0.70, "validation": 0.15, "gap_events": 64, "gap_fraction": 0.01}}


def test_an_export_loads_under_contract_names(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0), row("c1", "u1", 2.0, request=0, rosctr="3")])
    df = D.load_export(tmp_path / "c1.csv")
    assert list(df[list(D.FEATURES[:14])].dtypes.unique()) == [np.dtype("float64")]
    assert list(df["s7_operation"]) == ["READ_VAR", "READ_VAR"]
    assert list(df["s7_rosctr"]) == ["1", "3"]
    assert list(df["operation_code"]) == [4.0, 4.0]


def test_a_file_with_other_columns_is_refused(tmp_path):
    (tmp_path / "c1.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="S7commFeatureExport"):
        D.load_export(tmp_path / "c1.csv")


# Review Focus 2: a named capture with no export fails, naming the file.
def test_a_missing_export_is_named(tmp_path):
    cfg = config(tmp_path, {"s": {"captures": ["nowhere"], "role": "normal"}})
    with pytest.raises(FileNotFoundError, match="nowhere.csv"):
        D.load_sources(cfg, D.sources_of(cfg))


# Review Focus 5: an ACK (no function code, operation __MISSING__) loads with no NaN.
def test_an_ack_row_loads_without_nan(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, request=0, rosctr="2", operation="__MISSING__", code="")])
    df = D.load_export(tmp_path / "c1.csv")
    assert not df[list(D.FEATURES[:14])].isna().any().any()
    assert df["s7_operation"][0] == "__MISSING__"


def test_sources_claim_rows_by_capture_and_client_and_the_first_wins(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, client="A"), row("c1", "u2", 1.5, client="B")])
    cfg = config(tmp_path, {"hmi": {"captures": ["c1"], "clients": ["A"], "role": "normal"},
                            "rest": {"captures": ["c1"], "role": "report"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    assert dict(zip(data["client_ip"], data["source"])) == {"A": "hmi", "B": "rest"}
    assert dict(zip(data["source"], data["role"])) == {"hmi": "normal", "rest": "report"}


def test_unclaimed_rows_have_no_source_or_role(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, client="A"), row("c1", "u2", 1.5, client="Z")])
    cfg = config(tmp_path, {"hmi": {"captures": ["c1"], "clients": ["A"], "role": "normal"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    unclaimed = data[data["client_ip"] == "Z"]
    assert list(unclaimed["source"]) == [""] and list(unclaimed["role"]) == [""]


def test_the_event_index_counts_within_each_connection(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0), row("c1", "u2", 1.1), row("c1", "u1", 1.2)])
    cfg = config(tmp_path, {"s": {"captures": ["c1"], "role": "normal"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    assert list(zip(data["uid"], data["event_index"])) == [("u1", 0), ("u2", 0), ("u1", 1)]


def test_a_normal_source_is_split_by_time_with_gaps(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", float(i)) for i in range(1000)])
    cfg = config(tmp_path, {"s": {"captures": ["c1"], "role": "normal"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    part = D.split_by_time(data, cfg["split"])
    # gap = min(64, 1% of 1000) = 10
    assert part.value_counts().to_dict() == {"train": 700, "validation": 140, "test": 140, "gap": 20}
    ts = data["ts"]
    assert ts[part == "train"].max() < ts[part == "validation"].min() < ts[part == "test"].min()


def test_other_roles_are_test_only(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", float(i)) for i in range(20)])
    cfg = config(tmp_path, {"s": {"captures": ["c1"], "role": "unseen"}})
    data = D.load_sources(cfg, D.sources_of(cfg))
    assert set(D.split_by_time(data, cfg["split"])) == {"test"}


def test_an_unknown_role_is_refused(tmp_path):
    with pytest.raises(ValueError, match="role"):
        D.sources_of(config(tmp_path, {"s": {"captures": ["c1"], "role": "training"}}))


# Final review I1: a configured source that claims nothing (a mistyped client, a wrong capture)
# must stop the run by name, never train or gate on less.
def test_a_source_that_claims_no_rows_is_named(tmp_path):
    write(tmp_path, "c1", [row("c1", "u1", 1.0, client="A")])
    cfg = config(tmp_path, {"hmi": {"captures": ["c1"], "clients": ["10.0.0.99"], "role": "normal"}})
    with pytest.raises(ValueError, match="hmi claims no rows"):
        D.load_sources(cfg, D.sources_of(cfg))
