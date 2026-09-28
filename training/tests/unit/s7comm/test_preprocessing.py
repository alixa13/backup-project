"""The delivered StableNumericTransformer recipe, ported: its contract shape, its rules, and
parity with the delivered code when models/S7 is present."""
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from netsec_ml.s7comm import preprocessing as P
from netsec_ml.s7comm.data import BINARY, CATEGORICAL, CONTINUOUS, FEATURES

UPSTREAM = Path(__file__).resolve().parents[4] / "models/S7/src/s7zeek/modeling/preprocessing.py"


def frame(n=200, seed=0):
    """A training-like frame: counts, ratios in [0, 1], binaries, two categories each."""
    rng = np.random.default_rng(seed)
    f = pd.DataFrame({name: rng.uniform(0, 1, n) for name in CONTINUOUS})
    f["s7_outstanding_requests"] = rng.integers(0, 3, n).astype(float)
    f["s7_same_function_run_length"] = rng.integers(1, 2000, n).astype(float)
    f["is_request_direction"] = rng.integers(0, 2, n).astype(float)
    f["s7_function_changed"] = rng.integers(0, 2, n).astype(float)
    f["s7_rosctr"] = rng.choice(["1", "3"], n)
    f["s7_operation"] = rng.choice(["READ_VAR", "WRITE_VAR"], n)
    return f[list(FEATURES)]


def test_the_contract_has_the_delivered_shape(tmp_path):
    pre = P.fit(frame())
    c = P.write_contract(pre, tmp_path / "preprocessor_contract.json")
    assert c["raw_feature_order"] == list(FEATURES)
    assert c["continuous"]["transformer"] == "StableNumericTransformer"
    assert c["binary"]["imputer_statistics"] == [0.0, 0.0]
    assert c["categorical"]["categories"] == {"s7_rosctr": ["1", "3"], "s7_operation": ["READ_VAR", "WRITE_VAR"]}
    assert c["transformed_dimension"] == 12 + 2 + 2 + 2
    assert c["transformed_feature_order"][14] == "categorical__s7_rosctr_1"
    assert json.loads((tmp_path / "preprocessor_contract.json").read_text()) == c


def test_bounded_features_are_clipped_and_never_scaled():
    pre = P.fit(frame())
    f = frame(1, seed=1)
    f["s7_request_ratio_16"] = 1.7
    out = P.transform(pre, f)
    assert out.dtype == np.float32
    assert out[0, CONTINUOUS.index("s7_request_ratio_16")] == 1.0


# Review Focus 3: a category the fit never saw one-hot encodes as all zeros.
def test_an_unseen_category_is_all_zeros():
    pre = P.fit(frame())
    f = frame(1, seed=2)
    f["s7_rosctr"] = "2"
    out = P.transform(pre, f)
    assert list(out[0, 14:16]) == [0.0, 0.0]


@pytest.mark.skipif(not UPSTREAM.exists(), reason="models/S7 (the delivery) is not present on this machine")
def test_the_port_matches_the_delivered_code():
    spec = importlib.util.spec_from_file_location("upstream_preprocessing", UPSTREAM)
    up = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(up)
    train = frame()
    ours = P.fit(train)
    theirs = up.build_preprocessor(list(CONTINUOUS), list(BINARY), list(CATEGORICAL), bounded_ranges=P.BOUNDED)
    theirs.fit(train)
    probe = frame(50, seed=3)
    assert np.array_equal(P.transform(ours, probe), np.asarray(theirs.transform(probe), np.float32))
