"""The score groups, the online warm-up, and the group-conditional conformal decision, pinned to the
same hand-worked cases as the Java S7commConformalPolicy."""
import numpy as np
import pandas as pd

from netsec_ml.s7comm import calibrate as C
from netsec_ml.s7comm.model import LSTMAutoencoder
from netsec_ml.s7comm.sequences import connection_rows


def test_groups_follow_direction_and_operation():
    groups = C.score_groups(np.array([0, 1, 1, 1, 1]), np.array([4, 4, 5, 240, -1]))
    assert list(groups) == ["RESPONSE", "READ_REQUEST", "WRITE_REQUEST", "OTHER_REQUEST", "OTHER_REQUEST"]


def policy():
    scores = np.array([0.4, 0.1, 0.3, 0.2] + [0.05])
    groups = np.array(["RESPONSE"] * 4 + ["READ_REQUEST"])
    return C.calibrate(scores, groups, {"RESPONSE": 0.2}, 0.001, min_calibration=3)


def test_p_values_count_calibration_scores_at_least_the_score():
    p = C.p_values(policy(), np.array([0.35, 0.3, 0.5, 0.0]), np.array(["RESPONSE"] * 4))
    assert np.allclose(p, [0.4, 0.6, 0.2, 1.0])


def test_a_small_group_gets_the_smallest_attainable_alpha():
    pol = policy()
    assert pol.alpha_by_group["RESPONSE"] == 0.2
    assert pol.alpha_by_group["READ_REQUEST"] == 0.5  # 1 score < min_calibration: 1/(1+1)


# Review Focus 4: a group with no calibration scores uses every score pooled and the fallback alpha.
def test_a_group_without_scores_uses_the_pooled_fallback():
    pol = policy()
    assert "OTHER_REQUEST" not in pol.alpha_by_group
    p = C.p_values(pol, np.array([1.0]), np.array(["OTHER_REQUEST"]))
    assert np.isclose(p[0], 1 / 6)  # none of the 5 pooled scores is >= 1.0
    assert list(C.decide(pol, np.array([1.0]), np.array(["OTHER_REQUEST"]))) == ["NORMAL"]  # 1/6 > 0.001


def test_decide_marks_warmup_normal_and_anomaly():
    verdicts = C.decide(policy(), np.array([np.nan, 0.0, 0.5]), np.array(["RESPONSE"] * 3))
    assert list(verdicts) == ["WARMUP", "NORMAL", "ANOMALY"]


def test_stream_scores_start_at_each_connections_sixteenth_event():
    data = pd.DataFrame({"capture": ["c"] * 20, "uid": ["u"] * 20, "event_index": range(20)})
    X = np.zeros((20, 21), np.float32)
    scores = C.stream_scores(LSTMAutoencoder(21, 8, 4), X, connection_rows(data), np.ones(21, np.float32))
    assert np.isnan(scores[:15]).all() and np.isfinite(scores[15:]).all()
