"""Online scoring and the group-conditional conformal decision (spec section 3; upstream
operation_groups, conformal_pvalues and apply_group_conformal_policy), producing exactly what the
Java S7commConformalPolicy reads back from the release."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from netsec_ml.s7comm.model import last_event_scores
from netsec_ml.s7comm.sequences import window_rows

GROUPS = ("RESPONSE", "READ_REQUEST", "WRITE_REQUEST", "OTHER_REQUEST")


def score_groups(is_request_direction, operation_code) -> np.ndarray:
    """RESPONSE for a response; else READ_REQUEST (code 4), WRITE_REQUEST (5) or OTHER_REQUEST,
    read from the vector as the Java scorer reads it (ruling A5)."""
    request = np.asarray(is_request_direction) == 1
    code = np.asarray(operation_code)
    out = np.full(len(request), "RESPONSE", dtype=object)
    out[request] = "OTHER_REQUEST"
    out[request & (code == 4)] = "READ_REQUEST"
    out[request & (code == 5)] = "WRITE_REQUEST"
    return out


def stream_scores(model, X, connections, weights, rows_mask=None) -> np.ndarray:
    """The online rule, stride 1: a score for every row with 15 earlier rows in its connection,
    NaN during the warm-up. rows_mask (whole connections) limits which rows are scored."""
    keep = np.ones(len(X), dtype=bool) if rows_mask is None else np.asarray(rows_mask, dtype=bool)
    windows = window_rows(connections, 16, 1, keep)
    scores = np.full(len(X), np.nan)
    if len(windows):
        scores[windows[:, -1]] = last_event_scores(model, X, windows, weights)
    return scores


@dataclass
class Policy:
    alpha_by_group: dict
    fallback_alpha: float
    calibration: dict


def calibrate(scores, groups, alpha, fallback_alpha, min_calibration) -> Policy:
    """Each group's finite validation scores, sorted. A group with at least min_calibration scores
    takes its configured alpha (else the fallback); a smaller group the smallest p-value it can
    give, 1/(n+1); a group with none gets no alpha and is judged on everything pooled."""
    scores = np.asarray(scores, dtype=np.float64)
    groups = np.asarray(groups)
    calibration, alpha_by_group = {}, {}
    for g in GROUPS:
        s = np.sort(scores[(groups == g) & np.isfinite(scores)])
        calibration[g] = s
        if len(s) == 0:
            continue
        alpha_by_group[g] = float(alpha.get(g, fallback_alpha)) if len(s) >= min_calibration else 1.0 / (len(s) + 1)
    return Policy(alpha_by_group, float(fallback_alpha), calibration)


def _reference(policy, group):
    """A group's calibration scores and alpha, or everything pooled with the fallback alpha."""
    own = policy.calibration.get(group, np.empty(0))
    if len(own):
        return own, policy.alpha_by_group[group]
    pooled = np.sort(np.concatenate([policy.calibration[g] for g in GROUPS]))
    return pooled, policy.fallback_alpha


def p_values(policy, scores, groups) -> np.ndarray:
    """(#calibration >= score + 1) / (n + 1) per row; NaN for a non-finite score."""
    scores = np.asarray(scores, dtype=np.float64)
    groups = np.asarray(groups)
    out = np.full(len(scores), np.nan)
    for g in GROUPS:
        rows = (groups == g) & np.isfinite(scores)
        if rows.any():
            ref, _ = _reference(policy, g)
            at_least = len(ref) - np.searchsorted(ref, scores[rows], side="left")
            out[rows] = (at_least + 1.0) / (len(ref) + 1.0)
    return out


def decide(policy, scores, groups) -> np.ndarray:
    """WARMUP where there is no score, ANOMALY where p <= the group's alpha, NORMAL otherwise."""
    groups = np.asarray(groups)
    p = p_values(policy, scores, groups)
    out = np.full(len(p), "WARMUP", dtype=object)
    for g in GROUPS:
        rows = (groups == g) & np.isfinite(p)
        _, a = _reference(policy, g)
        out[rows] = np.where(p[rows] <= a, "ANOMALY", "NORMAL")
    return out
