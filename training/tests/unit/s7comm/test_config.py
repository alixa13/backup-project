"""The shipped run configuration against the failure run-1 exposed (2026-09-28): with a
last-event-only loss, a training stride that shares a factor with a poller's request/response
period makes every training window end on the same direction, so the model never learns the
other one (stride 8 gave 0% request-ending windows on every alternating source)."""
from pathlib import Path

import numpy as np
import pandas as pd

from netsec_ml.s7comm import data as D
from netsec_ml.s7comm.sequences import connection_rows, window_rows

CONFIG = Path(__file__).resolve().parents[3] / "configs" / "s7comm-v2.yaml"


def test_training_windows_end_on_both_directions_of_an_alternating_poller():
    cfg = D.load_config(CONFIG)
    # One strictly alternating connection (request, response, ...), and a pipelined one
    # (request, request, response, response, ...), as the real sources poll.
    for pattern in ([1, 0], [1, 1, 0, 0]):
        n = 400
        request = np.resize(np.array(pattern), n) == 1
        data = pd.DataFrame({"capture": ["c"] * n, "uid": ["u"] * n, "event_index": range(n)})
        windows = window_rows(connection_rows(data), cfg["model"]["sequence"], cfg["model"]["train_stride"],
                              np.ones(n, bool))
        share = request[windows[:, -1]].mean()
        assert 0.4 <= share <= 0.6, f"pattern {pattern}: {share:.0%} of training windows end on a request"
