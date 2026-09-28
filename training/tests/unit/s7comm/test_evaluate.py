"""G1's arithmetic: rates past each connection's 64th event, per source, and the failures it names."""
import numpy as np
import pandas as pd

from netsec_ml.s7comm import evaluate as E


def data(n=100):
    return pd.DataFrame({"source": ["a"] * n, "event_index": np.arange(n)})


def test_rates_count_only_scored_rows_and_split_at_the_64th_event():
    verdicts = np.array(["WARMUP"] * 15 + ["ANOMALY"] * 49 + ["NORMAL"] * 35 + ["ANOMALY"])
    table = E.rate_table(data(), verdicts, np.ones(100, bool))
    row = table.iloc[0]
    assert (row.source, row.scored, row.past_64) == ("a", 85, 36)
    assert np.isclose(row.normal_rate_past_64, 35 / 36)
    assert row.normal_rate_16_to_64 == 0.0


def test_g1_names_a_source_below_the_threshold():
    table = pd.DataFrame([{"source": "a", "scored": 10, "past_64": 100, "normal_rate_past_64": 0.98,
                           "normal_rate_16_to_64": 1.0}])
    failures = E.gate_g1(table, 0.99)
    assert len(failures) == 1 and failures[0].startswith("a: 98.00% NORMAL")
    assert E.gate_g1(table, 0.98) == []


def test_g1_fails_a_source_with_nothing_past_the_64th_event():
    table = pd.DataFrame([{"source": "a", "scored": 10, "past_64": 0, "normal_rate_past_64": np.nan,
                           "normal_rate_16_to_64": 1.0}])
    assert E.gate_g1(table, 0.99) == ["a: no scored event past the 64th in its gated rows"]


def test_the_card_shows_every_gated_source_and_the_gates():
    summary = {"release_id": "r1", "gates": {"G1": {"passed": True, "threshold": 0.99, "failures": []},
                                             "G3": {"passed": True, "max_abs_difference": 1e-7, "threshold": 1e-4}},
               "g1": [{"source": "qut-control", "scored": 5, "past_64": 4, "normal_rate_past_64": 1.0,
                       "normal_rate_16_to_64": 1.0}],
               "unseen": [], "report": [], "groups": [], "leave_one_source_out": {},
               "selection": {"chosen": "c1", "candidates": []}, "sources": {}, "inference_us_per_window": 1.0}
    card = E.render_card(summary)
    assert "| qut-control | 5 | 4 | 100.00% | 100.00% |" in card
    assert "G1" in card and "PASSED" in card
