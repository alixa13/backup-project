"""The false-alarm gate G1 and the reported rates (spec section 7), from one verdict per row, and
the model card rendered from the run's evaluation summary (every number comes from that file)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from netsec_ml.s7comm.calibrate import GROUPS

# G1 counts events after the connection's 64th (event_index is 0-based).
PAST = 64
RATE_COLUMNS = ["source", "scored", "past_64", "normal_rate_past_64", "normal_rate_16_to_64"]


def _share(verdicts) -> float:
    """NORMAL's share of the verdicts, NaN when there are none."""
    return float(np.mean(verdicts == "NORMAL")) if len(verdicts) else float("nan")


def rate_table(data, verdicts, mask) -> pd.DataFrame:
    """Per source, over the scored rows in `mask`: how many, how many past the 64th event, and the
    NORMAL share past the 64th event and from the 16th to the 64th."""
    frame = pd.DataFrame({"source": data["source"].to_numpy(), "late": data["event_index"].to_numpy() >= PAST,
                          "verdict": np.asarray(verdicts, dtype=object)})[np.asarray(mask, dtype=bool)]
    frame = frame[frame["verdict"] != "WARMUP"]
    rows = []
    for source, g in frame.groupby("source", sort=True):
        late = g.loc[g["late"], "verdict"].to_numpy()
        rows.append({"source": source, "scored": int(len(g)), "past_64": int(len(late)),
                     "normal_rate_past_64": _share(late),
                     "normal_rate_16_to_64": _share(g.loc[~g["late"], "verdict"].to_numpy())})
    return pd.DataFrame(rows, columns=RATE_COLUMNS)


def group_table(groups, verdicts, mask, late) -> pd.DataFrame:
    """Per score group, over the scored rows in `mask` past the 64th event: count and NORMAL share."""
    keep = np.asarray(mask, dtype=bool) & np.asarray(late, dtype=bool) & (np.asarray(verdicts) != "WARMUP")
    g, v = np.asarray(groups)[keep], np.asarray(verdicts)[keep]
    return pd.DataFrame([{"group": name, "scored": int((g == name).sum()), "normal_rate": _share(v[g == name])}
                         for name in GROUPS])


def gate_g1(table, threshold, gated_sources=()) -> list[str]:
    """G1's failures, named: a gated source below the threshold past the 64th event, or with no
    scored event there at all -- including one absent from the table. Empty means G1 holds."""
    missing = [s for s in gated_sources if s not in set(table["source"])]
    failures = [f"{s}: no scored event past the 64th in its gated rows" for s in missing]
    for r in table.itertuples():
        if r.past_64 == 0:
            failures.append(f"{r.source}: no scored event past the 64th in its gated rows")
        elif r.normal_rate_past_64 < threshold:
            failures.append(f"{r.source}: {r.normal_rate_past_64:.2%} NORMAL past the 64th event, "
                            f"below {threshold:.2%}")
    return failures


def _pct(x) -> str:
    """A rate as a percentage, or a dash when undefined."""
    return "-" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.2%}"


def _rates(rows) -> list[str]:
    """A markdown table of rate_table rows."""
    out = ["| source | scored | past 64th | NORMAL past 64th | NORMAL 16th-64th |", "|---|---|---|---|---|"]
    out += [f"| {r['source']} | {r['scored']} | {r['past_64']} | {_pct(r['normal_rate_past_64'])} | "
            f"{_pct(r['normal_rate_16_to_64'])} |" for r in rows]
    return out


def render_card(summary) -> str:
    """The model card: gates, per-source rates, selection and data, all from the summary."""
    g1, g3 = summary["gates"]["G1"], summary["gates"]["G3"]
    lines = [f"# S7comm Stage 1 detector {summary['release_id']}", "",
             "Generated from `evaluation_summary.json` by `netsec_ml.s7comm.evaluate.render_card`.", "",
             "## Gates", "",
             f"- **G1** ({g1['threshold']:.0%} NORMAL past each connection's 64th event, every normal "
             f"source's held-out rows): {'PASSED' if g1['passed'] else 'FAILED'}",
             *[f"  - {f}" for f in g1["failures"]],
             f"- **G3** (ONNX vs PyTorch within {g3['threshold']:g}): {'PASSED' if g3['passed'] else 'FAILED'}, "
             f"max difference {g3['max_abs_difference']:.3g}", "",
             "## Held-out normal traffic (gated)", "", *_rates(summary["g1"]), "",
             "## Unseen clients (reported, not gated)", "", *_rates(summary["unseen"]), "",
             "## Engineering and other sessions (reported, not gated)", "", *_rates(summary["report"]), "",
             "## Per score group, gated rows past the 64th event", "",
             "| group | scored | NORMAL |", "|---|---|---|",
             *[f"| {r['group']} | {r['scored']} | {_pct(r['normal_rate'])} |" for r in summary["groups"]], "",
             "## Selection", "", f"Chosen candidate: `{summary['selection']['chosen']}`.", "",
             "| candidate | mean leave-one-source-out NORMAL | per held-out source |", "|---|---|---|",
             *[f"| {c['name']} | {_pct(c['objective'])} | "
               + ", ".join(f"{s} {_pct(v)}" for s, v in c["leave_one_source_out"].items()) + " |"
               for c in summary["selection"]["candidates"]], "",
             "## Data", "", "| source | role | captures | rows by part |", "|---|---|---|---|",
             *[f"| {name} | {s['role']} | {', '.join(s['captures'])} | "
               + ", ".join(f"{k} {v}" for k, v in s["rows"].items()) + " |" for name, s in summary["sources"].items()],
             "", f"Inference: {summary['inference_us_per_window']:.0f} us per window (ONNX Runtime, batch 1, one thread).",
             ""]
    return "\n".join(lines)
