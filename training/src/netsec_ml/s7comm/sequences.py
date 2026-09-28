"""16-event windows per connection, and the per-window sampling weights of the delivered recipe."""
from __future__ import annotations

import numpy as np
import torch
from numpy.lib.stride_tricks import sliding_window_view
from torch.utils.data import Dataset


def connection_rows(data) -> list[np.ndarray]:
    """Each connection's row positions (0..len(data)-1), in event order."""
    frame = data[["capture", "uid", "event_index"]].assign(_pos=np.arange(len(data)))
    frame = frame.sort_values(["capture", "uid", "event_index"], kind="stable")
    return [g["_pos"].to_numpy() for _, g in frame.groupby(["capture", "uid"], sort=False)]


def window_rows(connections, length, stride, keep) -> np.ndarray:
    """[m, length] row positions: every stride-th window of consecutive events of one connection,
    kept only when all its rows satisfy `keep`, so a training window never mixes parts."""
    out = []
    for rows in connections:
        if len(rows) < length:
            continue
        windows = sliding_window_view(rows, length)[::stride]
        ok = keep[windows].all(axis=1)
        if ok.any():
            out.append(windows[ok])
    return np.concatenate(out).astype(np.int64) if out else np.empty((0, length), dtype=np.int64)


def sampling_weights(sources, is_write, balance, write_fraction) -> np.ndarray:
    """One weight per training window, summing to 1: equal total per source ('equal') or one per
    window ('proportional'); windows ending in a write request are then raised to write_fraction
    of the total when rarer (the delivered recipe's write handling)."""
    sources = np.asarray(sources)
    is_write = np.asarray(is_write, dtype=bool)
    w = np.ones(len(sources), dtype=np.float64)
    if balance == "equal":
        for s in np.unique(sources):
            mask = sources == s
            w[mask] = 1.0 / mask.sum()
    elif balance != "proportional":
        raise ValueError(f"balance must be 'equal' or 'proportional', was {balance!r}")
    w /= w.sum()
    q = w[is_write].sum()
    if 0 < q < write_fraction:
        w[is_write] *= write_fraction * (1 - q) / (q * (1 - write_fraction))
        w /= w.sum()
    return w


class WindowSet(Dataset):
    """The windows as model inputs: X rows gathered per window, float32."""

    def __init__(self, X, windows):
        self.X = X
        self.windows = windows

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, i):
        return torch.from_numpy(self.X[self.windows[i]])
