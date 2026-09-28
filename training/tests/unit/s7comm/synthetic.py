"""Synthetic S7commFeatureExport files for the pipeline test: steady read polling, one connection per
capture, with small noise. Shaped like the real export; not real traffic, and never used to train
anything but the test's throwaway model."""
import numpy as np

from netsec_ml.s7comm.data import EXPORT_COLUMNS


def write_polling(path, capture, client, events, seed, start=1.6e9):
    """`events` alternating READ_VAR requests and responses on one connection, 0.1 s apart."""
    rng = np.random.default_rng(seed)
    lines = [",".join(EXPORT_COLUMNS)]
    for i in range(events):
        request = i % 2 == 0
        values = [float(request), 0.5, 1.0, 1.0 + i, 1.0, 0.5, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0625,
                  float(request), 0.0, 1.0 if request else 3.0, 4.0]
        values[1] += rng.normal(0, 0.01)
        ts = start + 0.1 * i
        lines.append(",".join([capture, f"U{capture}", f"{ts:.6f}", f"s:U{capture}:{i}", client, "10.9.9.9",
                               str(int(request)), "4", str(int(i == 0)), "1" if request else "3", "READ_VAR",
                               *(repr(v) for v in values), "0"]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
