"""Windows per connection, the delivered architecture and loss, and a deterministic training loop."""
import numpy as np
import pandas as pd
import torch

from netsec_ml.s7comm import model as M
from netsec_ml.s7comm import sequences as S
from netsec_ml.s7comm import train as T


def test_the_model_reconstructs_a_window_of_the_same_shape():
    assert M.LSTMAutoencoder(21, 32, 16)(torch.zeros(2, 16, 21)).shape == (2, 16, 21)


def test_column_weights_zero_every_column_of_a_listed_feature():
    names = ["continuous__a", "binary__b", "categorical__s7_operation_READ_VAR", "categorical__s7_operation_X"]
    assert list(M.column_weights(names, ["s7_operation"], 0.0)) == [1.0, 1.0, 0.0, 0.0]


def test_windows_follow_their_connection_and_the_stride():
    data = pd.DataFrame({"capture": ["c"] * 40, "uid": ["u"] * 40, "event_index": range(40)})
    windows = S.window_rows(S.connection_rows(data), 16, 8, np.ones(40, bool))
    assert [w[-1] for w in windows] == [15, 23, 31, 39]
    assert all((np.diff(w) == 1).all() for w in windows)


# Review Focus 1: a training window never mixes train rows with validation rows.
def test_a_training_window_never_mixes_parts():
    data = pd.DataFrame({"capture": ["c"] * 40, "uid": ["u"] * 40, "event_index": range(40)})
    keep = np.arange(40) < 30
    windows = S.window_rows(S.connection_rows(data), 16, 1, keep)
    assert len(windows) == 15 and windows.max() == 29


def test_sampling_weights_balance_sources_and_raise_writes():
    sources = np.array(["A"] * 90 + ["B"] * 10)
    w = S.sampling_weights(sources, np.zeros(100, bool), "equal", 0.1)
    assert np.isclose(w[sources == "A"].sum(), 0.5) and np.isclose(w[sources == "B"].sum(), 0.5)
    writes = np.zeros(100, bool)
    writes[:2] = True
    w = S.sampling_weights(np.array(["A"] * 100), writes, "proportional", 0.1)
    assert np.isclose(w[writes].sum(), 0.1)


def periodic(n=600):
    """A learnable stream: one connection, 21 columns sharing one 12-event period (each with its
    own phase), so validation windows repeat patterns the training windows hold."""
    t = np.arange(n, dtype=np.float32)
    X = np.stack([np.sin(2 * np.pi * t / 12 + k) for k in range(21)], axis=1).astype(np.float32)
    data = pd.DataFrame({"capture": ["c"] * n, "uid": ["u"] * n, "event_index": range(n)})
    return X, S.connection_rows(data)


def test_training_lowers_the_loss_and_is_deterministic():
    X, conns = periodic()
    train_w = S.window_rows(conns, 16, 2, np.arange(len(X)) < 450)
    val_w = S.window_rows(conns, 16, 1, np.arange(len(X)) >= 470)
    ones = np.ones(21, np.float32)
    # A small model with a high rate: 14 steps an epoch, so five epochs clearly learn.
    cfg = T.TrainConfig(hidden=16, latent=8, batch=16, lr=1e-2, max_epochs=5, patience=10, seed=7)
    weights = np.full(len(train_w), 1.0 / len(train_w))
    _, h1 = T.train_model(X, train_w, weights, val_w, ones, ones, cfg)
    _, h2 = T.train_model(X, train_w, weights, val_w, ones, ones, cfg)
    assert h1[-1]["val_score"] < h1[0]["val_score"]
    assert [e["val_score"] for e in h1] == [e["val_score"] for e in h2]
