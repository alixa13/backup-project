"""The delivered training recipe (spec section 6): Adam, weighted sampling, the last-event loss,
early stopping on the validation score, deterministic under its seed."""
from __future__ import annotations

import copy
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, WeightedRandomSampler

from netsec_ml.s7comm.model import LSTMAutoencoder, WeightedLastTimestepMSE, last_event_scores
from netsec_ml.s7comm.sequences import WindowSet


@dataclass(frozen=True)
class TrainConfig:
    hidden: int = 32
    latent: int = 16
    batch: int = 256
    lr: float = 1e-3
    weight_decay: float = 1e-5
    clip: float = 1.0
    max_epochs: int = 40
    patience: int = 7
    seed: int = 42


def train_model(X, train_windows, sample_weights, val_windows, loss_weights, score_weights, cfg):
    """Train, keep the epoch with the lowest mean validation score, and return (model, history)."""
    if len(train_windows) == 0 or len(val_windows) == 0:
        raise ValueError("training needs training and validation windows")
    torch.manual_seed(cfg.seed)
    np.random.seed(cfg.seed)
    model = LSTMAutoencoder(X.shape[1], cfg.hidden, cfg.latent)
    loss_fn = WeightedLastTimestepMSE(loss_weights)
    optimiser = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    sampler = WeightedRandomSampler(torch.as_tensor(sample_weights, dtype=torch.float64),
                                    num_samples=len(train_windows), replacement=True,
                                    generator=torch.Generator().manual_seed(cfg.seed))
    loader = DataLoader(WindowSet(X, train_windows), batch_size=cfg.batch, sampler=sampler)
    best, best_state, stale, history = float("inf"), None, 0, []
    for epoch in range(cfg.max_epochs):
        model.train()
        total, seen = 0.0, 0
        for x in loader:
            optimiser.zero_grad()
            loss = loss_fn(model(x), x)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), cfg.clip)
            optimiser.step()
            total += float(loss) * len(x)
            seen += len(x)
        val = float(np.mean(last_event_scores(model, X, val_windows, score_weights)))
        history.append({"epoch": epoch + 1, "train_loss": total / seen, "val_score": val})
        if val < best:
            best, best_state, stale = val, copy.deepcopy(model.state_dict()), 0
        else:
            stale += 1
            if stale >= cfg.patience:
                break
    model.load_state_dict(best_state)
    model.eval()
    return model, history
