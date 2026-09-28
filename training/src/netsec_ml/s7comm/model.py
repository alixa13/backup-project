"""The delivered S7 Stage 1 architecture and training loss (spec section 3), ported verbatim from
the model team's lstm_ae.py (LSTMAutoencoder) and debiased.py (WeightedLastTimestepMSE); below
them, the per-column weights and the runtime's last-event score."""
from __future__ import annotations

import numpy as np
import torch
from torch import nn

# ---- verbatim from the delivery (models/S7/src/s7zeek/modeling/lstm_ae.py, debiased.py) ----

class LSTMAutoencoder(nn.Module):
    def __init__(
        self,
        input_size: int,
        hidden_size: int = 32,
        latent_size: int = 16,
        num_layers: int = 1,
        dropout: float = 0.0,
    ) -> None:
        super().__init__()
        effective_dropout = float(dropout) if int(num_layers) > 1 else 0.0

        self.input_size = int(input_size)
        self.hidden_size = int(hidden_size)
        self.latent_size = int(latent_size)
        self.num_layers = int(num_layers)
        self.dropout = float(dropout)

        self.encoder = nn.LSTM(
            self.input_size,
            self.hidden_size,
            num_layers=self.num_layers,
            batch_first=True,
            dropout=effective_dropout,
        )
        self.to_latent = nn.Linear(self.hidden_size, self.latent_size)
        self.from_latent = nn.Linear(self.latent_size, self.hidden_size)
        self.decoder = nn.LSTM(
            self.hidden_size,
            self.hidden_size,
            num_layers=self.num_layers,
            batch_first=True,
            dropout=effective_dropout,
        )
        self.output = nn.Linear(self.hidden_size, self.input_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        encoded, _ = self.encoder(x)
        latent = self.to_latent(encoded[:, -1, :])
        seed = self.from_latent(latent).unsqueeze(1).repeat(1, x.size(1), 1)
        decoded, _ = self.decoder(seed)
        return self.output(decoded)


class WeightedLastTimestepMSE(nn.Module):
    """
    Production-aligned causal training objective.

    The sequence contains only past/current events and the loss is computed
    ONLY on the final/current event. This matches online scoring semantics:
    event t is judged from [t-L+1, ..., t], never from future events.

    Earlier timesteps remain context for the LSTM encoder but are not direct
    reconstruction targets.
    """

    def __init__(self, feature_weights: np.ndarray) -> None:
        super().__init__()
        weights = np.asarray(feature_weights, dtype=np.float32)
        if weights.ndim != 1:
            raise ValueError("feature_weights must be 1D")
        if not np.isfinite(weights).all() or np.any(weights < 0):
            raise ValueError("feature_weights must be finite and >= 0")
        if float(weights.sum()) <= 0:
            raise ValueError("At least one feature weight must be > 0")
        self.register_buffer(
            "weights",
            torch.as_tensor(weights, dtype=torch.float32).view(1, -1),
        )
        self.weight_sum = float(weights.sum())

    def forward(self, reconstruction: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        if reconstruction.ndim != 3 or target.ndim != 3:
            raise ValueError("Expected [batch, sequence, feature] tensors")
        sq = (reconstruction[:, -1, :] - target[:, -1, :]) ** 2
        per_event = (sq * self.weights).sum(dim=1) / self.weight_sum
        return per_event.mean()


# ---- end of the verbatim copy ----


def column_weights(names, features, weight) -> np.ndarray:
    """One weight per transformed column: `weight` for every column a listed raw feature produces
    (upstream transformed_columns_for_raw_features), 1 elsewhere."""
    w = np.ones(len(names), dtype=np.float32)
    for i, name in enumerate(names):
        for f in features:
            if name in (f"continuous__{f}", f"binary__{f}") or name.startswith(f"categorical__{f}_"):
                w[i] = weight
    return w


def last_event_scores(model, X, windows, weights, batch=2048) -> np.ndarray:
    """causal_shadow.py's score for each window, in float32 arithmetic as the runtime computes it:
    sum_j w_j (reconstruction_j - x_j)^2 / sum_j w_j over the window's last row."""
    w = torch.as_tensor(np.asarray(weights, dtype=np.float32)).view(1, -1)
    total = float(w.sum())
    out = np.empty(len(windows), dtype=np.float64)
    model.eval()
    with torch.no_grad():
        for start in range(0, len(windows), batch):
            x = torch.from_numpy(X[windows[start:start + batch]])
            rec = model(x)
            out[start:start + len(x)] = ((((rec[:, -1, :] - x[:, -1, :]) ** 2) * w).sum(dim=1) / total).numpy()
    return out
