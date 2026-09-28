"""The delivered S7 Stage 1 preprocessing recipe (spec section 3), ported verbatim from the
model team's models/S7/src/s7zeek/modeling/preprocessing.py (sha256 63d4c7ec...0891; ruling A6):
StableNumericTransformer, build_preprocessor, export_preprocessor_contract. Below them, the
platform's three entry points."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from netsec_ml.s7comm.data import BINARY, CATEGORICAL, CONTINUOUS, FEATURES

# ---- verbatim from the delivery (models/S7/src/s7zeek/modeling/preprocessing.py) ----

class StableNumericTransformer(BaseEstimator, TransformerMixin):
    """
    Numerically stable transformer for continuous network features.

    Rules:
    - Explicitly bounded ratios stay in their physical range and are not
      divided by a tiny train-only robust scale.
    - Other features use median centering and a robust quantile span.
    - The scale has a configurable lower floor.
    - Final transformed values are clipped to a finite deployment-safe range.

    Every fitted parameter is exportable to JSON so the exact transform can
    later be reproduced in Flink/Java rather than relying on Python at runtime.
    """

    def __init__(
        self,
        feature_names: tuple[str, ...],
        quantile_range: tuple[float, float] = (5.0, 95.0),
        bounded_ranges: dict[str, tuple[float, float]] | None = None,
        min_scale: float = 0.1,
        transformed_clip: float = 20.0,
    ) -> None:
        self.feature_names = feature_names
        self.quantile_range = quantile_range
        self.bounded_ranges = bounded_ranges
        self.min_scale = min_scale
        self.transformed_clip = transformed_clip

    def fit(self, X, y=None):
        x = np.asarray(X, dtype=np.float64)
        if x.ndim != 2:
            raise ValueError(f"Expected 2D numeric input, got shape={x.shape}")
        if x.shape[1] != len(self.feature_names):
            raise ValueError(
                f"Feature count mismatch: matrix={x.shape[1]} names={len(self.feature_names)}"
            )
        if not (0.0 <= self.quantile_range[0] < self.quantile_range[1] <= 100.0):
            raise ValueError(f"Invalid quantile_range={self.quantile_range}")
        if self.min_scale <= 0:
            raise ValueError("min_scale must be > 0")
        if self.transformed_clip <= 0:
            raise ValueError("transformed_clip must be > 0")

        bounded = self.bounded_ranges or {}
        n = x.shape[1]
        center = np.zeros(n, dtype=np.float64)
        scale = np.ones(n, dtype=np.float64)
        bounded_low = np.full(n, np.nan, dtype=np.float64)
        bounded_high = np.full(n, np.nan, dtype=np.float64)
        is_bounded = np.zeros(n, dtype=bool)

        q_low, q_high = self.quantile_range
        for j, name in enumerate(self.feature_names):
            col = x[:, j]
            finite = col[np.isfinite(col)]
            if len(finite) == 0:
                # The imputer should normally prevent this, but the safe
                # identity fallback keeps artifact creation deterministic.
                center[j] = 0.0
                scale[j] = 1.0
                continue

            if name in bounded:
                low, high = bounded[name]
                low = float(low)
                high = float(high)
                if not np.isfinite(low) or not np.isfinite(high) or high <= low:
                    raise ValueError(f"Invalid bounded range for {name}: {(low, high)}")
                is_bounded[j] = True
                bounded_low[j] = low
                bounded_high[j] = high
                center[j] = 0.0
                scale[j] = 1.0
            else:
                center[j] = float(np.median(finite))
                lo = float(np.percentile(finite, q_low))
                hi = float(np.percentile(finite, q_high))
                robust_span = hi - lo
                scale[j] = max(abs(robust_span), float(self.min_scale))

        self.center_ = center
        self.scale_ = scale
        self.is_bounded_ = is_bounded
        self.bounded_low_ = bounded_low
        self.bounded_high_ = bounded_high
        self.n_features_in_ = n
        self.feature_names_in_ = np.asarray(self.feature_names, dtype=object)
        return self

    def transform(self, X):
        x = np.asarray(X, dtype=np.float64)
        if x.ndim != 2 or x.shape[1] != self.n_features_in_:
            raise ValueError(
                f"Expected shape (*,{self.n_features_in_}), got {x.shape}"
            )

        out = np.empty_like(x, dtype=np.float64)
        for j in range(self.n_features_in_):
            col = x[:, j]
            if self.is_bounded_[j]:
                out[:, j] = np.clip(
                    col,
                    self.bounded_low_[j],
                    self.bounded_high_[j],
                )
            else:
                z = (col - self.center_[j]) / self.scale_[j]
                out[:, j] = np.clip(
                    z,
                    -float(self.transformed_clip),
                    float(self.transformed_clip),
                )
        return out.astype(np.float32, copy=False)

    def get_feature_names_out(self, input_features=None):
        if input_features is not None:
            return np.asarray(input_features, dtype=object)
        return np.asarray(self.feature_names, dtype=object)


def build_preprocessor(
    continuous: list[str],
    binary: list[str],
    categorical: list[str],
    quantile_range: tuple[float, float] = (5.0, 95.0),
    bounded_ranges: dict[str, tuple[float, float]] | None = None,
    min_scale: float = 0.1,
    transformed_clip: float = 20.0,
    binary_imputer_value: int = 0,
    categorical_imputer_value: str = "__MISSING__",
) -> ColumnTransformer:
    continuous_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
            (
                "scaler",
                StableNumericTransformer(
                    feature_names=tuple(continuous),
                    quantile_range=quantile_range,
                    bounded_ranges=bounded_ranges or {},
                    min_scale=float(min_scale),
                    transformed_clip=float(transformed_clip),
                ),
            ),
        ]
    )

    binary_pipe = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="constant",
                    fill_value=binary_imputer_value,
                    keep_empty_features=True,
                ),
            )
        ]
    )

    categorical_pipe = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="constant",
                    fill_value=categorical_imputer_value,
                    keep_empty_features=True,
                ),
            ),
            (
                "onehot",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=False,
                    dtype=np.float32,
                ),
            ),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("continuous", continuous_pipe, continuous),
            ("binary", binary_pipe, binary),
            ("categorical", categorical_pipe, categorical),
        ],
        remainder="drop",
        sparse_threshold=0.0,
        verbose_feature_names_out=True,
    )


def export_preprocessor_contract(
    preprocessor: ColumnTransformer,
    continuous: list[str],
    binary: list[str],
    categorical: list[str],
    output_path: str | Path,
) -> dict[str, Any]:
    continuous_pipe = preprocessor.named_transformers_["continuous"]
    cont_imputer = continuous_pipe.named_steps["imputer"]
    scaler: StableNumericTransformer = continuous_pipe.named_steps["scaler"]

    binary_pipe = preprocessor.named_transformers_["binary"]
    binary_imputer = binary_pipe.named_steps["imputer"]

    categorical_pipe = preprocessor.named_transformers_["categorical"]
    categorical_imputer = categorical_pipe.named_steps["imputer"]
    onehot = categorical_pipe.named_steps["onehot"]

    output_features = preprocessor.get_feature_names_out().tolist()

    bounded_ranges = {}
    for j, feature in enumerate(continuous):
        if bool(scaler.is_bounded_[j]):
            bounded_ranges[feature] = [
                float(scaler.bounded_low_[j]),
                float(scaler.bounded_high_[j]),
            ]

    contract = {
        "schema_version": "stable-preprocessor-v2",
        "raw_feature_order": continuous + binary + categorical,
        "continuous": {
            "features": continuous,
            "imputer_statistics": [float(x) for x in cont_imputer.statistics_],
            "transformer": "StableNumericTransformer",
            "robust_center": [float(x) for x in scaler.center_],
            "robust_scale": [float(x) for x in scaler.scale_],
            "quantile_range": [float(x) for x in scaler.quantile_range],
            "minimum_scale": float(scaler.min_scale),
            "transformed_clip": float(scaler.transformed_clip),
            "bounded_ranges": bounded_ranges,
            "bounded_policy": "clip to physical range; no robust scaling",
            "unbounded_policy": "(x - median) / max(q_high-q_low,min_scale), then transformed clipping",
        },
        "binary": {
            "features": binary,
            "imputer_statistics": [float(x) for x in binary_imputer.statistics_],
        },
        "categorical": {
            "features": categorical,
            "imputer_statistics": [str(x) for x in categorical_imputer.statistics_],
            "categories": {
                feature: [str(v) for v in categories.tolist()]
                for feature, categories in zip(categorical, onehot.categories_)
            },
            "unknown_policy": "all-zero one-hot for unseen category",
        },
        "transformed_feature_order": output_features,
        "transformed_dimension": len(output_features),
        "dtype": "float32",
    }

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(contract, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return contract


# ---- end of the verbatim copy ----


# The eight ratios the delivered contract keeps in their physical range [0, 1].
BOUNDED = {name: (0.0, 1.0) for name in (
    "s7_response_match_rate_16", "s7_request_ratio_16", "s7_direction_change_rate_16",
    "s7_function_change_rate_16", "s7_function_entropy_16", "s7_function_transition_entropy_16",
    "s7_rosctr_change_rate_16", "s7_pdu_reference_unique_ratio_32")}


def fit(train: pd.DataFrame) -> ColumnTransformer:
    """The delivered recipe (5-95 percentile span, 0.1 floor, clip 20, the eight bounded ratios),
    fitted on the training part only."""
    pre = build_preprocessor(list(CONTINUOUS), list(BINARY), list(CATEGORICAL), bounded_ranges=BOUNDED)
    pre.fit(train[list(FEATURES)])
    return pre


def transform(pre: ColumnTransformer, frame: pd.DataFrame) -> np.ndarray:
    """The model's input rows, float32, in the contract's transformed order."""
    return np.asarray(pre.transform(frame[list(FEATURES)]), dtype=np.float32)


def write_contract(pre: ColumnTransformer, path) -> dict:
    """preprocessor_contract.json, in the delivered schema the Java loader reads."""
    return export_preprocessor_contract(pre, list(CONTINUOUS), list(BINARY), list(CATEGORICAL), Path(path))
