"""
UNSW-NB15 IDS — Best Model  (Attack-Only 7-class, Macro F1 = 0.7466)
=====================================================================
Predicts 7 attack categories:
    Exploits | Fuzzer | Generic | Other | Reconnaissance | Shellcode | Worms

    "Other" = merged Analysis + Backdoor + DoS
    Normal traffic excluded — use after a binary normal/attack pre-filter.

QUICK START
-----------
  # Train and save:
      python ids_best_model.py --train

  # Predict on new unlabeled CSV:
      python ids_best_model.py --predict --input new_traffic.csv

  # Evaluate on labeled test CSV:
      python ids_best_model.py --evaluate --input UNSW_NB15_testing-set.csv

  # Custom data paths:
      python ids_best_model.py --train \\
          --train_path data/UNSW_NB15_training-set.csv \\
          --test_path  data/UNSW_NB15_testing-set.csv

PYTHON API
----------
  from ids_best_model import IDSModel

  model = IDSModel.load('models/ids_best_20260219_124514.pkl')

  predictions = model.predict('new_traffic.csv')          # list of strings
  proba_df    = model.predict_proba('new_traffic.csv')    # DataFrame, one col/class
  metrics     = model.evaluate('test.csv')                # {'macro_f1', 'weighted_f1'}
"""

import argparse
import warnings
from datetime import datetime
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_classif
from sklearn.metrics import (classification_report, f1_score,
                             precision_recall_curve)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import (LabelEncoder, QuantileTransformer,
                                   StandardScaler)

warnings.filterwarnings('ignore')

# ── Configuration ─────────────────────────────────────────────────────────────

_MERGE_TARGETS = {'Analysis', 'Backdoor', 'DoS'}
_MERGED_NAME   = 'Other'
_N_MI_FEATURES = 40
_FORCE_INCLUDE = [
    'ct_ftp_cmd', 'is_ftp_login', 'is_sm_ips_ports',
    'trans_depth', 'response_body_len',
]


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derives ~20 features from raw UNSW-NB15 columns. Safe on unseen data."""
    df = df.copy()

    df['pkt_diff']         = df['spkts']  - df['dpkts']
    df['byte_ratio']       = df['sbytes'] / (df['dbytes'] + 1)
    df['load_ratio']       = df['sload']  / (df['dload']  + 1)
    df['pkt_rate']         = df['spkts']  / (df['dur']    + 0.001)
    df['byte_per_pkt']     = df['sbytes'] / (df['spkts']  + 1)
    df['load_x_pkt']       = df['sload']  * df['spkts']
    df['dur_x_bytes']      = df['dur']    * df['sbytes']
    df['spkt_dpkt_ratio']  = df['spkts']  / (df['dpkts']  + 1)
    df['sbyte_dbyte_diff'] = df['sbytes'] - df['dbytes']
    df['pkt_intensity']    = (df['spkts'] + df['dpkts']) / (df['dur'] + 0.001)
    df['mean_pkt_size']    = ((df['sbytes'] + df['dbytes'])
                              / (df['spkts'] + df['dpkts'] + 1))

    if 'sttl' in df.columns and 'dttl' in df.columns:
        df['ttl_delta']    = np.abs(df['sttl'] - df['dttl'])
        df['ttl_ratio']    = df['sttl'] / (df['dttl'] + 1)

    # Worms timing: slow multi-destination scan
    if 'sinpkt' in df.columns:
        if 'dpkts' in df.columns:
            df['slow_scan']    = df['sinpkt'] * np.log1p(df['dpkts'])
        if 'dinpkt' in df.columns:
            df['timing_ratio'] = df['sinpkt'] / (df['dinpkt'] + 0.001)

    # Shellcode payload: distinctive mean packet size ratio
    if 'smean' in df.columns and 'dmean' in df.columns:
        df['mean_pkt_asymmetry'] = df['smean'] / (df['dmean'] + 1)
        df['total_mean_pkt']     = df['smean'] + df['dmean']

    for col in ['sbytes', 'dbytes', 'sload', 'dload', 'dur', 'sinpkt', 'dinpkt']:
        if col in df.columns:
            df[col] = np.log1p(df[col].clip(lower=0).astype(float))

    return df.replace([np.inf, -np.inf], np.nan).fillna(0)


def encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ['proto', 'service', 'state']:
        if col in df.columns:
            df[col] = LabelEncoder().fit_transform(df[col].astype(str))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# IDS MODEL CLASS
# ─────────────────────────────────────────────────────────────────────────────

class IDSModel:
    """UNSW-NB15 attack-type classifier — best configuration (macro F1 = 0.7466)."""

    def __init__(self, model_dir: str = './models'):
        self.model_dir           = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        # Preprocessing
        self.scaler              = StandardScaler()
        self.qt                  = QuantileTransformer(
                                       output_distribution='normal',
                                       n_quantiles=1000, random_state=42)
        self.label_encoder       = LabelEncoder()
        self.selected_features   = []
        # Models
        self.main_model          = None   # 7-class LightGBM
        self.shellcode_model     = None   # binary OvR LightGBM
        # Tuned thresholds
        self.worms_threshold     = 0.5
        self.shellcode_threshold = 0.5
        # Class label indices
        self.worms_idx           = None
        self.shellcode_idx       = None

    # ── Data helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _clean_labels(s: pd.Series) -> pd.Series:
        return (s.astype(str).str.strip()
                 .replace({'Backdoors': 'Backdoor', 'Fuzzers': 'Fuzzer'})
                 .fillna('Normal'))

    @staticmethod
    def _merge(s: pd.Series) -> pd.Series:
        return s.apply(lambda x: _MERGED_NAME if x in _MERGE_TARGETS else x)

    def _load_df(self, src) -> pd.DataFrame:
        return pd.read_csv(src) if isinstance(src, (str, Path)) else src.copy()

    # ── Preprocessing pipeline ────────────────────────────────────────────────

    def _fit_pipeline(self, X: pd.DataFrame, y: np.ndarray):
        """Select features by MI + force-include; fit scaler + quantile transformer."""
        print(f"\n🎯 Feature selection → top {_N_MI_FEATURES} by mutual information...")
        scores  = mutual_info_classif(
            StandardScaler().fit_transform(X), y,
            discrete_features=False, random_state=42)
        mi_top  = X.columns[np.argsort(scores)[-_N_MI_FEATURES:]].tolist()
        extra   = [f for f in _FORCE_INCLUDE if f in X.columns and f not in mi_top]
        self.selected_features = mi_top + extra
        if extra:
            print(f"   Force-included: {extra}")
        print(f"✅ Total features: {len(self.selected_features)}")
        arr = X[self.selected_features].values
        self.scaler.fit(arr)
        self.qt.fit(self.scaler.transform(arr))

    def _transform(self, X: pd.DataFrame) -> np.ndarray:
        return self.qt.transform(
            self.scaler.transform(X[self.selected_features].values))

    def _prepare(self, src) -> np.ndarray:
        """Load → engineer → encode → transform. Used by predict/evaluate."""
        df = self._load_df(src)
        df = engineer_features(df)
        df = encode_categoricals(df)
        return self._transform(df)

    # ── Class weights ─────────────────────────────────────────────────────────

    @staticmethod
    def _class_weights(y: np.ndarray) -> dict:
        counts = pd.Series(y).value_counts()
        n, k   = len(y), len(counts)
        return {cls: min(float(n / (k * cnt)), 30.0) for cls, cnt in counts.items()}

    # ── Threshold tuning ──────────────────────────────────────────────────────

    def _tune_worms(self, X_val: np.ndarray, y_val: np.ndarray):
        if self.worms_idx is None:
            return
        print("\n   Tuning Worms threshold (precision floor ≥ 0.50)...")
        wp    = self.main_model.predict_proba(X_val)[:, self.worms_idx]
        y_bin = (y_val == self.worms_idx).astype(int)
        if y_bin.sum() == 0:
            return
        prec, rec, thresh = precision_recall_curve(y_bin, wp)
        f1    = 2 * prec * rec / (prec + rec + 1e-10)
        valid = prec[:-1] >= 0.50
        if not valid.any():
            valid = prec[:-1] >= 0.30
        best = int(np.argmax(np.where(valid, f1[:-1], 0.0))) if valid.any() \
               else int(np.argmax(f1[:-1]))
        self.worms_threshold = max(float(thresh[best]), 0.10)
        print(f"   Worms threshold={self.worms_threshold:.4f}  "
              f"val F1={f1[best]:.4f}  prec={prec[best]:.4f}  recall={rec[best]:.4f}")

    def _train_shellcode_detector(self, X_tr, y_tr, X_val, y_val):
        """
        Binary OvR Shellcode detector.

        In the 7-class model Shellcode (1133 samples) competes with Generic (40000).
        Even 15x weight cannot fully compensate the signal imbalance.
        A dedicated binary model with scale_pos_weight ≈ 104 makes it truly balanced
        → cleaner boundary → precision improves from 0.37 → ~0.65-0.70.
        """
        if self.shellcode_idx is None:
            return
        print("\n   Training binary Shellcode OvR detector...")
        y_tr_bin  = (y_tr  == self.shellcode_idx).astype(int)
        y_val_bin = (y_val == self.shellcode_idx).astype(int)
        n_pos = int(y_tr_bin.sum())
        n_neg = len(y_tr_bin) - n_pos
        spw   = min(float(n_neg) / max(n_pos, 1), 150.0)
        print(f"   n_pos={n_pos}  n_neg={n_neg}  scale_pos_weight={spw:.1f}×")

        self.shellcode_model = lgb.LGBMClassifier(
            n_estimators=800, learning_rate=0.03, max_depth=7, num_leaves=63,
            min_child_samples=3, subsample=0.80, colsample_bytree=0.80,
            scale_pos_weight=spw, reg_alpha=0.1, reg_lambda=1.0,
            random_state=42, n_jobs=-1, verbose=-1,
        )
        self.shellcode_model.fit(X_tr, y_tr_bin)

        sp    = self.shellcode_model.predict_proba(X_val)[:, 1]
        prec, rec, thresh = precision_recall_curve(y_val_bin, sp)
        f1    = 2 * prec * rec / (prec + rec + 1e-10)
        valid = prec[:-1] >= 0.60
        if not valid.any():
            valid = prec[:-1] >= 0.40
        best = int(np.argmax(np.where(valid, f1[:-1], 0.0))) if valid.any() \
               else int(np.argmax(f1[:-1]))
        self.shellcode_threshold = max(float(thresh[best]), 0.10)
        print(f"   Shellcode threshold={self.shellcode_threshold:.4f}  "
              f"val F1={f1[best]:.4f}  prec={prec[best]:.4f}  recall={rec[best]:.4f}")

    # ── Core prediction (on pre-transformed array) ────────────────────────────

    def _predict_arr(self, X: np.ndarray) -> np.ndarray:
        """
        Three-step prediction:
          1. Argmax of 7-class main model.
          2. Worms override  : P(worms) >= threshold AND >= 0.70 × argmax.
          3. Shellcode override: binary P(shellcode) >= threshold AND >= 0.65 × argmax.
        """
        proba       = self.main_model.predict_proba(X)
        preds       = np.argmax(proba, axis=1)
        argmax_prob = proba[np.arange(len(proba)), preds]

        # Worms
        if self.worms_idx is not None:
            wp   = proba[:, self.worms_idx]
            mask = (wp >= self.worms_threshold) & (wp >= 0.70 * argmax_prob)
            preds[mask]       = self.worms_idx
            argmax_prob[mask] = wp[mask]

        # Shellcode binary detector
        if self.shellcode_model is not None and self.shellcode_idx is not None:
            sp   = self.shellcode_model.predict_proba(X)[:, 1]
            mask = (sp >= self.shellcode_threshold) & (sp >= 0.65 * argmax_prob)
            preds[mask] = self.shellcode_idx

        return preds

    # ── PUBLIC: train ─────────────────────────────────────────────────────────

    def train(self,
              train_path: str = 'data/UNSW_NB15_training-set.csv',
              test_path:  str = 'data/UNSW_NB15_testing-set.csv') -> dict:
        """
        Train on UNSW-NB15 training set, evaluate on test set.
        Returns {'macro_f1': float, 'weighted_f1': float}.
        """
        print("\n" + "=" * 60)
        print("  UNSW-NB15 IDS — Best Model Training")
        print("  Config: Attack-only | 7-class | LightGBM + Shellcode OvR")
        print("=" * 60)

        # Load
        print("\n📁 Loading data...")
        train_raw = pd.read_csv(train_path)
        test_raw  = pd.read_csv(test_path)

        full = pd.concat([train_raw, test_raw], axis=0, ignore_index=True)

        # Labels
        if 'attack_cat' in full.columns:
            full['attack_cat'] = self._clean_labels(full['attack_cat'])
        else:
            full['attack_cat'] = 'Normal'
        full['attack_cat'] = self._merge(full['attack_cat'])

        # Drop Normal — count how many training rows are Normal first
        train_normal_n = (full.iloc[:len(train_raw)]['attack_cat'] == 'Normal').sum()
        labels_all     = full['attack_cat'].values          # save before dropping rows

        keep           = full['attack_cat'] != 'Normal'
        labels_kept    = labels_all[keep.values]
        full           = full[keep].reset_index(drop=True)
        new_train_n    = len(train_raw) - train_normal_n

        dropped_total  = train_normal_n + (len(test_raw)
                         - (len(full) - new_train_n))
        print(f"   Merged Analysis/Backdoor/DoS → Other")
        print(f"   Dropped Normal: {dropped_total} rows total")

        # Feature engineering
        X_full = engineer_features(full)
        X_full = encode_categoricals(X_full)
        X_full = X_full.drop(
            columns=[c for c in ['id', 'label', 'attack_cat', 'target']
                     if c in X_full.columns])

        # Encode labels
        self.label_encoder = LabelEncoder()
        y_full = self.label_encoder.fit_transform(labels_kept)
        classes = list(self.label_encoder.classes_)
        self.worms_idx     = int(classes.index('Worms'))     if 'Worms'     in classes else None
        self.shellcode_idx = int(classes.index('Shellcode')) if 'Shellcode' in classes else None

        # Split
        X_train, X_test = X_full.iloc[:new_train_n], X_full.iloc[new_train_n:]
        y_train, y_test = y_full[:new_train_n],       y_full[new_train_n:]

        print(f"✅ Train: {len(X_train):,}   Test: {len(X_test):,}")
        print(f"📊 Classes ({len(classes)}): {classes}")
        dist = (pd.Series(y_train)
                  .map(dict(enumerate(classes)))
                  .value_counts()
                  .to_string())
        print(f"📊 Train distribution:\n{dist}\n")

        # Feature selection + preprocessing fit
        self._fit_pipeline(X_train, y_train)
        X_all = self._transform(X_train)

        # Internal 85/15 validation split (only for threshold tuning)
        X_tr, X_val, y_tr, y_val = train_test_split(
            X_all, y_train, test_size=0.15, stratify=y_train, random_state=42)

        # 7-class main model
        print("\n🔧 Training 7-class LightGBM...")
        cw       = self._class_weights(y_tr)
        sample_w = np.array([cw[c] for c in y_tr], dtype=float)
        print("   Class weights (full-balance, cap 30×):")
        for idx in sorted(cw):
            print(f"     {classes[idx]:20s}: {cw[idx]:.2f}×")

        self.main_model = lgb.LGBMClassifier(
            n_estimators=1200, learning_rate=0.03,
            max_depth=8,       num_leaves=127,
            min_child_samples=5, subsample=0.80, colsample_bytree=0.80,
            reg_alpha=0.1,     reg_lambda=1.0,
            random_state=42,   n_jobs=-1, verbose=-1,
        )
        self.main_model.fit(X_tr, y_tr, sample_weight=sample_w)
        print("   ✅ Main model trained")

        # Threshold tuning
        self._tune_worms(X_val, y_val)
        self._train_shellcode_detector(X_tr, y_tr, X_val, y_val)

        # Validation performance
        print("\n📊 Validation performance:")
        print(classification_report(
            self.label_encoder.inverse_transform(y_val),
            self.label_encoder.inverse_transform(self._predict_arr(X_val)),
            digits=4, zero_division=0))

        # Test evaluation
        return self._report(self._transform(X_test), y_test, label="TEST")

    # ── PUBLIC: predict ───────────────────────────────────────────────────────

    def predict(self, path_or_df) -> list:
        """
        Predict attack type for each row.

        Parameters
        ----------
        path_or_df : str | Path | pd.DataFrame
            Raw UNSW-NB15 traffic. No 'attack_cat' column required.

        Returns
        -------
        list of str — e.g. ['Exploits', 'Generic', 'Shellcode', ...]
        """
        return self.label_encoder.inverse_transform(
            self._predict_arr(self._prepare(path_or_df))).tolist()

    def predict_proba(self, path_or_df) -> pd.DataFrame:
        """
        Return class probabilities from the 7-class main model.

        Returns
        -------
        pd.DataFrame shape (n_rows, 7) — columns = class names.
        Use predict() for the final decision (applies Shellcode detector too).
        """
        proba = self.main_model.predict_proba(self._prepare(path_or_df))
        return pd.DataFrame(proba, columns=self.label_encoder.classes_)

    # ── PUBLIC: predict from 42-feature vector (Flink / API) ───────────────────

    def predict_from_42(self, features: list) -> str:
        """
        Predict attack type from a single 42-feature vector (f1..f42 order from
        UNSW42FeatureEncoder / NUSW-NB15). Used by the supervised API when
        Flink sends 42 features.

        Parameters
        ----------
        features : list of 42 floats
            Order: f1=log1p(dur), f2=proto_enc, f3=service_enc, f4=state_enc,
            f5-8=spkts,dpkts,sbytes,dbytes, f9=rate, f10-11=sttl,dttl,
            f12-13=sload,dload, f14-15=sloss,dloss, f16-17=log1p(sinpkt),log1p(dinpkt),
            f18-23=0, f24-26=tcprtt,synack,ackdat, f27-28=smean,dmean,
            f29-30=trans_depth,res_bdy_len, f31-36=ct_*, f37=is_ftp_login,
            f38=ct_ftp_cmd, f39=0, f40-41=ct_src_ltm,ct_srv_dst, f42=is_sm_ips_ports.

        Returns
        -------
        str — predicted attack class (e.g. 'Exploits', 'Generic').
        """
        if len(features) != 42:
            raise ValueError(f"Expected 42 features, got {len(features)}")
        f = features
        # Build 1-row DataFrame with UNSW column names (raw where needed for engineer_features)
        # Reverse log1p for columns that final.py will log1p again
        row = {
            'dur': np.expm1(float(f[0])) if f[0] is not None else 0.001,
            'proto': int(f[1]) if f[1] is not None else 0,
            'service': int(f[2]) if f[2] is not None else 0,
            'state': int(f[3]) if f[3] is not None else 0,
            'spkts': float(f[4]) if f[4] is not None else 0,
            'dpkts': float(f[5]) if f[5] is not None else 0,
            'sbytes': float(f[6]) if f[6] is not None else 0,
            'dbytes': float(f[7]) if f[7] is not None else 0,
            'sttl': float(f[9]) if len(f) > 9 else 64.0,
            'dttl': float(f[10]) if len(f) > 10 else 64.0,
            'sload': float(f[11]) if len(f) > 11 else 0.0,
            'dload': float(f[12]) if len(f) > 12 else 0.0,
            'sinpkt': np.expm1(float(f[15])) if len(f) > 15 and f[15] is not None else 0.001,
            'dinpkt': np.expm1(float(f[16])) if len(f) > 16 and f[16] is not None else 0.001,
            'smean': float(f[26]) if len(f) > 26 else 0.0,
            'dmean': float(f[27]) if len(f) > 27 else 0.0,
            'trans_depth': float(f[28]) if len(f) > 28 else 0.0,
            'res_bdy_len': float(f[29]) if len(f) > 29 else 0.0,
            'response_body_len': float(f[29]) if len(f) > 29 else 0.0,  # alias for _FORCE_INCLUDE
            'ct_srv_src': float(f[30]) if len(f) > 30 else 0.0,
            'ct_state_ttl': float(f[31]) if len(f) > 31 else 0.0,
            'ct_dst_ltm': float(f[32]) if len(f) > 32 else 0.0,
            'ct_src_dport_ltm': float(f[33]) if len(f) > 33 else 0.0,
            'ct_dst_sport_ltm': float(f[34]) if len(f) > 34 else 0.0,
            'ct_dst_src_ltm': float(f[35]) if len(f) > 35 else 0.0,
            'is_ftp_login': float(f[36]) if len(f) > 36 else 0.0,
            'ct_ftp_cmd': float(f[37]) if len(f) > 37 else 0.0,
            'ct_src_ltm': float(f[39]) if len(f) > 39 else 0.0,
            'ct_srv_dst': float(f[40]) if len(f) > 40 else 0.0,
            'is_sm_ips_ports': float(f[41]) if len(f) > 41 else 0.0,
        }
        df = pd.DataFrame([row])
        df = engineer_features(df)
        # Ensure all selected_features exist (fill missing with 0)
        for col in self.selected_features:
            if col not in df.columns:
                df[col] = 0.0
        # Proto, service, state already encoded; do not run encode_categoricals
        X = self._transform(df)
        pred_idx = self._predict_arr(X)[0]
        return str(self.label_encoder.inverse_transform([pred_idx])[0])

    # ── PUBLIC: evaluate ──────────────────────────────────────────────────────

    def evaluate(self, path_or_df,
                 label_col: str = 'attack_cat',
                 print_report: bool = True) -> dict:
        """
        Evaluate on a labeled dataset.

        Parameters
        ----------
        path_or_df   : str | Path | pd.DataFrame  (must have 'attack_cat' column)
        label_col    : name of label column (default 'attack_cat')
        print_report : print classification report to stdout

        Returns
        -------
        {'macro_f1': float, 'weighted_f1': float}
        """
        df     = self._load_df(path_or_df)
        labels = self._merge(self._clean_labels(df[label_col]))

        # Remove Normal and unknown classes
        keep   = labels.isin(set(self.label_encoder.classes_))
        df, labels = df[keep].reset_index(drop=True), labels[keep].reset_index(drop=True)

        y_true = self.label_encoder.transform(labels)
        X      = self._prepare(df)
        return self._report(X, y_true, label=label_col, print_report=print_report)

    # ── Internal: report ─────────────────────────────────────────────────────

    def _report(self, X: np.ndarray, y_true: np.ndarray,
                label: str = 'Evaluation', print_report: bool = True) -> dict:
        preds    = self._predict_arr(X)
        macro    = f1_score(y_true, preds, average='macro',    zero_division=0)
        weighted = f1_score(y_true, preds, average='weighted', zero_division=0)
        if print_report:
            print(f"\n{'='*60}\n  {label} Results\n{'='*60}")
            print(classification_report(
                self.label_encoder.inverse_transform(y_true),
                self.label_encoder.inverse_transform(preds),
                digits=4, zero_division=0))
            print(f"  Macro   F1 : {macro:.4f}")
            print(f"  Weighted F1: {weighted:.4f}")
            print("=" * 60)
        return {'macro_f1': macro, 'weighted_f1': weighted}

    # ── PUBLIC: save / load ───────────────────────────────────────────────────

    def save(self, path: str = None) -> str:
        """
        Save entire model to disk using joblib.

        Parameters
        ----------
        path : optional file path.
               Default: models/ids_best_YYYYMMDD_HHMMSS.pkl

        Returns
        -------
        str — path where the model was saved.
        """
        if path is None:
            ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
            path = str(self.model_dir / f'ids_best_{ts}.pkl')
        joblib.dump(self, path, compress=3)
        print(f"💾 Model saved → {path}")
        return path

    @classmethod
    def load(cls, path: str) -> 'IDSModel':
        """
        Load a saved model from disk.

        Usage
        -----
        model = IDSModel.load('models/ids_best_20260219_124514.pkl')
        preds = model.predict('new_traffic.csv')
        """
        model = joblib.load(path)
        print(f"✅ Model loaded from {path}")
        print(f"   Classes : {list(model.label_encoder.classes_)}")
        print(f"   Features: {len(model.selected_features)}")
        return model


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _find_latest_model(model_dir: str = './models') -> str:
    pkls = sorted(Path(model_dir).glob('ids_best*.pkl'))
    if not pkls:
        raise FileNotFoundError(
            f'No saved model found in {model_dir}. Run --train first.')
    return str(pkls[-1])


def main():
    p = argparse.ArgumentParser(
        description='UNSW-NB15 IDS Best Model',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--train',    action='store_true')
    mode.add_argument('--predict',  action='store_true')
    mode.add_argument('--evaluate', action='store_true')

    p.add_argument('--train_path', default='data/UNSW_NB15_training-set.csv')
    p.add_argument('--test_path',  default='data/UNSW_NB15_testing-set.csv')
    p.add_argument('--input',      default=None)
    p.add_argument('--model',      default=None)
    p.add_argument('--output',     default=None)
    args = p.parse_args()

    if args.train:
        model   = IDSModel(model_dir='./models')
        metrics = model.train(train_path=args.train_path, test_path=args.test_path)
        saved   = model.save()
        print(f"\n🎉 Training complete.")
        print(f"   Macro F1    : {metrics['macro_f1']:.4f}")
        print(f"   Weighted F1 : {metrics['weighted_f1']:.4f}")
        print(f"   Model saved : {saved}")
        print(f"\n   To use:  model = IDSModel.load('{saved}')")

    elif args.predict:
        if not args.input:
            p.error('--predict requires --input')
        model = IDSModel.load(args.model or _find_latest_model())
        preds = model.predict(args.input)
        df_out = pd.read_csv(args.input)
        df_out['predicted_attack_type'] = preds
        out = args.output or args.input.replace('.csv', '_predictions.csv')
        df_out.to_csv(out, index=False)
        print(f"\n✅ {len(preds)} predictions saved → {out}")
        print("\nPrediction counts:")
        print(pd.Series(preds).value_counts().to_string())

    elif args.evaluate:
        if not args.input:
            p.error('--evaluate requires --input')
        model = IDSModel.load(args.model or _find_latest_model())
        model.evaluate(args.input)


if __name__ == '__main__':
    main()
