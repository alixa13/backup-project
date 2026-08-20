"""
app/logs/unsw42.py

Endpoint for UNSW-NB15 42-feature model.
Uses final.py IDSModel (ids_best*.pkl from supervised/models) if available,
else falls back to train.py joblib (xgboost_unsw42.joblib).
Flink SupervisedFlinkKafkaConsumerUNSW42 sends 42 features here.
"""
from flask import Blueprint, request, jsonify
from datetime import datetime
import os
import numpy as np
import joblib
import logging
from pathlib import Path

from app.config import (
    IDS_BEST_MODEL_PATH,
    MODEL_PATH_UNSW42,
    ENCODER_PATH_UNSW42,
    PREPROCESSOR_PATH_UNSW42,
)
from app.model_manager import get_trusted_ips

logger = logging.getLogger("unsw42")

unsw42_bp = Blueprint("unsw42_bp", __name__)

# final.py IDSModel (7-class attack classifier) — preferred
IDS_BEST_MODEL = None
# Set when ids_best pkl is found but load fails (for /model-info)
LAST_IDS_BEST_LOAD_ERROR = None
# Legacy: train.py joblib model + encoder
UNSW42_MODEL = None
UNSW42_ENCODER = None
UNSW42_PREPROCESSOR = None


def _find_ids_best_pkl():
    """Resolve path to ids_best*.pkl: env path, then supervised/models, then /app/models (Docker)."""
    if IDS_BEST_MODEL_PATH:
        p = Path(IDS_BEST_MODEL_PATH)
        if p.is_file():
            return str(p)
        if p.is_dir():
            pkls = sorted(p.glob("ids_best*.pkl"))
            if pkls:
                return str(pkls[-1])
    # API root: supervised/API or /app in Docker
    api_root = Path(__file__).resolve().parents[2]
    # 1) supervised/models (when run from repo: .../supervised/API/app -> api_root=.../supervised/API)
    models_dir = api_root.parent / "models"
    if models_dir.is_dir():
        pkls = sorted(models_dir.glob("ids_best*.pkl"))
        if pkls:
            return str(pkls[-1])
    # 2) /app/models (Docker: volume or copied pkl)
    app_models = api_root / "models"
    if app_models.is_dir():
        pkls = sorted(app_models.glob("ids_best*.pkl"))
        if pkls:
            return str(pkls[-1])
    return None


def _load_ids_best():
    """Load final.py IDSModel from ids_best*.pkl."""
    global IDS_BEST_MODEL, LAST_IDS_BEST_LOAD_ERROR
    LAST_IDS_BEST_LOAD_ERROR = None
    if IDS_BEST_MODEL is not None:
        return True
    pkl_path = _find_ids_best_pkl()
    if not pkl_path:
        return False
    try:
        # Ensure final.py can be imported: supervised/ (dev) or parent of pkl dir (e.g. /supervised)
        api_root = Path(__file__).resolve().parents[2]
        supervised_root = api_root.parent
        pkl_parent = Path(pkl_path).resolve().parents[1]  # parent of model dir, e.g. /supervised
        sys_path = __import__("sys").path
        for d in (pkl_parent, supervised_root, api_root):
            s = str(d)
            if s and s not in sys_path:
                sys_path.insert(0, s)
        import final as _final_mod  # supervised/final.py
        from final import IDSModel
        # The pickle was created by running final.py as __main__, so every class,
        # function and constant in final.py is stored as __main__.<name>.
        # Under gunicorn __main__ is gunicorn itself, not final — so we copy
        # all public names from the final module into __main__ before unpickling.
        import __main__ as _main_mod
        for _attr in dir(_final_mod):
            if not _attr.startswith('__'):
                setattr(_main_mod, _attr, getattr(_final_mod, _attr))
        IDS_BEST_MODEL = IDSModel.load(pkl_path)
        logger.info("Loaded supervised model from final.py: %s", pkl_path)
        return True
    except Exception as e:
        LAST_IDS_BEST_LOAD_ERROR = str(e)
        logger.warning("Failed to load ids_best model from %s: %s", pkl_path, e)
        return False


def _load_unsw42_legacy():
    """Load legacy train.py joblib model (xgboost_unsw42.joblib + encoder)."""
    global UNSW42_MODEL, UNSW42_ENCODER, UNSW42_PREPROCESSOR
    if UNSW42_MODEL is not None:
        return True
    model_dir = os.path.dirname(MODEL_PATH_UNSW42)
    for name in ("xgboost_unsw42", "rf_unsw42", "xgb_unsw42"):
        path = os.path.join(model_dir, f"{name}.joblib")
        if os.path.exists(path):
            try:
                UNSW42_MODEL = joblib.load(path)
                UNSW42_ENCODER = joblib.load(ENCODER_PATH_UNSW42)
                UNSW42_PREPROCESSOR = joblib.load(PREPROCESSOR_PATH_UNSW42) if os.path.exists(PREPROCESSOR_PATH_UNSW42) else None
                logger.info("Loaded UNSW42 legacy model from %s", path)
                return True
            except Exception as e:
                logger.warning("Failed to load UNSW42 legacy model: %s", e)
    if os.path.exists(MODEL_PATH_UNSW42) and os.path.exists(ENCODER_PATH_UNSW42):
        try:
            UNSW42_MODEL = joblib.load(MODEL_PATH_UNSW42)
            UNSW42_ENCODER = joblib.load(ENCODER_PATH_UNSW42)
            UNSW42_PREPROCESSOR = joblib.load(PREPROCESSOR_PATH_UNSW42) if os.path.exists(PREPROCESSOR_PATH_UNSW42) else None
            logger.info("Loaded UNSW42 legacy model")
            return True
        except Exception as e:
            logger.warning("Failed to load UNSW42 legacy model: %s", e)
    return False


def _load_unsw42():
    """Load preferred ids_best model first; else legacy joblib."""
    if _load_ids_best():
        return True
    return _load_unsw42_legacy()


def _model_info():
    """Return which model is loaded, classes, and why legacy might be used."""
    pkl_path = _find_ids_best_pkl()
    _load_unsw42()  # ensure something is loaded; sets LAST_IDS_BEST_LOAD_ERROR if ids_best fails
    api_root = Path(__file__).resolve().parents[2]
    info = {
        "model_type": "ids_best" if IDS_BEST_MODEL is not None else "legacy",
        "ids_best_pkl_path": pkl_path,
        "ids_best_load_error": LAST_IDS_BEST_LOAD_ERROR,
        "api_root": str(api_root),
        "paths_checked": {
            "api_root_parent_models": str(api_root.parent / "models"),
            "api_root_models": str(api_root / "models"),
        },
    }
    if IDS_BEST_MODEL is not None:
        info["classes"] = list(IDS_BEST_MODEL.label_encoder.classes_)
        info["backdoor_as_class"] = "Backdoor" in IDS_BEST_MODEL.label_encoder.classes_
    else:
        info["classes"] = list(UNSW42_ENCODER.classes_) if UNSW42_ENCODER is not None else []
        info["backdoor_as_class"] = "Backdoor" in info["classes"] if info["classes"] else False
    return info


@unsw42_bp.route("/model-info", methods=["GET"])
def model_info():
    """Report which UNSW42 model is loaded and its classes (for checking Backdoor vs Other)."""
    try:
        return jsonify(_model_info()), 200
    except Exception as e:
        logger.exception("model-info error")
        return jsonify({"error": str(e)}), 500


@unsw42_bp.route("/predict_unsw42", methods=["POST"])
def predict_unsw42():
    data = request.get_json(force=True)
    # If source IP is in trusted list, skip anomaly check and return normal
    source_ip = data.get("id_orig_h") or data.get("source_ip")
    if source_ip and source_ip in get_trusted_ips():
        return jsonify({
            "ts": datetime.utcnow().isoformat() + "Z",
            "log_type": "unsw42",
            "prediction": "normal",
            "anomaly_score": 0.0,
            "trusted_ip_skipped": True,
        }), 200

    features = data.get("features", [])

    if not features:
        return jsonify({"error": "No features provided", "log_type": "unsw42"}), 400

    if isinstance(features[0], list):
        features = features[0] if features[0] else []

    if len(features) != 42:
        return jsonify({
            "error": f"Expected 42 UNSW features, got {len(features)}",
            "log_type": "unsw42"
        }), 400

    if not _load_unsw42():
        return jsonify({
            "error": "UNSW42 model not loaded. Put ids_best*.pkl in supervised/models/ or set IDS_BEST_MODEL_PATH.",
            "log_type": "unsw42"
        }), 503

    try:
        # Prefer final.py IDSModel (7-class attack type)
        if IDS_BEST_MODEL is not None:
            predicted_label = IDS_BEST_MODEL.predict_from_42([float(x) for x in features])
            anomaly_score = 0.1  # IDSModel returns class only; optional: add proba later
            return jsonify({
                "ts": datetime.utcnow().isoformat() + "Z",
                "log_type": "unsw42",
                "prediction": str(predicted_label),
                "anomaly_score": anomaly_score,
            }), 200

        # Legacy joblib path
        X = np.array(features, dtype=np.float64).reshape(1, -1)
        if UNSW42_PREPROCESSOR and "scaler" in UNSW42_PREPROCESSOR:
            X = UNSW42_PREPROCESSOR["scaler"].transform(X)
        y_pred_num = UNSW42_MODEL.predict(X)[0]
        predicted_label = UNSW42_ENCODER.inverse_transform([y_pred_num])[0]
        try:
            probas = UNSW42_MODEL.predict_proba(X)[0]
            anomaly_score = 1.0 - float(max(probas))
        except Exception:
            anomaly_score = 0.05

        return jsonify({
            "ts": datetime.utcnow().isoformat() + "Z",
            "log_type": "unsw42",
            "prediction": str(predicted_label),
            "anomaly_score": anomaly_score,
        }), 200
    except Exception as e:
        logger.exception("UNSW42 prediction error")
        return jsonify({"error": str(e), "log_type": "unsw42"}), 500
