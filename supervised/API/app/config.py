"""
app/config.py

UNSW-NB15 42-feature model configuration.
"""
import os

# Default algorithm: xgboost (high performance), can also be rf or logistic
MODEL_ALGORITHM = os.getenv("MODEL_ALGORITHM", "xgboost")

# UNSW-NB15 42-feature model: prefer final.py IDSModel (ids_best*.pkl), else train.py joblib
IDS_BEST_MODEL_PATH = os.getenv("IDS_BEST_MODEL_PATH", "")  # path to .pkl or dir with ids_best*.pkl
MODEL_PATH_UNSW42 = os.getenv("MODEL_PATH_UNSW42", "./app/models/xgboost_unsw42.joblib")
ENCODER_PATH_UNSW42 = os.getenv("ENCODER_PATH_UNSW42", "./app/models/label_encoder_unsw42.joblib")
PREPROCESSOR_PATH_UNSW42 = os.getenv("PREPROCESSOR_PATH_UNSW42", "./app/models/preprocessor_unsw42.joblib")

# Flask app settings
HOST = "0.0.0.0"
PORT = 8000
DEBUG = False
