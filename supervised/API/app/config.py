"""
app/config.py

Holds application configuration constants, model paths, etc.
"""
import os

# Example environment variables (one model per log type)
MODEL_PATH_CONN = os.getenv("MODEL_PATH_CONN", "./app/models/random_forest_cpu_model_conn.joblib")
ENCODER_PATH_CONN = os.getenv("ENCODER_PATH_CONN", "./app/models/label_encoder_conn.joblib")

MODEL_PATH_SSL = os.getenv("MODEL_PATH_SSL", "./app/models/random_forest_cpu_model_ssl.joblib")
ENCODER_PATH_SSL = os.getenv("ENCODER_PATH_SSL", "./app/models/label_encoder_ssl.joblib")

MODEL_PATH_DNS = os.getenv("MODEL_PATH_DNS", "./app/models/random_forest_cpu_model_dns.joblib")
ENCODER_PATH_DNS = os.getenv("ENCODER_PATH_DNS", "./app/models/label_encoder_dns.joblib")

MODEL_PATH_HTTP = os.getenv("MODEL_PATH_HTTP", "./app/models/random_forest_cpu_model_http.joblib")
ENCODER_PATH_HTTP = os.getenv("ENCODER_PATH_HTTP", "./app/models/label_encoder_http.joblib")

# Flask app settings
HOST = "0.0.0.0"
PORT = 8000
DEBUG = False
