"""
app/model_manager.py

Loads multiple models (one per log type) and exposes a generic predict function.
"""
import joblib
import numpy as np
import pandas as pd
from typing import List, Tuple, Union, Dict, Any
import os
import logging
import sys

from app.config import (
    MODEL_PATH_CONN, ENCODER_PATH_CONN,
    MODEL_PATH_SSL,  ENCODER_PATH_SSL,
    MODEL_PATH_DNS,  ENCODER_PATH_DNS,
    MODEL_PATH_HTTP, ENCODER_PATH_HTTP
)

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Log to stdout
    ]
)
logger = logging.getLogger("model_manager")
logger.info("Model manager initializing...")

print("Loading models and label encoders for each log type...")

# Load models for each log type
MODELS = {
    "conn": joblib.load(MODEL_PATH_CONN),
    "ssl":  joblib.load(MODEL_PATH_SSL),
    "dns":  joblib.load(MODEL_PATH_DNS),
    "http": joblib.load(MODEL_PATH_HTTP)
}
logger.info(f"Loaded models: {list(MODELS.keys())}")

# Load label encoders for each log type
ENCODERS = {
    "conn": joblib.load(ENCODER_PATH_CONN),
    "ssl":  joblib.load(ENCODER_PATH_SSL),
    "dns":  joblib.load(ENCODER_PATH_DNS),
    "http": joblib.load(ENCODER_PATH_HTTP)
}
logger.info(f"Loaded label encoders: {list(ENCODERS.keys())}")

# Store model feature names
MODEL_FEATURE_NAMES = {}
for log_type, model in MODELS.items():
    try:
        if hasattr(model, 'feature_names_in_'):
            MODEL_FEATURE_NAMES[log_type] = model.feature_names_in_.tolist()
            logger.info(f"Extracted feature names from {log_type} model: {MODEL_FEATURE_NAMES[log_type]}")
        else:
            logger.warning(f"Model for {log_type} does not have feature_names_in_ attribute")
    except Exception as e:
        logger.error(f"Error extracting feature names from {log_type} model: {e}")

# Define feature names for each log type
FEATURE_NAMES = {
    "conn": [
        'ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p',
        'proto', 'service', 'duration', 'orig_bytes', 'resp_bytes',
        'conn_state', 'local_orig', 'missed_bytes', 'history', 'orig_pkts',
        'orig_ip_bytes', 'resp_pkts', 'resp_ip_bytes', 'tunnel_parents'
    ],
    "ssl": [
        'ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p',
        'version', 'cipher', 'curve', 'server_name', 'resumed', 'established',
        'cert_chain_fuids', 'subject', 'issuer', 'validation_status'
    ],
    "dns": [
        'ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p',
        'proto', 'trans_id', 'rtt', 'query', 'qclass', 'qclass_name',
        'qtype', 'qtype_name', 'rcode', 'rcode_name', 'AA', 'TC', 'RD', 'RA',
        'Z', 'answers', 'TTLs', 'rejected'
    ],
    "http": [
        'ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p',
        'trans_depth', 'method', 'host', 'uri', 'referrer', 'version', 'user_agent',
        'request_body_len', 'response_body_len', 'status_code', 'status_msg',
        'info_code', 'info_msg', 'tags', 'username', 'password', 'proxied'
    ]
}

# Update feature names with model feature names if available
for log_type, feature_names in MODEL_FEATURE_NAMES.items():
    if feature_names:
        FEATURE_NAMES[log_type] = feature_names
        logger.info(f"Using model feature names for {log_type}")

# Define categorical features for each log type
CATEGORICAL_FEATURES = {
    "conn": ['uid', 'id.orig_h', 'id.resp_h', 'proto', 'service', 'conn_state', 'local_orig', 'history', 'tunnel_parents'],
    "ssl": ['uid', 'id.orig_h', 'id.resp_h', 'version', 'cipher', 'curve', 'server_name', 'cert_chain_fuids', 'subject', 'issuer', 'validation_status'],
    "dns": ['uid', 'id.orig_h', 'id.resp_h', 'proto', 'query', 'qclass_name', 'qtype_name', 'rcode_name', 'answers'],
    "http": ['uid', 'id.orig_h', 'id.resp_h', 'method', 'host', 'uri', 'referrer', 'version', 'user_agent', 'status_msg', 'tags', 'username']
}

# Try to load feature label encoders for each log type
FEATURE_ENCODERS = {}
for log_type in ["conn", "ssl", "dns", "http"]:
    encoder_path = f"./app/models/feature_label_encoders_{log_type}.joblib"
    if os.path.exists(encoder_path):
        try:
            FEATURE_ENCODERS[log_type] = joblib.load(encoder_path)
            logger.info(f"Loaded feature encoders for {log_type} log type")
        except Exception as e:
            logger.warning(f"Failed to load feature encoders for {log_type}: {e}")
            FEATURE_ENCODERS[log_type] = {}
    else:
        logger.warning(f"Feature encoders file not found for {log_type}")
        FEATURE_ENCODERS[log_type] = {}

# Trusted IP addresses (will be treated as false positives)
TRUSTED_IPS = set()

# Path to save/load trusted IPs
TRUSTED_IPS_PATH = "./app/models/trusted_ips.txt"

# Load trusted IPs if file exists
def load_trusted_ips():
    global TRUSTED_IPS
    try:
        if os.path.exists(TRUSTED_IPS_PATH):
            with open(TRUSTED_IPS_PATH, 'r') as f:
                TRUSTED_IPS = set(line.strip() for line in f if line.strip())
            logger.info(f"Loaded {len(TRUSTED_IPS)} trusted IPs from {TRUSTED_IPS_PATH}")
    except Exception as e:
        logger.error(f"Error loading trusted IPs: {e}")

# Save trusted IPs to file
def save_trusted_ips():
    try:
        with open(TRUSTED_IPS_PATH, 'w') as f:
            for ip in sorted(TRUSTED_IPS):
                f.write(f"{ip}\n")
        logger.info(f"Saved {len(TRUSTED_IPS)} trusted IPs to {TRUSTED_IPS_PATH}")
    except Exception as e:
        logger.error(f"Error saving trusted IPs: {e}")

# Add an IP to trusted list
def add_trusted_ip(ip):
    TRUSTED_IPS.add(ip)
    save_trusted_ips()
    logger.info(f"Added {ip} to trusted IPs list")
    return True

# Remove an IP from trusted list
def remove_trusted_ip(ip):
    if ip in TRUSTED_IPS:
        TRUSTED_IPS.remove(ip)
        save_trusted_ips()
        logger.info(f"Removed {ip} from trusted IPs list")
        return True
    return False

# Get the current list of trusted IPs
def get_trusted_ips():
    return sorted(list(TRUSTED_IPS))

# Initialize by loading any existing trusted IPs
load_trusted_ips()

def preprocess_features(log_type: str, features: List[str]) -> pd.DataFrame:
    """
    Convert features to the format expected by the model.
    
    Args:
        log_type: Type of log ('conn', 'ssl', 'dns', 'http')
        features: List of feature values
    
    Returns:
        Dataframe with properly formatted features
    """
    # Get the feature names for this log type
    feature_names = FEATURE_NAMES.get(log_type, [])
    
    # If feature names aren't defined or length mismatch, use generic names
    if not feature_names or len(feature_names) != len(features):
        logger.warning(f"Feature names mismatch for {log_type}. Expected {len(feature_names)}, got {len(features)}")
        # Use generic column names if needed
        feature_names = [f"feature_{i}" for i in range(len(features))]
    
    # Convert to DataFrame
    input_df = pd.DataFrame([features], columns=feature_names)
    
    # Get feature encoders for this log type
    encoders = FEATURE_ENCODERS.get(log_type, {})
    
    # Process each feature
    for col in input_df.columns:
        # Check if this is a categorical feature that needs encoding
        if col in CATEGORICAL_FEATURES.get(log_type, []):
            # If we have an encoder for this column, use it
            if col in encoders:
                try:
                    # Convert to string first (encoders only work with strings)
                    input_df[col] = input_df[col].astype(str)
                    
                    # Check if the value exists in the encoder's classes
                    encoder = encoders[col]
                    classes = encoder.classes_
                    
                    # If the value is not in the encoder's classes, replace with a default value
                    value = input_df[col].iloc[0]
                    if value not in classes:
                        logger.warning(f"Unknown category value '{value}' for feature '{col}'. Using default.")
                        # Use the most common class as default
                        input_df[col] = classes[0]
                    
                    # Apply the encoder
                    input_df[col] = encoder.transform(input_df[col])
                    
                except Exception as e:
                    logger.warning(f"Error encoding feature {col}: {e}")
                    # Default to 0 if encoding fails
                    input_df[col] = 0
            else:
                # If no encoder available, try numeric conversion
                input_df[col] = pd.to_numeric(input_df[col], errors='coerce').fillna(0)
        else:
            # For numeric features, convert to float
            input_df[col] = pd.to_numeric(input_df[col], errors='coerce').fillna(0)
    
    # Convert to numpy array for prediction (avoid feature names warning)
    feature_array = input_df.values
    
    return feature_array

def predict(log_type: str, features: Union[List[str], List[List[str]]]) -> Tuple[str, float]:
    """
    Predict using the appropriate model.
    
    Args:
        log_type: Type of log ('conn', 'ssl', 'dns', 'http')
        features: List of features or list of lists of features
    
    Returns:
        Tuple of (predicted_label, anomaly_score)
    """
    if log_type not in MODELS:
        msg = f"Unsupported log_type: {log_type}"
        logger.error(msg)
        raise ValueError(msg)
    
    clf = MODELS[log_type]
    encoder = ENCODERS[log_type]

    # Validate features
    if features is None:
        msg = f"Features is None for {log_type}"
        logger.error(msg)
        raise ValueError(msg)

    if not isinstance(features, (list, tuple)):
        msg = f"Features must be a list or tuple, got {type(features)} for {log_type}"
        logger.error(msg)
        raise TypeError(msg)

    if not features:
        msg = f"Empty features list provided for {log_type}"
        logger.error(msg)
        raise ValueError(msg)

    # Log the type and content of features for debugging
    logger.info(f"Processing {log_type} features. Type: {type(features)}, Length: {len(features)}")
    
    # Handle both 1D and 2D input formats
    if isinstance(features[0], list):
        if not features[0]:  # Check if inner list is empty
            msg = f"Empty inner feature list provided for {log_type}"
            logger.error(msg)
            raise ValueError(msg)
        # If 2D array, take first row
        features = features[0]
        logger.info(f"Converted 2D features to 1D. New length: {len(features)}")
    
    # Validate feature length against expected features
    expected_features = len(FEATURE_NAMES.get(log_type, []))
    if len(features) != expected_features:
        msg = f"Feature length mismatch for {log_type}. Expected {expected_features}, got {len(features)}"
        logger.error(msg)
        raise ValueError(msg)
    
    logger.info(f"Received prediction request for {log_type} with {len(features)} features")
    
    try:
        # Preprocess features
        input_features = preprocess_features(log_type, features)
        
        # Predict numeric label
        y_pred_num = clf.predict(input_features)
        
        # Decode numeric label -> string
        predicted_label = encoder.inverse_transform(y_pred_num)[0]

        # Calculate anomaly score using predict_proba
        try:
            probas = clf.predict_proba(input_features)[0]
            anomaly_score = 1.0 - max(probas)
            
            # Check if source or destination IP is in the trusted IPs list
            is_trusted_ip = False
            src_ip = None
            dest_ip = None
            
            if log_type in ["conn", "ssl", "dns", "http"] and len(features) > 5:
                src_ip = features[2]  # id.orig_h
                dest_ip = features[4]  # id.resp_h
                
                if src_ip in TRUSTED_IPS or dest_ip in TRUSTED_IPS:
                    is_trusted_ip = True
                    logger.info(f"Trusted IP detected: src={src_ip}, dest={dest_ip}")
            
            # Apply rules to reduce false positives
            anomaly_threshold = 0.7  # Base threshold
            
            # If IP is trusted, set a very high threshold (essentially always normal)
            if is_trusted_ip:
                anomaly_threshold = 0.999  # Nearly impossible to exceed
                
            # Apply final threshold check
            if anomaly_score < anomaly_threshold:
                predicted_label = "normal"
                # Reduce the reported anomaly score for normal traffic
                anomaly_score = min(anomaly_score, 0.3)
                
            logger.info(f"Decision factors: score={anomaly_score:.4f}, threshold={anomaly_threshold:.2f}, trusted_ip={is_trusted_ip}")
        except Exception as e:
            logger.warning(f"Could not calculate anomaly score using predict_proba: {e}")
            # If predict_proba is not available, use a default score
            anomaly_score = 0.05

        logger.info(f"Prediction for {log_type}: {predicted_label} (score: {anomaly_score:.4f})")
        return predicted_label, anomaly_score
    except Exception as e:
        logger.error(f"Error during prediction for {log_type}: {str(e)}", exc_info=True)
        raise
