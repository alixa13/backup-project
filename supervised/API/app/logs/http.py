"""
app/logs/http.py

Endpoint and logic for 'http' log type.
"""
from flask import Blueprint, request, jsonify
from datetime import datetime
from app.model_manager import predict

http_bp = Blueprint("http_bp", __name__)

@http_bp.route("/predict_http", methods=["POST"])
def predict_http():
    data = request.get_json(force=True)
    features = data.get("features", [])
    
    # Validate features
    if not features:
        return jsonify({
            "error": "No features provided",
            "log_type": "http"
        }), 400
        
    # Handle both 1D and 2D input formats
    if isinstance(features[0], list):
        if not features[0]:  # Check if inner list is empty
            return jsonify({
                "error": "Empty feature list provided",
                "log_type": "http"
            }), 400
        features = features[0]
    
    try:
        predicted_label, anomaly_score = predict("http", features)
        current_ts = datetime.utcnow().isoformat() + "Z"
        src_ip = features[2] if len(features) > 2 else "unknown"  # id.orig_h at index 2
        dest_ip = features[4] if len(features) > 4 else "unknown"  # id.resp_h at index 4

        response = {
            "ts": current_ts,
            "log_type": "http",
            "prediction": predicted_label,
            "anomaly_score": anomaly_score,
            "src_ip": src_ip,
            "dest_ip": dest_ip,
            # http-specific fields
        }
        return jsonify(response), 200
    except Exception as e:
        return jsonify({
            "error": str(e),
            "log_type": "http"
        }), 500
