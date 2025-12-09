"""
app/logs/conn.py

Endpoint and logic for 'conn' log type.
"""
from flask import Blueprint, request, jsonify
from datetime import datetime
from app.model_manager import predict

conn_bp = Blueprint("conn_bp", __name__)

@conn_bp.route("/predict_conn", methods=["POST"])
def predict_conn():
    data = request.get_json(force=True)
    features = data.get("features", [])
    
    # Validate features
    if not features:
        return jsonify({
            "error": "No features provided",
            "log_type": "conn"
        }), 400
        
    # Handle both 1D and 2D input formats
    if isinstance(features[0], list):
        if not features[0]:  # Check if inner list is empty
            return jsonify({
                "error": "Empty feature list provided",
                "log_type": "conn"
            }), 400
        features = features[0]
    
    try:
        predicted_label, anomaly_score = predict("conn", features)
        current_ts = datetime.utcnow().isoformat() + "Z"
        src_ip = features[2] if len(features) > 2 else "unknown"  # id.orig_h at index 2
        dest_ip = features[4] if len(features) > 4 else "unknown"  # id.resp_h at index 4

        response = {
            "ts": current_ts,
            "log_type": "conn",
            "prediction": predicted_label,
            "anomaly_score": anomaly_score,
            "src_ip": src_ip,
            "dest_ip": dest_ip,
            # etc. for conn-specific fields
        }
        return jsonify(response), 200
    except Exception as e:
        return jsonify({
            "error": str(e),
            "log_type": "conn"
        }), 500
