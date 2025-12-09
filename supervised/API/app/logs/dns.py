"""
app/logs/dns.py

Endpoint and logic for 'dns' log type.
"""
from flask import Blueprint, request, jsonify
from datetime import datetime
from app.model_manager import predict
import logging

logger = logging.getLogger(__name__)
dns_bp = Blueprint("dns_bp", __name__)

@dns_bp.route("/predict_dns", methods=["POST"])
def predict_dns():
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({
                "error": "No JSON data provided",
                "log_type": "dns"
            }), 400

        features = data.get("features")
        if features is None:
            return jsonify({
                "error": "No 'features' field in request data",
                "log_type": "dns"
            }), 400

        if not features:
            return jsonify({
                "error": "Empty features list provided",
                "log_type": "dns"
            }), 400

        # Log the received features for debugging
        logger.info(f"Received DNS features: {features}")
        
        # Handle both 1D and 2D input formats
        if isinstance(features, list):
            if len(features) == 0:
                return jsonify({
                    "error": "Empty features list provided",
                    "log_type": "dns"
                }), 400
                
            if isinstance(features[0], list):
                if not features[0]:  # Check if inner list is empty
                    return jsonify({
                        "error": "Empty inner feature list provided",
                        "log_type": "dns"
                    }), 400
                features = features[0]
        else:
            return jsonify({
                "error": "Features must be a list",
                "log_type": "dns"
            }), 400

        # Validate feature length
        expected_features = 23  # Based on DNS feature names in model_manager.py
        if len(features) != expected_features:
            return jsonify({
                "error": f"Expected {expected_features} features, got {len(features)}",
                "log_type": "dns"
            }), 400
    
        predicted_label, anomaly_score = predict("dns", features)
        current_ts = datetime.utcnow().isoformat() + "Z"
        src_ip = features[2] if len(features) > 2 else "unknown"  # id.orig_h at index 2
        dest_ip = features[4] if len(features) > 4 else "unknown"  # id.resp_h at index 4

        response = {
            "ts": current_ts,
            "log_type": "dns",
            "prediction": predicted_label,
            "anomaly_score": anomaly_score,
            "src_ip": src_ip,
            "dest_ip": dest_ip,
            # dns-specific fields
        }
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error in predict_dns: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "log_type": "dns"
        }), 500
