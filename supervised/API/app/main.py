"""
app/main.py

Flask app with UNSW-NB15 42-feature prediction endpoint only.
"""
from flask import Flask, jsonify, request
from app.logs.unsw42 import unsw42_bp
from app.model_manager import add_trusted_ip, remove_trusted_ip, get_trusted_ips

def create_app():
    app = Flask(__name__)

    app.register_blueprint(unsw42_bp)

    @app.route("/")
    def index():
        return {"message": "UNSW-NB15 Supervised ML API"}

    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({
            "status": "ok",
            "service": "supervised-ml-unsw42",
            "version": "1.0.0"
        }), 200

    @app.route("/trusted-ips", methods=["GET"])
    def list_trusted_ips():
        return jsonify({"trusted_ips": get_trusted_ips()}), 200

    @app.route("/trusted-ips", methods=["POST"])
    def create_trusted_ip():
        data = request.get_json(force=True)
        ip = data.get("ip")
        if not ip:
            return jsonify({"error": "IP address is required"}), 400
        success = add_trusted_ip(ip)
        return jsonify({
            "success": success,
            "message": f"Added {ip} to trusted IPs list",
            "trusted_ips": get_trusted_ips()
        }), 201

    @app.route("/trusted-ips/<ip>", methods=["DELETE"])
    def delete_trusted_ip(ip):
        success = remove_trusted_ip(ip)
        if success:
            return jsonify({
                "success": True,
                "message": f"Removed {ip} from trusted IPs list",
                "trusted_ips": get_trusted_ips()
            }), 200
        return jsonify({
            "success": False,
            "message": f"IP {ip} not found in trusted IPs list",
            "trusted_ips": get_trusted_ips()
        }), 404

    return app
