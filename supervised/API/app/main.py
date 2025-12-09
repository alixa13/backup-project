"""
app/main.py

Central file that creates the Flask app, registers the endpoints for each log type.
"""
from flask import Flask, jsonify, request
from app.logs.conn import conn_bp
from app.logs.ssl import ssl_bp
from app.logs.dns import dns_bp
from app.logs.http import http_bp
from app.model_manager import add_trusted_ip, remove_trusted_ip, get_trusted_ips

def create_app():
    app = Flask(__name__)

    # Register blueprints
    app.register_blueprint(conn_bp)
    app.register_blueprint(ssl_bp)
    app.register_blueprint(dns_bp)
    app.register_blueprint(http_bp)

    @app.route("/")
    def index():
        return {"message": "Welcome to the multi-model ML API!"}
        
    @app.route("/health", methods=["GET"])
    def health_check():
        """Health check endpoint for service availability verification"""
        return jsonify({
            "status": "ok",
            "service": "supervised-ml",
            "version": "1.0.0"
        }), 200
        
    # Trusted IP management endpoints
    @app.route("/trusted-ips", methods=["GET"])
    def list_trusted_ips():
        """List all trusted IPs"""
        return jsonify({
            "trusted_ips": get_trusted_ips()
        }), 200
        
    @app.route("/trusted-ips", methods=["POST"])
    def create_trusted_ip():
        """Add an IP to the trusted list"""
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
        """Remove an IP from the trusted list"""
        success = remove_trusted_ip(ip)
        
        if success:
            return jsonify({
                "success": True,
                "message": f"Removed {ip} from trusted IPs list",
                "trusted_ips": get_trusted_ips()
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": f"IP {ip} not found in trusted IPs list",
                "trusted_ips": get_trusted_ips()
            }), 404

    return app
