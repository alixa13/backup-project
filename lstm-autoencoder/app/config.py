"""
Configuration settings for the Flask application.
"""
import os

# Flask app settings
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', 5000))
DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'

# Model settings
MODEL_PATH = os.environ.get('MODEL_PATH', '/app/models') 