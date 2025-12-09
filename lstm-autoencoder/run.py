"""
run.py

Script to run your Flask app using the config in app/config.py.
"""
from app.main import create_app
from app.config import HOST, PORT, DEBUG

app = create_app()

if __name__ == "__main__":
    # Start Flask application
    app.run(host=HOST, port=PORT, debug=DEBUG) 