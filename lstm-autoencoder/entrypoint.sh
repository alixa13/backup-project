#!/bin/bash
set -e

MODEL_PATH=${MODEL_PATH:-"/app/models"}
DB_HOST=${DB_HOST:-"postgres"}
DB_PORT=${DB_PORT:-"5432"}
DB_USER=${DB_USER:-"lstm_user"}

# Ensure data directory exists and has proper permissions
mkdir -p /app/data
chmod 777 /app/data

echo "=== LSTM Autoencoder Initialization ==="
echo "Database: PostgreSQL at $DB_HOST:$DB_PORT"

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ PostgreSQL failed to become ready after 60 seconds"
        exit 1
    fi
    echo "Waiting for PostgreSQL... ($i/30)"
    sleep 2
done

# Check for LSTM models
echo "Checking for LSTM models in $MODEL_PATH..."
if ls $MODEL_PATH/*/*.h5 1> /dev/null 2>&1; then
    echo "✅ Found LSTM model file(s):"
    ls -l $MODEL_PATH/*/*.h5
else
    echo "ℹ️  No models found. Service will start in learning mode."
fi

# Start the application with Gunicorn
echo "=== Starting LSTM Autoencoder Service ==="
echo "Workers: 16 (gevent async)"
echo "Connections per worker: 3000"
echo "========================================="

exec gunicorn --bind 0.0.0.0:5000 \
    --workers 16 \
    --worker-class gevent \
    --worker-connections 3000 \
    --timeout 180 \
    --keep-alive 5 \
    --max-requests 10000 \
    --max-requests-jitter 1000 \
    --log-level warning \
    --access-logfile /app/data/gunicorn-access.log \
    --error-logfile /app/data/gunicorn-error.log \
    --capture-output \
    --enable-stdio-inheritance \
    run:app 