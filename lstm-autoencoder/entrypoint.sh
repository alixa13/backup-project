#!/bin/bash
set -e

MODEL_PATH=${MODEL_PATH:-"/app/models"}
DB_HOST=${DB_HOST:-"postgres"}
DB_PORT=${DB_PORT:-"5432"}
DB_USER=${DB_USER:-"lstm_user"}

# ========================================================
# TensorFlow / NumPy CPU optimization (inference + training)
# ========================================================
# intra_op HIGH so one worker doing TRAINING can use all cores (48+). inter_op low (2–4).
TOTAL_CORES=$(nproc)
WORKERS=${GUNICORN_WORKERS:-4}

if [ -z "$TF_NUM_INTRAOP_THREADS" ] || [ "$TF_NUM_INTRAOP_THREADS" = "2" ]; then
    # intra_op: use all available cores for training (cap 64)
    INTRA=$TOTAL_CORES
    [ "$INTRA" -gt 64 ] && INTRA=64
    [ "$INTRA" -lt 2 ] && INTRA=2
    export TF_NUM_INTRAOP_THREADS=$INTRA
    export OMP_NUM_THREADS=$INTRA
    export MKL_NUM_THREADS=$INTRA
fi
if [ -z "$TF_NUM_INTEROP_THREADS" ]; then
    INTEROP=4
    [ "$INTEROP" -gt "$TOTAL_CORES" ] && INTEROP=$TOTAL_CORES
    export TF_NUM_INTEROP_THREADS=$INTEROP
fi
if [ -z "$OMP_NUM_THREADS" ]; then
    export OMP_NUM_THREADS=$TF_NUM_INTRAOP_THREADS
fi
if [ -z "$MKL_NUM_THREADS" ]; then
    export MKL_NUM_THREADS=$TF_NUM_INTRAOP_THREADS
fi

# Force CPU only (disable CUDA/GPU) — must be set before TensorFlow is imported
export CUDA_VISIBLE_DEVICES=""
# Suppress TensorFlow INFO and cuInit/CUDA noise (3=FATAL only; 2=show errors)
export TF_CPP_MIN_LOG_LEVEL="${TF_CPP_MIN_LOG_LEVEL:-3}"

# Memory optimization
export TF_FORCE_GPU_ALLOW_GROWTH=false
export TF_ENABLE_ONEDNN_OPTS=1  # Intel MKL-DNN optimization

echo "=== CPU Configuration (GPU disabled) ==="
echo "Total cores in container: $TOTAL_CORES"
echo "TensorFlow inter_op threads: $TF_NUM_INTEROP_THREADS"
echo "TensorFlow intra_op threads: $TF_NUM_INTRAOP_THREADS"
echo "OMP threads: $OMP_NUM_THREADS"
echo "MKL threads: $MKL_NUM_THREADS"
echo "========================="

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
if ls $MODEL_PATH/*/*.h5 $MODEL_PATH/*/*.keras 1> /dev/null 2>&1; then
    echo "✅ Found LSTM model file(s):"
    ls -l $MODEL_PATH/*/*.h5 $MODEL_PATH/*/*.keras 2>/dev/null || true
else
    echo "ℹ️  No models found. Service will start in learning mode."
fi

# Gunicorn worker configuration
# Each worker creates a TensorFlow session - balance between parallelism and memory
WORKERS=${GUNICORN_WORKERS:-4}
THREADS_PER_WORKER=8

# Start the application with Gunicorn
echo "=== Starting LSTM Autoencoder Service ==="
echo "Workers: $WORKERS (optimized for ML - each uses all cores)"
echo "Threads per worker: $THREADS_PER_WORKER"
echo "TensorFlow cores per worker: $AVAILABLE_CORES"
echo "Timeout: 900s (15 minutes for training)"
echo "Memory: Each worker can use up to 4GB for training"
echo "========================================="

exec gunicorn --bind 0.0.0.0:5000 \
    --workers ${WORKERS} \
    --threads ${THREADS_PER_WORKER} \
    --worker-class sync \
    --timeout 900 \
    --graceful-timeout 120 \
    --keep-alive 5 \
    --max-requests 1000 \
    --max-requests-jitter 100 \
    --worker-tmp-dir /dev/shm \
    --log-level info \
    --access-logfile /app/data/gunicorn-access.log \
    --error-logfile /app/data/gunicorn-error.log \
    --capture-output \
    --enable-stdio-inheritance \
    run:app 