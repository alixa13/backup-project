#!/usr/bin/env bash
# Test LSTM API endpoints (no code changes).
# Run from host: ./ops/test-lstm-endpoints.sh
# LSTM must be reachable at localhost:5000 (docker-compose port mapping).

set -e
BASE="${LSTM_BASE_URL:-http://localhost:5000}"

echo "=============================================="
echo "LSTM API endpoint tests (base: $BASE)"
echo "=============================================="

echo ""
echo "1) GET /health"
curl -s -w "\nHTTP %{http_code}\n" "$BASE/health" | head -20

echo ""
echo "2) GET /models/status?log_type=conn  (files on disk for conn)"
curl -s -w "\nHTTP %{http_code}\n" "$BASE/models/status?log_type=conn" | head -80

echo ""
echo "3) GET /threshold/conn  (threshold from DB)"
curl -s -w "\nHTTP %{http_code}\n" "$BASE/threshold/conn" | head -20

echo ""
echo "4) POST /predict/conn  (single 33-feature vector, like Flink sends)"
# 33 features: same shape the Flink preprocessor sends
FEATURES='[0.1,0.2,0.0,0.0,0.1,0.2,0.3,0.4,0.1,0.2,0.0,0.0,0.1,0.2,0.3,0.4,0.5,0.0,0.0,0.0,0.0,0.0,0.1,0.2,0.3,0.4,0.5,0.0,0.0,0.0,0.0,0.0,0.0]'
curl -s -w "\nHTTP %{http_code}\n" -X POST "$BASE/predict/conn" \
  -H "Content-Type: application/json" \
  -d "{\"data\": $FEATURES}" | head -20

echo ""
echo "=============================================="
echo "If step 2 shows .h5 files but step 4 says 'no_model',"
echo "reload the in-memory model (no container restart):"
echo "  curl -X POST $BASE/models/reload/conn"
echo "=============================================="
