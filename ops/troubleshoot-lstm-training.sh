#!/bin/bash
# LSTM Autoencoder Training Troubleshooting Script
# Run from project root: ./ops/troubleshoot-lstm-training.sh [log_type]
# Without log_type: checks all (conn, dns, http, ssl)

set -e
LSTM_CONTAINER="${LSTM_CONTAINER:-lstm-autoencoder}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres}"
LOG_TYPES=("conn" "dns" "http" "ssl")
TARGET_LOG_TYPE="${1:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  LSTM Autoencoder Training Troubleshooter                        ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# 1. Container check
echo -e "${CYAN}[1] Container Status${NC}"
if ! docker ps | grep -q "$LSTM_CONTAINER"; then
    echo -e "   ${RED}✗ $LSTM_CONTAINER is not running${NC}"
    echo "   Fix: ./start-system.sh"
    exit 1
fi
echo -e "   ${GREEN}✓ $LSTM_CONTAINER running${NC}"

if ! docker ps | grep -q "$POSTGRES_CONTAINER"; then
    echo -e "   ${RED}✗ $POSTGRES_CONTAINER is not running${NC}"
    exit 1
fi
echo -e "   ${GREEN}✓ $POSTGRES_CONTAINER running${NC}"
echo ""

# 2. Health check
echo -e "${CYAN}[2] LSTM API Health${NC}"
HEALTH=$(docker exec "$LSTM_CONTAINER" curl -s http://localhost:5000/health 2>/dev/null || echo '{"status":"error"}')
STATUS=$(echo "$HEALTH" | jq -r '.status // "error"')
if [ "$STATUS" != "ok" ] && [ "$STATUS" != "degraded" ]; then
    echo -e "   ${RED}✗ Health check failed: $HEALTH${NC}"
else
    echo -e "   ${GREEN}✓ API responding (status: $STATUS)${NC}"
fi
echo ""

# 3. Database connectivity
echo -e "${CYAN}[3] Database Connectivity${NC}"
if docker exec "$LSTM_CONTAINER" python3 -c "
import os
os.environ.setdefault('DB_HOST', 'postgres')
from app.database import Database
db = Database()
print('OK' if db.check_connection() else 'FAIL')
" 2>/dev/null | grep -q OK; then
    echo -e "   ${GREEN}✓ LSTM can connect to PostgreSQL${NC}"
else
    echo -e "   ${RED}✗ LSTM cannot connect to PostgreSQL${NC}"
fi
echo ""

# 4. Data availability per log type
echo -e "${CYAN}[4] Collected Data (min 10 rows needed for training)${NC}"
for lt in "${LOG_TYPES[@]}"; do
    [ -n "$TARGET_LOG_TYPE" ] && [ "$lt" != "$TARGET_LOG_TYPE" ] && continue
    RESP=$(docker exec "$LSTM_CONTAINER" curl -s "http://localhost:5000/learning/status/$lt" 2>/dev/null || echo '{}')
    DATA_SIZE=$(echo "$RESP" | jq -r '.data_size // 0')
    LEARNING=$(echo "$RESP" | jq -r '.learning_enabled // false')
    if [ "$DATA_SIZE" -ge 10 ]; then
        echo -e "   ${GREEN}✓ $lt: $DATA_SIZE rows (learning: $LEARNING)${NC}"
    elif [ "$DATA_SIZE" -gt 0 ]; then
        echo -e "   ${YELLOW}⚠ $lt: $DATA_SIZE rows - need 10+ (learning: $LEARNING)${NC}"
    else
        echo -e "   ${RED}✗ $lt: 0 rows - enable learning and collect data${NC}"
    fi
done
echo ""

# 5. Model directory and format
echo -e "${CYAN}[5] Model Directory & Format${NC}"
MODEL_PATH=$(docker exec "$LSTM_CONTAINER" printenv MODEL_PATH 2>/dev/null || echo "/app/models")
for lt in "${LOG_TYPES[@]}"; do
    [ -n "$TARGET_LOG_TYPE" ] && [ "$lt" != "$TARGET_LOG_TYPE" ] && continue
    LT_PATH="$MODEL_PATH/$lt"
    if docker exec "$LSTM_CONTAINER" test -d "$LT_PATH" 2>/dev/null; then
        KERAS=$(docker exec "$LSTM_CONTAINER" ls "$LT_PATH"/*.keras 2>/dev/null | wc -l)
        H5=$(docker exec "$LSTM_CONTAINER" ls "$LT_PATH"/*.h5 2>/dev/null | wc -l)
        echo -e "   $lt: ${GREEN}$LT_PATH${NC} (.keras: $KERAS, .h5: $H5)"
    else
        echo -e "   $lt: ${YELLOW}No model dir yet${NC}"
    fi
done
echo ""

# 6. Trigger training (if data available)
echo -e "${CYAN}[6] Training Test${NC}"
for lt in "${LOG_TYPES[@]}"; do
    [ -n "$TARGET_LOG_TYPE" ] && [ "$lt" != "$TARGET_LOG_TYPE" ] && continue
    RESP=$(docker exec "$LSTM_CONTAINER" curl -s "http://localhost:5000/learning/status/$lt" 2>/dev/null || echo '{}')
    DATA_SIZE=$(echo "$RESP" | jq -r '.data_size // 0')
    if [ "$DATA_SIZE" -lt 10 ]; then
        echo -e "   $lt: ${YELLOW}Skipping (need 10+ rows, have $DATA_SIZE)${NC}"
        continue
    fi
    echo -e "   $lt: Triggering training (disable-learn = start training)..."
    DISABLE_RESP=$(docker exec "$LSTM_CONTAINER" curl -s -X POST "http://localhost:5000/learning/disable/$lt" 2>/dev/null || echo '{}')
    TRAIN_STARTED=$(echo "$DISABLE_RESP" | jq -r '.training_started // false')
    if [ "$TRAIN_STARTED" = "true" ]; then
        echo -e "   ${GREEN}✓ Training started in background${NC}"
        echo -e "   Wait ~2-5 min, then check: curl -s http://localhost:5000/training/status/$lt | jq"
    else
        MSG=$(echo "$DISABLE_RESP" | jq -r '.message // .error // "unknown"')
        echo -e "   ${YELLOW}Response: $MSG${NC}"
    fi
done
echo ""

# 7. Recent logs (last 30 lines)
echo -e "${CYAN}[7] Recent LSTM Logs (errors/warnings)${NC}"
docker exec "$LSTM_CONTAINER" tail -50 /app/data/gunicorn-error.log 2>/dev/null | grep -E "ERROR|WARNING|Error|error" | tail -15 || echo "   (no error log or no matches)"
echo ""

# 8. Quick reference
echo -e "${BLUE}══════════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}Quick Commands:${NC}"
echo "  Enable learning:  curl -X POST http://localhost:5000/learning/enable/<conn|dns|http|ssl>"
echo "  Disable (train):  curl -X POST http://localhost:5000/learning/disable/<conn|dns|http|ssl>"
echo "  Training status:  curl -s http://localhost:5000/training/status/<log_type> | jq"
echo "  Model status:     curl -s http://localhost:5000/models/status | jq"
echo "  Full logs:        docker logs lstm-autoencoder 2>&1 | tail -100"
echo "  Error log:        docker exec lstm-autoencoder tail -100 /app/data/gunicorn-error.log"
echo -e "${BLUE}══════════════════════════════════════════════════════════════════${NC}"
