#!/bin/bash
#
# LSTM Model Reload Script
# Reloads trained models without restarting the container
#

LSTM_API_URL="${LSTM_API_URL:-http://localhost:5000}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=========================================="
echo "  LSTM Model Reload Utility"
echo "=========================================="
echo ""

# Check if a specific log type was provided
if [ $# -eq 1 ]; then
    LOG_TYPE=$1
    
    # Validate log type
    if [[ ! "$LOG_TYPE" =~ ^(conn|dns|http|ssl)$ ]]; then
        echo -e "${RED}Error: Invalid log type '$LOG_TYPE'${NC}"
        echo "Valid log types: conn, dns, http, ssl"
        exit 1
    fi
    
    echo "Reloading model for: $LOG_TYPE"
    echo ""
    
    # Reload specific model
    RESPONSE=$(curl -s -X POST "$LSTM_API_URL/models/reload/$LOG_TYPE")
    STATUS=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status', 'error'))")
    
    if [ "$STATUS" = "success" ]; then
        echo -e "${GREEN}✓ Model reloaded successfully for $LOG_TYPE${NC}"
        echo "$RESPONSE" | python3 -m json.tool
    else
        echo -e "${RED}✗ Failed to reload model for $LOG_TYPE${NC}"
        echo "$RESPONSE" | python3 -m json.tool
        exit 1
    fi
else
    # Reload all models
    echo "Reloading all models..."
    echo ""
    
    RESPONSE=$(curl -s -X POST "$LSTM_API_URL/models/reload")
    STATUS=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status', 'error'))")
    ALL_LOADED=$(echo "$RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('all_models_loaded', False))")
    
    if [ "$STATUS" = "success" ] && [ "$ALL_LOADED" = "True" ]; then
        echo -e "${GREEN}✓ All models reloaded successfully${NC}"
        echo ""
        echo "$RESPONSE" | python3 -m json.tool
    else
        echo -e "${YELLOW}⚠ Some models may not have loaded${NC}"
        echo ""
        echo "$RESPONSE" | python3 -m json.tool
        exit 1
    fi
fi

echo ""
echo "=========================================="
echo "  Reload Complete"
echo "=========================================="

