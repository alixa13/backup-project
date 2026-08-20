#!/bin/bash

# If we're on a host that only has Docker (no curl/jq/bc/compose),
# transparently re-run this script inside the helper ops image.
if [ -z "${RUN_IN_OPS:-}" ]; then
  OPS_IMAGE="${OPS_IMAGE:-project-ops:latest}"
  if docker image inspect "$OPS_IMAGE" >/dev/null 2>&1; then
    exec docker run --rm -it \
      -e RUN_IN_OPS=1 \
      -e OPS_IMAGE="$OPS_IMAGE" \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v "$PWD":"$PWD" -w "$PWD" \
      "$OPS_IMAGE" \
      bash "$0" "$@"
  fi
fi

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
DELETE_DATA=false
if [ "$1" = "--delete-data" ] || [ "$1" = "-d" ] || [ "$1" = "delete" ]; then
    DELETE_DATA=true
fi

echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
if [ "$DELETE_DATA" = true ]; then
    echo -e "${RED}  Stopping System & DELETING ALL DATA${NC}"
else
    echo -e "${BLUE}  Stopping System (Keeping Data)${NC}"
fi
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""

# Check if LSTM autoencoder container is running
if docker ps | grep -q lstm-autoencoder; then
    echo -e "${YELLOW}🛑 Stopping LSTM autoencoder Kafka processing...${NC}"
    # Use direct container access to stop Kafka processing
    docker exec lstm-autoencoder curl -X POST http://localhost:5000/kafka/stop 2>/dev/null || true
    
    # Wait a moment to ensure processing is stopped
    sleep 3
    echo -e "${GREEN}✅ LSTM Kafka processing stopped${NC}"
else
    echo -e "${BLUE}ℹ️  LSTM autoencoder container is not running${NC}"
fi

# Delete data if requested
if [ "$DELETE_DATA" = true ]; then
    echo ""
    echo -e "${RED}⚠️  DELETING ALL DATA - This cannot be undone!${NC}"
    echo ""
    
    # Delete Kafka topics
    if docker ps | grep -q kafka; then
        echo -e "${YELLOW}🗑️  Deleting Kafka topics...${NC}"
        
        # Get all zeek topics
        topics=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "zeek-|malicious-|supervised-" || true)
        
        if [ -n "$topics" ]; then
            for topic in $topics; do
                echo "   Deleting topic: $topic"
                docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
            done
            echo -e "${GREEN}✅ Kafka topics deleted${NC}"
        else
            echo -e "${BLUE}ℹ️  No Kafka topics to delete${NC}"
        fi
    fi
    
    # Delete PostgreSQL data
    if docker ps | grep -q postgres; then
        echo ""
        echo -e "${YELLOW}🗑️  Deleting PostgreSQL database data...${NC}"
        
        # Drop and recreate database
        docker exec postgres psql -U lstm_user -c "DROP DATABASE IF EXISTS lstm_db;" 2>/dev/null || true
        docker exec postgres psql -U lstm_user -c "CREATE DATABASE lstm_db OWNER lstm_user;" 2>/dev/null || true
        
        echo -e "${GREEN}✅ PostgreSQL database cleared${NC}"
    fi
    
    echo ""
fi

echo -e "${YELLOW}🛑 Stopping all Docker services...${NC}"
echo -e "${BLUE}   • PostgreSQL database${NC}"
echo -e "${BLUE}   • LSTM API${NC}"
echo -e "${BLUE}   • Flink${NC}"
echo -e "${BLUE}   • Kafka${NC}"
echo -e "${BLUE}   • Zeek${NC}"
echo -e "${BLUE}   • Supervised API${NC}"
echo -e "${BLUE}   • Zookeeper${NC}"
echo ""

if [ "$DELETE_DATA" = true ]; then
    # Stop and remove volumes
    docker compose down -v
    echo -e "${RED}✅ All containers stopped and volumes deleted${NC}"
else
    # Stop but keep volumes
docker compose down
    echo -e "${GREEN}✅ All containers stopped (data preserved)${NC}"
fi

# Remove the access-services.sh script if it exists
if [ -f "access-services.sh" ]; then
    rm access-services.sh
    echo -e "${GREEN}✅ Cleaned up helper scripts${NC}"
fi

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
if [ "$DELETE_DATA" = true ]; then
    echo -e "${RED}✅ System stopped and ALL DATA DELETED${NC}"
else
    echo -e "${GREEN}✅ System stopped (data preserved)${NC}"
fi
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""

if [ "$DELETE_DATA" = true ]; then
    echo -e "${YELLOW}To start fresh: ./start-system.sh${NC}"
else
    echo -e "${YELLOW}To start again: ./start-system.sh${NC}"
    echo -e "${BLUE}Data in PostgreSQL and Kafka will be preserved${NC}"
fi

echo ""
echo -e "${BLUE}Usage:${NC}"
echo -e "  ${GREEN}./stop-system.sh${NC}                  - Stop and keep data"
echo -e "  ${RED}./stop-system.sh --delete-data${NC}    - Stop and DELETE all data"
echo -e "  ${RED}./stop-system.sh -d${NC}               - Stop and DELETE all data (shorthand)"
echo -e "  ${RED}./stop-system.sh delete${NC}           - Stop and DELETE all data"
echo "" 