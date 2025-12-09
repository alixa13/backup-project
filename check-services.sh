#!/bin/bash

# Environment variables
LSTM_CONTAINER=${LSTM_CONTAINER:-"lstm-autoencoder"}
SUPERVISED_CONTAINER=${SUPERVISED_CONTAINER:-"supervised"}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-"kafka"}
ZOOKEEPER_CONTAINER=${ZOOKEEPER_CONTAINER:-"zookeeper"}
FLINK_CONTAINER=${FLINK_CONTAINER:-"flink"}
ZEEK_CONTAINER=${ZEEK_CONTAINER:-"zeek"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Checking status of all services...${NC}\n"

# Function to check if a container is running
check_container() {
    local container=$1
    local name=${2:-$container}
    if docker ps | grep -q "$container"; then
        echo -e "${GREEN}✅ $name is running${NC}"
        return 0
    else
        echo -e "${RED}❌ $name is not running${NC}"
        return 1
    fi
}

# Function to get container IP
get_container_ip() {
    local container=$1
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container" 2>/dev/null || echo "N/A"
}

# Check if containers are running
echo -e "${YELLOW}Container Status:${NC}"
echo "----------------"
check_container "$ZOOKEEPER_CONTAINER" "Zookeeper"
check_container "$KAFKA_CONTAINER" "Kafka"
check_container "$FLINK_CONTAINER" "Flink"
check_container "$SUPERVISED_CONTAINER" "Supervised ML"
check_container "$LSTM_CONTAINER" "LSTM Autoencoder"
check_container "$ZEEK_CONTAINER" "Zeek"

# Get container IPs if they are running
echo -e "\n${YELLOW}Container IP Addresses:${NC}"
echo "----------------------"
for container in "$ZOOKEEPER_CONTAINER" "$KAFKA_CONTAINER" "$FLINK_CONTAINER" "$SUPERVISED_CONTAINER" "$LSTM_CONTAINER" "$ZEEK_CONTAINER"; do
    if docker ps | grep -q "$container"; then
        ip=$(get_container_ip "$container")
        echo -e "${GREEN}$container:${NC} $ip"
    fi
done

# Check Kafka topics
echo -e "\n${YELLOW}Kafka Topics:${NC}"
echo "-------------"
if check_container "$KAFKA_CONTAINER" > /dev/null; then
    echo -e "${GREEN}Available topics:${NC}"
    if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null; then
        echo "Topics listed successfully"
    else
        echo -e "${RED}Failed to list Kafka topics. Check Kafka container logs:${NC}"
        echo "docker logs $KAFKA_CONTAINER"
    fi
else
    echo -e "${RED}Cannot check Kafka topics because the Kafka container is not running${NC}"
fi

# Check LSTM autoencoder learning status
echo -e "\n${YELLOW}LSTM Autoencoder Status:${NC}"
echo "------------------------"
if check_container "$LSTM_CONTAINER" > /dev/null; then
    echo -e "${GREEN}Learning status for all log types:${NC}"
    for log_type in "conn" "http" "dns" "ssl"; do
        echo -e "${YELLOW}  $log_type:${NC}"
        docker exec "$LSTM_CONTAINER" curl -s "http://localhost:5000/learning/status/$log_type" | jq .
    done
    echo -e "\n${GREEN}Model status:${NC}"
    docker exec "$LSTM_CONTAINER" curl -s http://localhost:5000/models/status | jq .
else
    echo -e "${RED}Cannot check LSTM status because the container is not running${NC}"
fi

# Check Flink jobs
echo -e "\n${YELLOW}Flink Jobs:${NC}"
echo "-----------"
if check_container "$FLINK_CONTAINER" > /dev/null; then
    echo -e "${GREEN}Running jobs:${NC}"
    if docker exec "$FLINK_CONTAINER" flink list 2>/dev/null; then
        echo -e "\n${GREEN}Job Manager:${NC}"
        echo "  - UI: http://localhost:8081"
        echo "  - Container: http://$(get_container_ip "$FLINK_CONTAINER"):8081"
    else
        echo -e "${YELLOW}No jobs running${NC}"
    fi
else
    echo -e "${RED}Cannot check Flink jobs because the Flink container is not running${NC}"
fi

echo -e "\n${YELLOW}Helpful Commands:${NC}"
echo "----------------"
echo "• View container logs: docker compose logs -f [service_name]"
echo "• Access LSTM service: ./access-services.sh lstm [GET|POST] [endpoint]"
echo "• Access Supervised ML: ./access-services.sh supervised [GET|POST] [endpoint]"
echo "• Manage trusted IPs: ./trusted-ip.sh [add|remove|list] [ip-address]"
echo "• Control LSTM: ./lstm-control.sh [command]"
echo "• Stop system: ./stop-system.sh" 