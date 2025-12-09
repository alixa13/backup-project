#!/bin/bash

# Compare Kafka messages with collected data
# Shows how many packets are in Kafka vs collected in API/buffer

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONTAINER=${KAFKA_CONTAINER:-"kafka"}
LSTM_API_URL=${LSTM_API_URL:-"http://localhost:5000"}
LOG_TYPES=("conn" "dns" "http" "ssl")

echo -e "${BLUE}==================================================================================${NC}"
echo -e "${GREEN}         Kafka Messages vs Collected Data Comparison${NC}"
echo -e "${BLUE}==================================================================================${NC}"
echo ""

# Function to get Kafka topic message count
get_kafka_message_count() {
    local topic=$1
    local count=0
    
    # Try to get the latest offset for the topic
    # Using kafka-console-consumer with --max-messages 0 to get offset info
    local output=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages 0 \
        --timeout-ms 1000 2>&1 | grep -oP 'Processed a total of \K[0-9]+' || echo "0")
    
    # Alternative method using kafka-consumer-groups if the above doesn't work
    if [ "$output" = "0" ] || [ -z "$output" ]; then
        # Try using kafka-run-class (if available)
        output=$(docker exec "$KAFKA_CONTAINER" bash -c "kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic $topic 2>/dev/null" | awk -F: '{sum += $NF} END {print sum}' || echo "0")
    fi
    
    # If still 0, try using kafka-topics to describe
    if [ "$output" = "0" ] || [ -z "$output" ]; then
        # This is less accurate but works
        output=$(docker exec "$KAFKA_CONTAINER" kafka-topics \
            --bootstrap-server localhost:9092 \
            --describe \
            --topic "$topic" 2>/dev/null | grep -c "Partition:" || echo "0")
        # Multiply by approximate average if we can't get exact count
        if [ "$output" != "0" ]; then
            output="~$output partitions (count unavailable)"
        fi
    fi
    
    echo "$output"
}

# Function to get collected data stats from API
get_collected_stats() {
    local log_type=$1
    local response=$(curl -s "${LSTM_API_URL}/learning/status/${log_type}")
    
    if [ $? -ne 0 ]; then
        echo "ERROR"
        return 1
    fi
    
    local learning_enabled=$(echo "$response" | jq -r '.learning_enabled // false')
    local data_size=$(echo "$response" | jq -r '.data_size // 0')
    local total_rows=$(echo "$response" | jq -r '.total_rows // 0')
    local buffer_size=$(echo "$response" | jq -r '.buffer_size // 0')
    local buffer_updated=$(echo "$response" | jq -r '.buffer_updated_at // "never"')
    
    echo "$learning_enabled|$data_size|$total_rows|$buffer_size|$buffer_updated"
}

# Check if Kafka container is running
if ! docker ps | grep -q "$KAFKA_CONTAINER"; then
    echo -e "${RED}Error: Kafka container is not running${NC}"
    exit 1
fi

# Check if LSTM API is accessible
if ! curl -s "${LSTM_API_URL}/health" >/dev/null 2>&1; then
    echo -e "${RED}Error: LSTM API is not accessible at ${LSTM_API_URL}${NC}"
    exit 1
fi

# Print header
printf "${CYAN}%-10s${NC} | ${CYAN}%-15s${NC} | ${CYAN}%-12s${NC} | ${CYAN}%-12s${NC} | ${CYAN}%-12s${NC} | ${CYAN}%-10s${NC} | ${CYAN}%-20s${NC}\n" \
    "Log Type" "Kafka Messages" "In Buffer" "In DB (sess)" "Total Proc." "Learning" "Buffer Updated"
echo "--------------------------------------------------------------------------------------------------------"

# Total counters
total_kafka=0
total_buffer=0
total_collected=0
total_processed=0

# Process each log type
for log_type in "${LOG_TYPES[@]}"; do
    kafka_topic="zeek-${log_type}"
    
    # Get Kafka message count
    kafka_count=$(get_kafka_message_count "$kafka_topic")
    
    # Get collected stats
    stats=$(get_collected_stats "$log_type")
    
    if [ "$stats" = "ERROR" ]; then
        printf "${YELLOW}%-10s${NC} | ${RED}%-15s${NC} | ${RED}%-12s${NC} | ${RED}%-12s${NC} | ${RED}%-12s${NC} | ${RED}%-10s${NC} | ${RED}%-20s${NC}\n" \
            "$log_type" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A"
        continue
    fi
    
    IFS='|' read -r learning_enabled data_size total_rows buffer_size buffer_updated <<< "$stats"
    
    # Color code based on learning status
    if [ "$learning_enabled" = "true" ]; then
        status_color="${GREEN}"
        status_text="ENABLED"
    else
        status_color="${YELLOW}"
        status_text="DISABLED"
    fi
    
    # Format buffer updated time
    if [ "$buffer_updated" != "never" ] && [ "$buffer_updated" != "null" ]; then
        buffer_time=$(date -d "$buffer_updated" "+%H:%M:%S" 2>/dev/null || echo "recent")
    else
        buffer_time="never"
    fi
    
    # Print row
    printf "${CYAN}%-10s${NC} | ${GREEN}%-15s${NC} | ${BLUE}%-12s${NC} | ${BLUE}%-12s${NC} | ${YELLOW}%-12s${NC} | ${status_color}%-10s${NC} | ${BLUE}%-20s${NC}\n" \
        "$log_type" "$kafka_count" "$buffer_size" "$data_size" "$total_rows" "$status_text" "$buffer_time"
    
    # Add to totals (skip if not numeric)
    if [[ "$kafka_count" =~ ^[0-9]+$ ]]; then
        total_kafka=$((total_kafka + kafka_count))
    fi
    total_buffer=$((total_buffer + buffer_size))
    total_collected=$((total_collected + data_size))
    total_processed=$((total_processed + total_rows))
done

echo "--------------------------------------------------------------------------------------------------------"
printf "${CYAN}%-10s${NC} | ${GREEN}%-15s${NC} | ${BLUE}%-12s${NC} | ${BLUE}%-12s${NC} | ${YELLOW}%-12s${NC} | ${CYAN}%-10s${NC} | ${CYAN}%-20s${NC}\n" \
    "TOTAL" "$total_kafka" "$total_buffer" "$total_collected" "$total_processed" "-" "-"

echo ""
echo -e "${BLUE}Legend:${NC}"
echo -e "  ${GREEN}Kafka Messages${NC}  - Total messages available in Kafka topic"
echo -e "  ${BLUE}In Buffer${NC}       - Records in Flink in-memory buffer (not yet sent to API)"
echo -e "  ${BLUE}In DB (sess)${NC}    - Records collected in current learning session (stored in DB)"
echo -e "  ${YELLOW}Total Proc.${NC}     - Total rows processed across all sessions (cumulative)"
echo -e "  ${GREEN}ENABLED${NC}         - Learning mode is active, collecting data"
echo -e "  ${YELLOW}DISABLED${NC}        - Learning mode is off, running predictions"
echo ""

# Calculate discrepancy
echo -e "${BLUE}Analysis:${NC}"
pending_total=$((total_buffer + total_collected))
if [[ "$total_kafka" =~ ^[0-9]+$ ]]; then
    if [ $total_kafka -gt $pending_total ]; then
        discrepancy=$((total_kafka - pending_total))
        echo -e "  ${YELLOW}⚠️  Kafka has ${discrepancy} more messages than collected (buffer + DB)${NC}"
        echo -e "     This is normal if:"
        echo -e "       - Learning was recently enabled"
        echo -e "       - Messages are still being processed"
        echo -e "       - Some messages failed processing"
    elif [ $pending_total -gt $total_kafka ]; then
        echo -e "  ${GREEN}✅ All Kafka messages have been processed${NC}"
    else
        echo -e "  ${GREEN}✅ Kafka messages match collected data (buffer + DB)${NC}"
    fi
else
    echo -e "  ${YELLOW}⚠️  Could not get exact Kafka message counts${NC}"
fi

echo ""
echo -e "${BLUE}==================================================================================${NC}"

