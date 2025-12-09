#!/bin/bash

# Real-time monitoring script for high-volume packet processing
# Shows Kafka ingestion, Flink buffer, API processing rates + container resources
# Logs everything to file for bottleneck analysis

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

KAFKA_CONTAINER="kafka"
LSTM_CONTAINER="lstm-autoencoder"
POSTGRES_CONTAINER="postgres"
FLINK_CONTAINER="flink"
ZEEK_CONTAINER="zeek"
LOG_TYPES=("conn" "dns" "http" "ssl")

# Create log file with timestamp
LOG_FILE="monitor_log_$(date '+%Y%m%d_%H%M%S').txt"
echo "Performance Monitoring Log - Started at $(date)" > "$LOG_FILE"
echo "==========================================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Get initial counts
declare -A prev_kafka_count
declare -A prev_db_count

echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║    Real-Time Monitor with Resource Logging (Press Ctrl+C to stop)        ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${CYAN}📁 Logging to: ${LOG_FILE}${NC}"
echo -e "${YELLOW}Starting in 3 seconds... Start your tcpreplay now!${NC}"
sleep 3

# Initialize previous counts
for log_type in "${LOG_TYPES[@]}"; do
    prev_kafka_count[$log_type]=$(docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic zeek-$log_type --time -1 2>/dev/null | awk -F: '{sum+=$NF} END {print (sum==""?0:sum)}')
    prev_db_count[$log_type]=0
done

iteration=0
while true; do
    iteration=$((iteration + 1))
    timestamp=$(date '+%H:%M:%S')
    full_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Log separator to file
    echo "==================== Update #$iteration @ $full_timestamp ====================" >> "$LOG_FILE"
    
    # Get container resource stats (simplified parsing)
    container_stats=""
    for container in lstm-autoencoder postgres flink kafka zeek; do
        if docker ps --format "{{.Names}}" | grep -q "^${container}$"; then
            stats_line=$(docker stats --no-stream "$container" 2>/dev/null | tail -1)
            if [ -n "$stats_line" ]; then
                # Extract fields: NAME CPU% MEM_USAGE MEM_LIMIT MEM% NET_I/O BLOCK_I/O PIDS
                name=$(echo "$stats_line" | awk '{print $2}')
                cpu=$(echo "$stats_line" | awk '{print $3}')
                mem=$(echo "$stats_line" | awk '{print $4}')
                net=$(echo "$stats_line" | awk '{print $8}')
                disk=$(echo "$stats_line" | awk '{print $10}')
                pids=$(echo "$stats_line" | awk '{print $NF}')
                container_stats+="${name}\t${cpu}\t${mem}\t${net}\t${disk}\t${pids}\n"
            fi
        fi
    done
    
    # Get PostgreSQL stats
    pg_stats=$(docker exec $POSTGRES_CONTAINER psql -U lstm_user -d lstm_db -t -c "
    SELECT 
        numbackends as connections,
        xact_commit as commits,
        xact_rollback as rollbacks,
        blks_read as disk_reads,
        blks_hit as cache_hits,
        ROUND(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) as cache_pct
    FROM pg_stat_database 
    WHERE datname = 'lstm_db';
    " 2>/dev/null | xargs)
    
    # Get active PostgreSQL connections by state
    pg_conn_states=$(docker exec $POSTGRES_CONTAINER psql -U lstm_user -d lstm_db -t -c "
    SELECT state, count(*) FROM pg_stat_activity WHERE datname = 'lstm_db' GROUP BY state;
    " 2>/dev/null | tr '\n' ' ')
    
    # Clear screen (move cursor up)
    if [ $iteration -gt 1 ]; then
        for ((i=0; i<50; i++)); do
            echo -en "\033[1A\033[2K"
        done
    fi
    
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║ Update #$iteration @ $timestamp                                                    ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    # Table header
    printf "${BLUE}%-8s${NC} | ${BLUE}%-10s${NC} | ${BLUE}%-8s${NC} | ${BLUE}%-8s${NC} | ${BLUE}%-10s${NC} | ${BLUE}%-10s${NC} | ${BLUE}%-8s${NC}\n" \
        "Type" "Kafka Msgs" "Rate/s" "Buffer" "DB (sess)" "Rate/s" "Learning"
    echo "────────────────────────────────────────────────────────────────────────────────"
    
    total_kafka=0
    total_buffer=0
    total_db=0
    total_kafka_rate=0
    total_db_rate=0
    
    for log_type in "${LOG_TYPES[@]}"; do
        # Get Kafka count
        kafka_count=$(docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic zeek-$log_type --time -1 2>/dev/null | awk -F: '{sum+=$NF} END {print (sum==""?0:sum)}')
        
        # Get API stats
        api_response=$(docker exec $LSTM_CONTAINER curl -s http://localhost:5000/learning/status/$log_type)
        learning_enabled=$(echo "$api_response" | jq -r '.learning_enabled // false')
        buffer_size=$(echo "$api_response" | jq -r '.buffer_size // 0')
        db_size=$(echo "$api_response" | jq -r '.data_size // 0')
        
        # Calculate rates
        kafka_rate=$((kafka_count - prev_kafka_count[$log_type]))
        db_rate=$((db_size - prev_db_count[$log_type]))
        
        # Update previous counts
        prev_kafka_count[$log_type]=$kafka_count
        prev_db_count[$log_type]=$db_size
        
        # Color code learning status
        if [ "$learning_enabled" = "true" ]; then
            status_color="${GREEN}"
            status_text="ENABLED "
        else
            status_color="${YELLOW}"
            status_text="DISABLED"
        fi
        
        # Color code rates
        if [ $kafka_rate -gt 50 ]; then
            kafka_rate_color="${GREEN}"
        elif [ $kafka_rate -gt 10 ]; then
            kafka_rate_color="${YELLOW}"
        else
            kafka_rate_color="${NC}"
        fi
        
        if [ $db_rate -gt 50 ]; then
            db_rate_color="${GREEN}"
        elif [ $db_rate -gt 10 ]; then
            db_rate_color="${YELLOW}"
        else
            db_rate_color="${NC}"
        fi
        
        # Color code buffer
        if [ $buffer_size -gt 400 ]; then
            buffer_color="${RED}"
        elif [ $buffer_size -gt 200 ]; then
            buffer_color="${YELLOW}"
        else
            buffer_color="${BLUE}"
        fi
        
        # Print row
        printf "${CYAN}%-8s${NC} | ${NC}%-10s${NC} | ${kafka_rate_color}+%-7s${NC} | ${buffer_color}%-8s${NC} | ${NC}%-10s${NC} | ${db_rate_color}+%-9s${NC} | ${status_color}%-8s${NC}\n" \
            "$log_type" "$kafka_count" "$kafka_rate" "$buffer_size" "$db_size" "$db_rate" "$status_text"
        
        # Accumulate totals
        total_kafka=$((total_kafka + kafka_count))
        total_buffer=$((total_buffer + buffer_size))
        total_db=$((total_db + db_size))
        total_kafka_rate=$((total_kafka_rate + kafka_rate))
        total_db_rate=$((total_db_rate + db_rate))
    done
    
    echo "────────────────────────────────────────────────────────────────────────────────"
    printf "${MAGENTA}%-8s${NC} | ${MAGENTA}%-10s${NC} | ${MAGENTA}+%-7s${NC} | ${MAGENTA}%-8s${NC} | ${MAGENTA}%-10s${NC} | ${MAGENTA}+%-9s${NC} | ${MAGENTA}%-8s${NC}\n" \
        "TOTAL" "$total_kafka" "$total_kafka_rate" "$total_buffer" "$total_db" "$total_db_rate" "-"
    
    echo ""
    echo -e "${BLUE}📊 Performance Indicators:${NC}"
    
    # Calculate processing efficiency
    pending=$((total_buffer + (total_kafka - total_db)))
    if [ $total_kafka -gt 0 ]; then
        efficiency=$((total_db * 100 / total_kafka))
    else
        efficiency=0
    fi
    
    # Processing lag
    lag=$((total_kafka - total_db - total_buffer))
    
    echo -e "   ${CYAN}Ingestion Rate:${NC} ${kafka_rate_color}${total_kafka_rate} logs/sec${NC}"
    echo -e "   ${CYAN}Processing Rate:${NC} ${db_rate_color}${total_db_rate} logs/sec${NC}"
    echo -e "   ${CYAN}Buffered Records:${NC} ${buffer_color}${total_buffer}${NC}"
    echo -e "   ${CYAN}Processing Efficiency:${NC} ${efficiency}%"
    
    if [ $lag -gt 100 ]; then
        echo -e "   ${RED}⚠️  Processing Lag:${NC} ${RED}${lag} messages behind${NC}"
    elif [ $lag -gt 10 ]; then
        echo -e "   ${YELLOW}⚠️  Processing Lag:${NC} ${YELLOW}${lag} messages behind${NC}"
    else
        echo -e "   ${GREEN}✅ Processing:${NC} ${GREEN}Real-time (lag: ${lag})${NC}"
    fi
    
    # System health indicators
    echo ""
    echo -e "${BLUE}🔧 System Health:${NC}"
    
    if [ $total_buffer -gt 400 ]; then
        echo -e "   ${RED}⚠️  High buffer usage - system under heavy load${NC}"
    elif [ $total_buffer -gt 200 ]; then
        echo -e "   ${YELLOW}⚠️  Moderate buffer usage - handling load${NC}"
    else
        echo -e "   ${GREEN}✅ Buffer levels normal${NC}"
    fi
    
    if [ $total_kafka_rate -gt $total_db_rate ] && [ $iteration -gt 2 ]; then
        backlog=$((total_kafka_rate - total_db_rate))
        echo -e "   ${YELLOW}📈 Ingestion faster than processing (+${backlog}/s)${NC}"
    elif [ $total_db_rate -gt 0 ]; then
        echo -e "   ${GREEN}✅ Processing keeping up with ingestion${NC}"
    fi
    
    # Display and log container resources
    echo ""
    echo -e "${BLUE}🖥️  Container Resources:${NC}"
    
    if [ -z "$container_stats" ]; then
        echo -e "   ${YELLOW}⚠️  Unable to fetch container stats${NC}"
    else
        echo -e "$container_stats" | while IFS=$'\t' read -r container cpu mem net_io block_io pids; do
            if [ -n "$container" ] && [ "$container" != "" ]; then
                # Color code CPU usage
                cpu_num=$(echo "$cpu" | sed 's/%//' | sed 's/[^0-9.]//g')
                if [ -n "$cpu_num" ] && (( $(echo "$cpu_num > 80" | bc -l 2>/dev/null || echo 0) )); then
                    cpu_color="${RED}"
                elif [ -n "$cpu_num" ] && (( $(echo "$cpu_num > 50" | bc -l 2>/dev/null || echo 0) )); then
                    cpu_color="${YELLOW}"
                else
                    cpu_color="${GREEN}"
                fi
                
                printf "   ${CYAN}%-18s${NC} CPU:${cpu_color}%7s${NC} MEM:%-12s NET:%-18s DISK:%-15s\n" \
                    "$container" "$cpu" "$mem" "$net_io" "$block_io"
            fi
        done
    fi
    
    # Display PostgreSQL stats
    echo ""
    echo -e "${BLUE}🐘 PostgreSQL Stats:${NC}"
    echo "   Connections: $pg_conn_states"
    echo "   Performance: $pg_stats"
    
    echo ""
    echo -e "${YELLOW}Refreshing in 1 second... (Press Ctrl+C to stop)${NC}"
    
    # === LOG TO FILE ===
    {
        echo ""
        echo "--- Processing Stats ---"
        echo "Ingestion Rate: ${total_kafka_rate} logs/sec"
        echo "Processing Rate: ${total_db_rate} logs/sec"
        echo "Buffered Records: ${total_buffer}"
        echo "Processing Efficiency: ${efficiency}%"
        echo "Processing Lag: ${lag} messages"
        echo ""
        echo "--- Per-Type Stats ---"
        for log_type in "${LOG_TYPES[@]}"; do
            kafka_count=$(docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic zeek-$log_type --time -1 2>/dev/null | awk -F: '{print $NF}')
            api_response=$(docker exec $LSTM_CONTAINER curl -s http://localhost:5000/learning/status/$log_type)
            buffer_size=$(echo "$api_response" | jq -r '.buffer_size // 0')
            db_size=$(echo "$api_response" | jq -r '.data_size // 0')
            echo "${log_type}: Kafka=${kafka_count} Buffer=${buffer_size} DB=${db_size}"
        done
        echo ""
        echo "--- Container Resources ---"
        echo -e "$container_stats" | while IFS=$'\t' read -r container cpu mem net_io block_io pids; do
            if [ -n "$container" ]; then
                echo "${container}: CPU=${cpu} MEM=${mem} NET=${net_io} DISK=${block_io} PIDs=${pids}"
            fi
        done
        echo ""
        echo "--- PostgreSQL Stats ---"
        echo "Connection States: $pg_conn_states"
        echo "Performance: $pg_stats"
        echo ""
    } >> "$LOG_FILE"
    
    sleep 1
done

