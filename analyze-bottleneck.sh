#!/bin/bash

# Bottleneck Analysis Script
# Analyzes monitor log files to identify performance bottlenecks

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Get log file
if [ -z "$1" ]; then
    # Find most recent log file
    LOG_FILE=$(ls -t monitor_log_*.txt 2>/dev/null | head -1)
    if [ -z "$LOG_FILE" ]; then
        echo -e "${RED}No monitor log files found${NC}"
        echo "Usage: $0 [log_file]"
        echo "Or run monitor-realtime.sh first to generate logs"
        exit 1
    fi
    echo -e "${CYAN}Using most recent log: ${LOG_FILE}${NC}"
else
    LOG_FILE="$1"
    if [ ! -f "$LOG_FILE" ]; then
        echo -e "${RED}Log file not found: ${LOG_FILE}${NC}"
        exit 1
    fi
fi

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    Bottleneck Analysis Report                            ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Extract key metrics
echo -e "${YELLOW}📊 Processing Metrics:${NC}"
echo ""

# Average ingestion rate
avg_ingestion=$(grep "Ingestion Rate:" "$LOG_FILE" | awk '{sum+=$3; count++} END {if(count>0) print int(sum/count); else print 0}')
max_ingestion=$(grep "Ingestion Rate:" "$LOG_FILE" | awk '{if($3>max) max=$3} END {print int(max)}')

# Average processing rate
avg_processing=$(grep "Processing Rate:" "$LOG_FILE" | awk '{sum+=$3; count++} END {if(count>0) print int(sum/count); else print 0}')
max_processing=$(grep "Processing Rate:" "$LOG_FILE" | awk '{if($3>max) max=$3} END {print int(max)}')

# Average lag
avg_lag=$(grep "Processing Lag:" "$LOG_FILE" | awk '{sum+=$3; count++} END {if(count>0) print int(sum/count); else print 0}')
max_lag=$(grep "Processing Lag:" "$LOG_FILE" | awk '{if($3>max) max=$3} END {print int(max)}')

# Average efficiency
avg_efficiency=$(grep "Processing Efficiency:" "$LOG_FILE" | awk '{gsub(/%/,""); sum+=$3; count++} END {if(count>0) print int(sum/count); else print 0}')

echo -e "  Avg Ingestion Rate:  ${GREEN}${avg_ingestion}${NC} logs/sec  (peak: ${max_ingestion})"
echo -e "  Avg Processing Rate: ${CYAN}${avg_processing}${NC} logs/sec  (peak: ${max_processing})"
echo -e "  Avg Efficiency:      ${YELLOW}${avg_efficiency}%${NC}"
echo -e "  Avg Lag:             ${avg_lag} messages  (peak: ${max_lag})"

# Calculate gap
gap=$((avg_ingestion - avg_processing))
if [ $gap -gt 0 ]; then
    echo -e "  ${RED}Gap (falling behind): ${gap} logs/sec${NC}"
else
    echo -e "  ${GREEN}✅ Processing keeping up${NC}"
fi

echo ""
echo -e "${YELLOW}🖥️  Container Resource Usage (Averages):${NC}"
echo ""

# Parse container stats
for container in lstm-autoencoder postgres flink kafka zeek; do
    cpu_values=$(grep "${container}: CPU=" "$LOG_FILE" | sed 's/.*CPU=//' | awk '{print $1}' | sed 's/%//' | grep -E '^[0-9.]+$')
    if [ -n "$cpu_values" ]; then
        avg_cpu=$(echo "$cpu_values" | awk '{sum+=$1; count++} END {if(count>0) printf "%.1f", sum/count; else print 0}')
        max_cpu=$(echo "$cpu_values" | awk 'BEGIN{max=0} {if($1>max) max=$1} END {printf "%.1f", max}')
        
        # Color code based on CPU usage
        if (( $(echo "$avg_cpu > 80" | bc -l 2>/dev/null || echo 0) )); then
            color="${RED}"
            status="🔴 BOTTLENECK"
        elif (( $(echo "$avg_cpu > 50" | bc -l 2>/dev/null || echo 0) )); then
            color="${YELLOW}"
            status="⚠️  High Load"
        else
            color="${GREEN}"
            status="✅ Normal"
        fi
        
        printf "  ${CYAN}%-18s${NC} Avg CPU: ${color}%5s%%${NC}  Peak: ${color}%5s%%${NC}  %s\n" \
            "$container" "$avg_cpu" "$max_cpu" "$status"
    fi
done

echo ""
echo -e "${YELLOW}🐘 PostgreSQL Performance:${NC}"
echo ""

# Extract PostgreSQL stats from log
pg_cache_hits=$(grep "cache_pct" "$LOG_FILE" | tail -1 | awk '{print $(NF)}' || echo "N/A")
pg_active_conns=$(grep "active" "$LOG_FILE" | grep -v "inactive" | tail -1 | awk '{print $2}' || echo "N/A")
pg_idle_conns=$(grep "idle" "$LOG_FILE" | tail -1 | awk '{print $2}' || echo "N/A")

echo "  Cache Hit Ratio:    ${pg_cache_hits}%"
echo "  Active Connections: ${pg_active_conns}"
echo "  Idle Connections:   ${pg_idle_conns}"

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                         Bottleneck Analysis                              ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Determine bottleneck
bottleneck_found=false

# Check LSTM CPU
lstm_cpu=$(grep "lstm-autoencoder: CPU=" "$LOG_FILE" | sed 's/.*CPU=//' | awk '{print $1}' | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
if [ -n "$lstm_cpu" ] && (( $(echo "$lstm_cpu > 80" | bc -l 2>/dev/null || echo 0) )); then
    echo -e "${RED}🔴 BOTTLENECK FOUND: LSTM API CPU (${lstm_cpu}%)${NC}"
    echo "   Problem: LSTM container CPU maxed out"
    echo "   Solution:"
    echo "     - Increase workers to 16-20"
    echo "     - Allocate more CPU cores (16+)"
    echo "     - Optimize NumPy serialization"
    echo ""
    bottleneck_found=true
fi

# Check PostgreSQL CPU
pg_cpu=$(grep "postgres: CPU=" "$LOG_FILE" | sed 's/.*CPU=//' | awk '{print $1}' | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
if [ -n "$pg_cpu" ] && (( $(echo "$pg_cpu > 80" | bc -l 2>/dev/null || echo 0) )); then
    echo -e "${RED}🔴 BOTTLENECK FOUND: PostgreSQL CPU (${pg_cpu}%)${NC}"
    echo "   Problem: Database CPU maxed out"
    echo "   Solution:"
    echo "     - Increase PostgreSQL CPU allocation"
    echo "     - Enable async_commit for faster writes"
    echo "     - Add more database connections"
    echo ""
    bottleneck_found=true
fi

# Check Flink CPU
flink_cpu=$(grep "flink: CPU=" "$LOG_FILE" | sed 's/.*CPU=//' | awk '{print $1}' | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
if [ -n "$flink_cpu" ] && (( $(echo "$flink_cpu > 80" | bc -l 2>/dev/null || echo 0) )); then
    echo -e "${RED}🔴 BOTTLENECK FOUND: Flink CPU (${flink_cpu}%)${NC}"
    echo "   Problem: Stream processing CPU maxed out"
    echo "   Solution:"
    echo "     - Increase Flink parallelism"
    echo "     - Add more task slots"
    echo "     - Optimize Flink configuration"
    echo ""
    bottleneck_found=true
fi

# Check Kafka CPU
kafka_cpu=$(grep "kafka: CPU=" "$LOG_FILE" | sed 's/.*CPU=//' | awk '{print $1}' | sed 's/%//' | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print 0}')
if [ -n "$kafka_cpu" ] && (( $(echo "$kafka_cpu > 400" | bc -l 2>/dev/null || echo 0) )); then
    echo -e "${RED}🔴 BOTTLENECK FOUND: Kafka CPU (${kafka_cpu}%)${NC}"
    echo "   Problem: Kafka broker CPU very high"
    echo "   Solution:"
    echo "     - This is somewhat normal for high throughput"
    echo "     - Increase Kafka CPU allocation if > 600%"
    echo ""
    bottleneck_found=true
fi

# Check processing gap
if [ $gap -gt 1000 ]; then
    echo -e "${RED}🔴 BOTTLENECK: Processing can't keep up with ingestion${NC}"
    echo "   Gap: ${gap} logs/sec"
    echo "   Ingestion: ${avg_ingestion} logs/sec"
    echo "   Processing: ${avg_processing} logs/sec"
    echo "   Efficiency: ${avg_efficiency}%"
    echo ""
    bottleneck_found=true
fi

if [ "$bottleneck_found" = false ]; then
    echo -e "${GREEN}✅ No obvious bottlenecks detected!${NC}"
    echo -e "${GREEN}   System is handling the load well.${NC}"
    if [ $avg_efficiency -ge 90 ]; then
        echo -e "${GREEN}   Efficiency is excellent (${avg_efficiency}%)${NC}"
    elif [ $avg_efficiency -ge 70 ]; then
        echo -e "${YELLOW}   Efficiency is acceptable (${avg_efficiency}%)${NC}"
    else
        echo -e "${RED}   Efficiency is low (${avg_efficiency}%)${NC}"
        echo -e "${YELLOW}   Check for network issues or API errors${NC}"
    fi
fi

echo ""
echo -e "${CYAN}📁 Full details saved in: ${LOG_FILE}${NC}"
echo ""

