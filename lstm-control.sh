#!/bin/bash

# LSTM Autoencoder Control Script
# -------------------------------------------------------------
# This script provides a command-line interface to control
# the LSTM autoencoder service for anomaly detection in
# network traffic.
#
# You can use it in two ways:
#   1) **Named commands** (original behavior), for example:
#        ./lstm-control.sh health
#        ./lstm-control.sh enable-learn http
#        ./lstm-control.sh status conn
#        ./lstm-control.sh monitor-all
#   2) **Numeric shortcuts** (new), for example:
#        ./lstm-control.sh 1              # health
#        ./lstm-control.sh 2 http        # enable-learn http
#        ./lstm-control.sh 3 http        # disable-learn http
#        ./lstm-control.sh 4 http        # status http
#        ./lstm-control.sh 5 http        # model-status http
#        ./lstm-control.sh 6             # monitor-all
#      Or just run with no arguments for an interactive menu:
#        ./lstm-control.sh
# -------------------------------------------------------------

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


# Environment variables with defaults
LSTM_CONTAINER=${LSTM_CONTAINER:-"lstm-autoencoder"}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-"kafka"}
LSTM_PORT=${LSTM_PORT:-"5000"}
API_BASE_URL="http://localhost:${LSTM_PORT}"
TRAINING_THRESHOLD=20000
DEFAULT_LOG_TYPE="http"  # Default log type if not specified

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to print error messages and exit
print_error() {
    print_message "$RED" "❌ Error: $1"
    exit 1
}

# Function to print success messages
print_success() {
    print_message "$GREEN" "✅ $1"
}

# Function to print info messages
print_info() {
    print_message "$BLUE" "ℹ️ $1"
}

# Function to print warning messages
print_warning() {
    print_message "$YELLOW" "⚠️ $1"
}

# Function to check if LSTM container is running
check_container() {
    if ! docker ps | grep -q "$LSTM_CONTAINER"; then
        print_error "$LSTM_CONTAINER container is not running. Please start the system first with ./start-system.sh"
    fi
}

# Function to check if Kafka container is running (for topic-counts)
check_kafka() {
    if ! docker ps | grep -q "$KAFKA_CONTAINER"; then
        print_error "$KAFKA_CONTAINER container is not running. Start the system with ./start-system.sh"
    fi
}

# Get total message count (latest offset sum) for a Kafka topic
get_topic_count() {
    local topic=$1
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic "$topic" --time -1 2>/dev/null \
        | awk -F: '{sum+=$NF} END {print (sum==""?0:sum)}'
}

# Export a single Kafka topic to a text file (one message per line)
# Usage: export_topic_to_file <topic> <output_file> [max_messages] [dedup: 0|1]
export_topic_to_file() {
    local topic=$1
    local outfile=$2
    local max_msgs=${3:-0}
    local dedup=${4:-0}
    local count
    count=$(get_topic_count "$topic")
    [ -z "$count" ] && count=0
    if [ "$count" -eq 0 ]; then
        print_info "  $topic: 0 records, skipping $outfile"
        return 0
    fi
    # If max_messages given, cap at that; otherwise use actual count (slightly over to catch in-flight)
    local to_consume=$count
    [ "$max_msgs" -gt 0 ] && [ "$to_consume" -gt "$max_msgs" ] && to_consume=$max_msgs
    print_info "  $topic → $outfile ($to_consume records)"
    local rawfile="${outfile}.tmp.$$"
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages "$to_consume" \
        2>/dev/null > "$rawfile" || true
    if [ "$dedup" = "1" ]; then
        # Keep first occurrence of each unique line (removes exact duplicates)
        awk '!seen[$0]++' "$rawfile" > "$outfile"
        rm -f "$rawfile"
        local total=$(wc -l < "$outfile" 2>/dev/null || echo 0)
        print_success "    wrote $total unique lines to $outfile (duplicates removed)"
    else
        mv "$rawfile" "$outfile"
        local written=$(wc -l < "$outfile" 2>/dev/null || echo 0)
        print_success "    wrote $written lines to $outfile"
    fi
}

# Export all malicious-* and supervised-unsw42 topics to separate text files
# malicious-conn → conn-anomaly.txt, malicious-http → http-anomaly.txt, etc.
# Set TOPIC_EXPORT_UNIQUE=1 to remove exact duplicate lines (same UID/event can appear multiple times in Kafka).
export_topics_to_files() {
    local outdir="${1:-.}"
    local dedup=0
    [ "${TOPIC_EXPORT_UNIQUE:-0}" = "1" ] && dedup=1
    mkdir -p "$outdir"
    print_info "Exporting Kafka topics to $outdir/"
    [ "$dedup" = "1" ] && print_info "Deduplication: ON (unique lines only)"
    echo ""
    print_message "$BLUE" "MALICIOUS TOPICS"
    echo "----------------------------------------------------------------------"
    export_topic_to_file "malicious-conn"  "$outdir/conn-anomaly.txt" 0 "$dedup"
    export_topic_to_file "malicious-http"  "$outdir/http-anomaly.txt" 0 "$dedup"
    export_topic_to_file "malicious-dns"   "$outdir/dns-anomaly.txt" 0 "$dedup"
    export_topic_to_file "malicious-ssl"   "$outdir/ssl-anomaly.txt" 0 "$dedup"
    echo "----------------------------------------------------------------------"
    print_message "$BLUE" "SUPERVISED TOPIC"
    echo "----------------------------------------------------------------------"
    export_topic_to_file "supervised-unsw42" "$outdir/supervised-unsw42.txt" 0 "$dedup"
    echo "----------------------------------------------------------------------"
    print_success "Topic export finished. Files in $outdir/"
}

# Function to check service health
check_health() {
    local response
    response=$(docker exec "$LSTM_CONTAINER" curl -s "${API_BASE_URL}/health")
    if ! echo "$response" | jq -e '.status == "healthy"' >/dev/null; then
        print_error "Service is not healthy. Response: $response"
    fi
}

# Function to make API calls with error handling
call_api() {
    local endpoint=$1
    local method=${2:-GET}
    local data=$3
    local response
    
    # Construct the full URL
    local url="${API_BASE_URL}/${endpoint}"
    
    # Make the API call
    if [ -n "$data" ]; then
        response=$(docker exec "$LSTM_CONTAINER" curl -s -X "$method" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$url")
    else
        response=$(docker exec "$LSTM_CONTAINER" curl -s -X "$method" \
            "$url")
    fi
    
    # Check if the API call was successful
    if [ $? -ne 0 ]; then
        print_error "Failed to communicate with $LSTM_CONTAINER service"
    fi
    
    # Check if the response is valid JSON
    if ! echo "$response" | jq . >/dev/null 2>&1; then
        print_error "Invalid response from server: $response"
    fi
    
    echo "$response"
}

# Function to display a progress bar
display_progress_bar() {
    local progress=$1
    local width=40
    local completed=$((width * progress / 100))
    local remaining=$((width - completed))
    
    printf "["
    printf "%${completed}s" | tr " " "#"
    printf "%${remaining}s" | tr " " "-"
    printf "] %d%%\n" "$progress"
}

# Function to format seconds to human-readable time
format_time() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    
    if [ $hours -gt 0 ]; then
        printf "%dh %dm %ds" $hours $minutes $secs
    elif [ $minutes -gt 0 ]; then
        printf "%dm %ds" $minutes $secs
    else
        printf "%ds" $secs
    fi
}

# Function to monitor training progress
monitor_training_progress() {
    local log_type=$1
    local last_lines=12
    
    # Give training a moment to initialize
    sleep 2
    
    while true; do
        # Get training progress
        response=$(call_api "training/progress/$log_type")
        training_active=$(echo "$response" | jq -r '.training_active // false')
        
        if [ "$training_active" != "true" ]; then
            # Training completed or not active
            progress_status=$(echo "$response" | jq -r '.progress.status // "unknown"')
            if [ "$progress_status" = "completed" ]; then
                # Clear previous lines and show completion
                for ((i=0; i<$last_lines; i++)); do
                    echo -en "\033[1A\033[2K"
                done
                print_success "Training completed!"
                break
            elif [ "$progress_status" = "error" ]; then
                for ((i=0; i<$last_lines; i++)); do
                    echo -en "\033[1A\033[2K"
                done
                error_msg=$(echo "$response" | jq -r '.progress.error // "Unknown error"')
                print_error "Training failed: $error_msg"
                break
            else
                # Training hasn't started yet or just finished
                sleep 2
                continue
            fi
        fi
        
        # Extract progress details
        current_epoch=$(echo "$response" | jq -r '.progress.current_epoch // 0')
        total_epochs=$(echo "$response" | jq -r '.progress.total_epochs // 100')
        progress_pct=$(echo "$response" | jq -r '.progress.progress_pct // 0')
        loss=$(echo "$response" | jq -r '.progress.loss // 0')
        val_loss=$(echo "$response" | jq -r '.progress.val_loss // 0')
        elapsed=$(echo "$response" | jq -r '.progress.elapsed_seconds // 0')
        eta=$(echo "$response" | jq -r '.progress.eta_seconds // 0')
        
        # Move cursor up and clear previous block
        for ((i=0; i<$last_lines; i++)); do
            echo -en "\033[1A\033[2K"
        done
        
        # Print progress block
        echo "========================================================================"
        print_message "$GREEN" "🔄 TRAINING IN PROGRESS - ${log_type^^}"
        echo "========================================================================"
        print_message "$BLUE" "Epoch: $current_epoch / $total_epochs"
        print_message "$BLUE" "Progress:"
        printf "  "
        display_progress_bar "$progress_pct"
        print_message "$BLUE" "Loss: $(printf "%.6f" "$loss") | Val Loss: $(printf "%.6f" "$val_loss")"
        print_message "$BLUE" "Elapsed: $(format_time "$elapsed")"
        if [ "$eta" -gt 0 ]; then
            print_message "$BLUE" "ETA: $(format_time "$eta")"
        fi
        echo "========================================================================"
        echo ""
        
        sleep 2
    done
}

# Function to monitor learning status for a single log type
monitor_learning() {
    local pid=$1
    local log_type=${2:-$DEFAULT_LOG_TYPE}
    print_info "Starting learning mode monitor for $log_type (Press Ctrl+C to stop monitoring)"
    print_info "Training will start automatically at $TRAINING_THRESHOLD rows"
    echo

    local last_lines=9  # Increased by 1 for total rows display

    while true; do
        # Get current timestamp
        timestamp=$(date '+%Y-%m-%d %H:%M:%S')

        # Get learning status
        response=$(call_api "learning/status/$log_type")
        learning_enabled=$(echo "$response" | jq -r '.learning_enabled')
        data_size=$(echo "$response" | jq -r '.data_size')
        total_rows=$(echo "$response" | jq -r '.total_rows')
        buffer_size=$(echo "$response" | jq -r '.buffer_size // 0')

        # Move cursor up and clear previous block
        for ((i=0; i<$last_lines; i++)); do
            echo -en "\033[1A\033[2K"
        done

        # Print status block
        echo "----------------------------------------"
        print_message "$GREEN" "Status Update at $timestamp"
        print_message "$GREEN" "Log Type: $log_type"
        print_message "$GREEN" "Learning mode is enabled"
        print_message "$GREEN" "Current session: $data_size rows"
        print_message "$GREEN" "Total rows processed: $total_rows"
        print_message "$GREEN" "Buffered (Flink in-memory): $buffer_size rows"
        progress=$(( (data_size * 100) / TRAINING_THRESHOLD ))
        if [ "$progress" -gt 100 ]; then
            progress=100
        fi
        print_message "$BLUE" "Progress to training:"
        display_progress_bar "$progress"
        print_message "$BLUE" "($data_size/$TRAINING_THRESHOLD rows)"
        echo "----------------------------------------"

        if [ "$learning_enabled" != "true" ]; then
            print_message "$YELLOW" "Learning mode is disabled"
            print_message "$YELLOW" "Current session: $data_size rows"
            print_message "$YELLOW" "Total rows processed: $total_rows"
            kill $pid
            exit 0
        fi

        sleep 5
    done
}

# Function to monitor learning status for all log types
monitor_all_learning() {
    print_info "Starting comprehensive learning monitor for all log types (Press Ctrl+C to stop monitoring)"
    print_info "Training will start automatically at $TRAINING_THRESHOLD rows"
    echo

    local log_types=("conn" "ssl" "dns" "http")
    local last_lines=25  # Adjust based on the number of lines to clear

    while true; do
        # Get current timestamp
        timestamp=$(date '+%Y-%m-%d %H:%M:%S')

        # Move cursor up and clear previous block
        for ((i=0; i<$last_lines; i++)); do
            echo -en "\033[1A\033[2K"
        done

        # Print header
        echo "=================================================================================="
        print_message "$GREEN" "🔄 LSTM Learning Status Monitor - $timestamp"
        echo "=================================================================================="
        echo

        # Monitor each log type
        for log_type in "${log_types[@]}"; do
            response=$(call_api "learning/status/$log_type")
            learning_enabled=$(echo "$response" | jq -r '.learning_enabled')
            data_size=$(echo "$response" | jq -r '.data_size')
            total_rows=$(echo "$response" | jq -r '.total_rows')
        buffer_size=$(echo "$response" | jq -r '.buffer_size // 0')
            
            # Calculate progress
            progress=$(( (data_size * 100) / TRAINING_THRESHOLD ))
            if [ "$progress" -gt 100 ]; then
                progress=100
            fi

            # Print log type header
            echo "📊 $log_type.upper() LOGS:"
            
            if [ "$learning_enabled" = "true" ]; then
                print_message "$GREEN" "   ✅ Learning: ENABLED"
                print_message "$BLUE" "   📈 Data Collected: $data_size rows"
                print_message "$BLUE" "   📊 Total Processed: $total_rows rows"
                print_message "$BLUE" "   🧰 Buffered (Flink): $buffer_size rows"
                print_message "$BLUE" "   🎯 Progress to Training:"
                printf "   "
                display_progress_bar "$progress"
                printf "   (%d/%d rows)\n" "$data_size" "$TRAINING_THRESHOLD"
            else
                print_message "$YELLOW" "   ⚠️ Learning: DISABLED"
                print_message "$BLUE" "   📈 Data Collected: $data_size rows"
                print_message "$BLUE" "   📊 Total Processed: $total_rows rows"
                print_message "$BLUE" "   🧰 Buffered (Flink): $buffer_size rows"
            fi
            
            echo
        done

        # Print footer
        echo "=================================================================================="
        print_message "$BLUE" "⏰ Next update in 10 seconds... (Press Ctrl+C to stop)"
        echo "=================================================================================="

        sleep 10
    done
}

# Function to display usage information
show_usage() {
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  health                    - Check service health"
    echo "  predict <data>            - Make prediction on data (JSON format)"
    echo "  enable-learn [log_type]   - Enable learning mode and start monitoring"
    echo "  enable-learn-all          - Enable learning mode for ALL log types (conn, dns, http, ssl)"
    echo "  disable-learn [log_type]  - Disable learning mode and train model (shows progress)"
    echo "  disable-learn-all         - Disable learning mode for ALL log types"
    echo "  status [log_type]         - Get current learning status"
    echo "  model-status [log_type]   - Get status of all models (shows training progress if active)"
    echo "  metrics [log_type]        - View model performance metrics (accuracy, F1, etc.)"
    echo "  reload-model [log_type]   - Reload trained model without restarting container"
    echo "  reload-all                - Reload all trained models"
    echo "  monitor-all               - Monitor learning status of all log types (10s updates)"
    echo "  topic-counts               - Show record counts and export each topic to a text file"
    echo "  topic-counts unique        - Same as topic-counts but export unique rows only (no duplicate lines)"
    echo ""
    echo "Numeric shortcuts (equivalent to commands above):"
    echo "  1  -> health"
    echo "  2  -> enable-learn [log_type]"
    echo "  3  -> disable-learn [log_type]"
    echo "  4  -> status [log_type]"
    echo "  5  -> model-status [log_type]"
    echo "  6  -> metrics [log_type]"
    echo "  7  -> monitor-all"
    echo "  8  -> enable-learn-all"
    echo "  9  -> disable-learn-all"
    echo "  10 -> reload-model [log_type]"
    echo "  11 -> reload-all"
    echo "  12 -> topic-counts (show counts + export to conn-anomaly.txt, etc.)"
    echo "  13 -> topic-counts unique (export unique rows only, no duplicate lines)"
    echo ""
    echo "Options:"
    echo "  log_type                  - Type of logs to process (http, ssl, dns, conn)"
    echo "                            Default: $DEFAULT_LOG_TYPE"
    echo "  TOPIC_EXPORT_DIR          - For topic-counts (12): directory for exported files."
    echo "                            Default: ./topic-exports (files: conn-anomaly.txt, etc.)"
    echo "  TOPIC_EXPORT_UNIQUE=1     - For topic-counts (12): remove duplicate lines (same event can appear many times)."
    echo ""
    echo "Examples (named commands):"
    echo "  $0 health"
    echo "  $0 predict '{\"log_type\": \"http\", \"data\": [[1,2,3,4,5,6,7,8,9,10]]}'"
    echo "  $0 enable-learn http"
    echo "  $0 enable-learn-all              # Enable learning for all log types"
    echo "  $0 disable-learn ssl"
    echo "  $0 disable-learn-all             # Disable learning for all log types"
    echo "  $0 status dns"
    echo "  $0 reload-model conn             # Reload conn model"
    echo "  $0 reload-all                    # Reload all models"
    echo "  $0 monitor-all"
    echo ""
    echo "Examples (numeric shortcuts):"
    echo "  $0 1"
    echo "  $0 2 http"
    echo "  $0 3 http"
    echo "  $0 4 conn"
    echo "  $0 5 http"
    echo "  $0 6 http                        # View metrics"
    echo "  $0 7"
    echo "  $0 8                             # Enable learning for all"
    echo "  $0 9                             # Disable learning for all"
    echo "  $0 10 conn                       # Reload conn model"
    echo "  $0 11                            # Reload all models"
    echo "  $0 12                            # topic-counts + export topics to ./topic-exports/"
    echo "  $0 13                            # topic-counts + export unique rows only"
    echo ""
    # Do not exit here so main can decide whether to continue (e.g. show menu)
    return 0
}

# Function to validate JSON data
validate_json() {
    local json=$1
    if ! echo "$json" | jq . >/dev/null 2>&1; then
        print_error "Invalid JSON format"
    fi
}

# Main script logic
main() {
    # If no arguments, first show usage/help, then interactive numeric menu
    if [ $# -eq 0 ]; then
        show_usage
        echo ""
        echo -e "${BLUE}==================== LSTM Control Menu ====================${NC}"
        echo -e "${YELLOW}Default log_type:${NC} ${GREEN}${DEFAULT_LOG_TYPE}${NC}"
        echo ""
        echo -e "  ${GREEN}1${NC}) health"
        echo -e "  ${GREEN}2${NC}) enable-learn [log_type]"
        echo -e "  ${GREEN}3${NC}) disable-learn [log_type]"
        echo -e "  ${GREEN}4${NC}) status [log_type]"
        echo -e "  ${GREEN}5${NC}) model-status [log_type]"
        echo -e "  ${GREEN}6${NC}) metrics [log_type] ${YELLOW}(Accuracy, F1, FPR)${NC}"
        echo -e "  ${GREEN}7${NC}) monitor-all"
        echo -e "  ${GREEN}8${NC}) enable-learn-all ${YELLOW}(ALL log types)${NC}"
        echo -e "  ${GREEN}9${NC}) disable-learn-all ${YELLOW}(ALL log types)${NC}"
        echo -e "  ${GREEN}10${NC}) reload-model [log_type]"
        echo -e "  ${GREEN}11${NC}) reload-all ${YELLOW}(ALL models)${NC}"
        echo -e "  ${GREEN}12${NC}) topic-counts ${YELLOW}(counts + export to conn-anomaly.txt, etc.)${NC}"
        echo -e "  ${GREEN}13${NC}) topic-counts unique ${YELLOW}(export unique rows only)${NC}"
        echo -e "  ${GREEN}14${NC}) exit"
        echo -e "${BLUE}===========================================================${NC}"
        echo ""
        # Allow input like: "2 conn" or just "2"
        local choice menu_log_type
        read -rp "Select an option [1-14] (you can also add log_type, e.g. '2 http'): " choice menu_log_type

        case "$choice" in
            1) set -- health ;;
            2) set -- enable-learn "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            3) set -- disable-learn "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            4) set -- status "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            5) set -- model-status "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            6) set -- metrics "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            7) set -- monitor-all ;;
            8) set -- enable-learn-all ;;
            9) set -- disable-learn-all ;;
            10) set -- reload-model "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            11) set -- reload-all ;;
            12) set -- topic-counts ;;
            13) set -- topic-counts unique ;;
            14) echo "Exiting."; exit 0 ;;
            *) echo -e "${RED}Invalid choice.${NC}"; exit 1 ;;
        esac
    fi

    # topic-counts (and topic-counts unique) only need Kafka; others need LSTM container
    if [ "$1" != "topic-counts" ]; then
        check_container
    fi

    # Allow numeric shortcuts even when arguments are provided
    case "$1" in
        1) shift; set -- health "$@" ;;
        2) shift; set -- enable-learn "$@" ;;
        3) shift; set -- disable-learn "$@" ;;
        4) shift; set -- status "$@" ;;
        5) shift; set -- model-status "$@" ;;
        6) shift; set -- metrics "$@" ;;
        7) shift; set -- monitor-all "$@" ;;
        8) shift; set -- enable-learn-all "$@" ;;
        9) shift; set -- disable-learn-all "$@" ;;
        10) shift; set -- reload-model "$@" ;;
        11) shift; set -- reload-all "$@" ;;
        12) shift; set -- topic-counts "$@" ;;
        13) shift; set -- topic-counts unique "$@" ;;
    esac

    # Process commands (after possible numeric mapping)
    case "$1" in
        help|-h|--help)
            show_usage
            exit 0
            ;;

        health)
            print_info "Checking LSTM autoencoder service health..."
            check_health
            print_success "Service is healthy"
            ;;
            
        predict)
            if [ $# -lt 2 ]; then
                print_error "Usage: $0 predict <data>"
            fi
            validate_json "$2"
            print_info "Making prediction..."
            response=$(call_api "predict" "POST" "$2")
            if echo "$response" | jq -e '.status == "success"' >/dev/null; then
                print_success "Prediction Results:"
                echo "$response" | jq -r '. | select(.status == "success") | "Log Type: \(.log_type)\nPrediction: \(.prediction)\nAnomaly Scores:\n  MSE: \(.anomaly_scores.mse)\n  MAE: \(.anomaly_scores.mae)\nThreshold: \(.threshold)\nModel Version: \(.details.model_info.path)"'
                if echo "$response" | jq -e '.details.raw_scores' >/dev/null; then
                    print_info "Raw Scores:"
                    echo "$response" | jq -r '.details.raw_scores | to_entries[] | "\(.key): \(.value[])"'
                fi
            else
                error_msg=$(echo "$response" | jq -r '.error // "Unknown error"')
                print_error "Prediction failed: $error_msg"
            fi
            ;;
            
        enable-learn)
            log_type=${2:-$DEFAULT_LOG_TYPE}
            print_info "Enabling learning mode for $log_type..."
            response=$(call_api "learning/enable/$log_type" "POST")
            if echo "$response" | jq -e '.learning_enabled == true' >/dev/null; then
                print_success "Learning mode enabled successfully for $log_type"
                # Run monitoring in the foreground
                monitor_learning $$ "$log_type"
            else
                print_error "Failed to enable learning mode: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        enable-learn-all)
            print_info "Enabling learning mode for ALL log types (conn, dns, http, ssl)..."
            echo ""
            
            local all_success=true
            for log_type in conn dns http ssl; do
                print_info "Enabling learning for $log_type..."
                response=$(call_api "learning/enable/$log_type" "POST")
                if echo "$response" | jq -e '.learning_enabled == true' >/dev/null; then
                    print_success "  ✅ $log_type learning enabled"
                else
                    print_error "  ❌ Failed to enable $log_type: $(echo "$response" | jq -r '.message // "Unknown error"')"
                    all_success=false
                fi
            done
            
            echo ""
            if $all_success; then
                print_success "All log types are now in learning mode!"
                echo ""
                print_info "Starting comprehensive monitoring for all log types..."
                monitor_all_learning
            else
                print_warning "Some log types failed to enable. Check the errors above."
            fi
            ;;
            
        disable-learn)
            log_type=${2:-$DEFAULT_LOG_TYPE}
            print_info "Disabling learning mode for $log_type..."
            response=$(call_api "learning/disable/$log_type" "POST")
            if echo "$response" | jq -e '.learning_enabled == false' >/dev/null; then
                print_success "Learning mode disabled successfully for $log_type"
                
                # Check if training started
                training_started=$(echo "$response" | jq -r '.training_started // false')
                
                if [ "$training_started" = "true" ]; then
                    print_info "Training started in background. Monitoring progress..."
                    echo ""
                    
                    # Monitor training progress
                    monitor_training_progress "$log_type"
                    
                    echo ""
                    print_success "Training completed for $log_type!"
                    echo ""
                    
                    # Show final training results
                    print_info "Fetching final training results..."
                    status_response=$(call_api "training/status/$log_type")
                    if echo "$status_response" | jq -e '.training_status.result' >/dev/null; then
                    print_info "Training Results:"
                        echo "$status_response" | jq -r '.training_status.result | to_entries[] | "  \(.key): \(.value)"'
                    fi
                else
                    required=$(echo "$response" | jq -r '.required_rows // 10')
                    have=$(echo "$response" | jq -r '.final_data_size // "?"')
                    print_warning "Not enough data to start training (have ${have} rows, need at least ${required} for $log_type). Enable learning and let data flow, or run ./refill-lstm-from-kafka.sh to backfill."
                fi
            else
                print_error "Failed to disable learning mode: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        disable-learn-all)
            print_info "Disabling learning mode for ALL log types (conn, dns, http, ssl)..."
            echo ""
            
            local all_success=true
            local training_types=()
            
            # Disable learning for all types
            for log_type in conn dns http ssl; do
                print_info "Disabling learning for $log_type..."
                response=$(call_api "learning/disable/$log_type" "POST")
                if echo "$response" | jq -e '.learning_enabled == false' >/dev/null; then
                    print_success "  ✅ $log_type learning disabled"
                    
                    training_started=$(echo "$response" | jq -r '.training_started // false')
                    if [ "$training_started" = "true" ]; then
                        training_types+=("$log_type")
                        print_info "  🔄 Training started for $log_type"
                    else
                        required=$(echo "$response" | jq -r '.required_rows // 10')
                        have=$(echo "$response" | jq -r '.final_data_size // "?"')
                        print_warning "  ⚠️ Not enough data to train $log_type (have ${have}, need ≥${required} rows)"
                    fi
                else
                    print_error "  ❌ Failed to disable $log_type: $(echo "$response" | jq -r '.message // "Unknown error"')"
                    all_success=false
                fi
                echo ""
            done
            
            # Monitor all training jobs
            if [ ${#training_types[@]} -gt 0 ]; then
                print_info "Monitoring training for ${#training_types[@]} log type(s)..."
                echo ""
                
                for log_type in "${training_types[@]}"; do
                    print_info "Training $log_type..."
                    monitor_training_progress "$log_type"
                    echo ""
                done
                
                print_success "All training jobs completed!"
            fi
            
            if $all_success; then
                print_success "All log types have been disabled and trained!"
            else
                print_warning "Some log types failed to disable. Check the errors above."
            fi
            ;;
            
        status)
            log_type=${2:-$DEFAULT_LOG_TYPE}
            print_info "Getting learning status for $log_type..."
            response=$(call_api "learning/status/$log_type")
            if echo "$response" | jq -e '.learning_enabled != null' >/dev/null; then
                status=$(echo "$response" | jq -r '.learning_enabled')
                if [ "$status" = "true" ]; then
                    print_success "Learning mode is enabled for $log_type"
                else
                    print_info "Learning mode is disabled for $log_type"
                fi
                # Show additional status information
                data_size=$(echo "$response" | jq -r '.data_size')
                total_rows=$(echo "$response" | jq -r '.total_rows')
            buffer_size=$(echo "$response" | jq -r '.buffer_size // 0')
                echo "Data collected: $data_size rows"
                echo "Total rows processed: $total_rows"
            echo "Buffered (Flink in-memory): $buffer_size rows"
            else
                print_error "Failed to get learning status: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        model-status)
            log_type=$2  # Optional log type
            
            # If specific log type provided, check training progress first
            if [ -n "$log_type" ]; then
                print_info "Checking training status for $log_type..."
                progress_response=$(call_api "training/progress/$log_type")
                training_active=$(echo "$progress_response" | jq -r '.training_active // false')
                
                if [ "$training_active" = "true" ]; then
                    # Show training progress
                    echo ""
                    echo "======================================================================"
                    print_message "$YELLOW" "🔄 TRAINING IN PROGRESS - ${log_type^^}"
                    echo "======================================================================"
                    
                    current_epoch=$(echo "$progress_response" | jq -r '.progress.current_epoch // 0')
                    total_epochs=$(echo "$progress_response" | jq -r '.progress.total_epochs // 100')
                    progress_pct=$(echo "$progress_response" | jq -r '.progress.progress_pct // 0')
                    loss=$(echo "$progress_response" | jq -r '.progress.loss // 0')
                    val_loss=$(echo "$progress_response" | jq -r '.progress.val_loss // 0')
                    elapsed=$(echo "$progress_response" | jq -r '.progress.elapsed_seconds // 0')
                    eta=$(echo "$progress_response" | jq -r '.progress.eta_seconds // 0')
                    
                    print_message "$BLUE" "Epoch: $current_epoch / $total_epochs"
                    print_message "$BLUE" "Progress:"
                    printf "  "
                    display_progress_bar "$progress_pct"
                    print_message "$BLUE" "Loss: $(printf "%.6f" "$loss") | Val Loss: $(printf "%.6f" "$val_loss")"
                    print_message "$BLUE" "Elapsed: $(format_time "$elapsed")"
                    if [ "$eta" -gt 0 ]; then
                        print_message "$BLUE" "ETA: $(format_time "$eta")"
                    fi
                    echo "======================================================================"
                    echo ""
                    print_info "Use './lstm-control.sh 5 $log_type' to refresh"
                    echo ""
                fi
            fi
            
            # Show model status
            print_info "Getting model status..."
            url="models/status"
            if [ -n "$log_type" ]; then
                url="${url}?log_type=$log_type"
            fi
            response=$(call_api "$url")
            if echo "$response" | jq -e '.status == "success"' >/dev/null; then
                if [ "$training_active" != "true" ]; then
                    echo ""
                    echo "======================================================================"
                fi
                print_success "MODEL STATUS - ${log_type^^:-ALL TYPES}"
                echo "======================================================================"
                echo ""
                if [ -n "$log_type" ]; then
                    # Show detailed info for specific log type
                    models=$(echo "$response" | jq -r '.log_types.'"$log_type"'.models')
                    count=$(echo "$response" | jq -r '.log_types.'"$log_type"'.count')
                    
                    if [ "$count" -gt 0 ]; then
                        print_message "$BLUE" "Total Models: $count"
                        echo ""
                        echo "$response" | jq -r '.log_types.'"$log_type"'.models[] | "Model: \(.filename)\nCreated: \(.created_at)\nSize: \(.size_bytes) bytes\n" + 
                        if .training_info then 
                            "Training Samples: \(.training_info.training_samples // "N/A")\n" +
                            "Validation Loss: \(.training_info.validation_loss // "N/A")\n" +
                            "Training Loss: \(.training_info.training_loss // "N/A")\n" +
                            if .training_info.accuracy then
                                "Accuracy: \(.training_info.accuracy)\n" +
                                "F1 Score: \(.training_info.f1_score // "N/A")\n" +
                                "FPR: \(.training_info.false_positive_rate // "N/A")\n"
                            else "" end
                        else "Training Info: Not available\n" end'
                else
                        print_warning "No models found for $log_type"
                    fi
                else
                    # Show summary for all log types
                    echo "$response" | jq -r '.log_types | to_entries[] | 
                    "📊 " + (.key | ascii_upcase) + " LOGS:\n" +
                    "   Models: \(.value.count)\n" + 
                    if .value.count > 0 then
                        "   Latest: \(.value.latest_model.filename)\n" +
                        "   Created: \(.value.latest_model.created_at)\n"
                    else "   No models trained yet\n" end + "\n"'
                fi
                echo "======================================================================"
            else
                print_error "Failed to get model status: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        metrics)
            log_type=${2:-$DEFAULT_LOG_TYPE}
            print_info "Getting performance metrics for $log_type..."
            response=$(call_api "metrics/$log_type")
            if echo "$response" | jq -e '.status == "success" and .has_metrics == true' >/dev/null; then
                echo ""
                echo "======================================================================"
                print_success "MODEL METRICS - ${log_type^^}"
                echo "======================================================================"
                echo ""
                
                # Model info
                model_path=$(echo "$response" | jq -r '.metrics.model_path')
                created_at=$(echo "$response" | jq -r '.metrics.created_at')
                training_samples=$(echo "$response" | jq -r '.metrics.training_samples')
                echo "Model: $(basename "$model_path")"
                echo "Created: $created_at"
                echo "Training Samples: $training_samples"
                
                # Loss metrics
                echo ""
                echo "----------------------------------------------------------------------"
                print_message "$BLUE" "LOSS METRICS"
                echo "----------------------------------------------------------------------"
                training_loss=$(echo "$response" | jq -r '.metrics.training_loss')
                validation_loss=$(echo "$response" | jq -r '.metrics.validation_loss')
                threshold=$(echo "$response" | jq -r '.metrics.anomaly_threshold')
                printf "Training Loss:   %.6f\n" "$training_loss"
                printf "Validation Loss: %.6f\n" "$validation_loss"
                printf "Threshold:       %.6f\n" "$threshold"
                
                # Classification metrics
                echo ""
                echo "----------------------------------------------------------------------"
                print_message "$BLUE" "CLASSIFICATION METRICS (on training set)"
                echo "----------------------------------------------------------------------"
                accuracy=$(echo "$response" | jq -r '.metrics.accuracy')
                precision=$(echo "$response" | jq -r '.metrics.precision')
                recall=$(echo "$response" | jq -r '.metrics.recall')
                f1_score=$(echo "$response" | jq -r '.metrics.f1_score')
                fpr=$(echo "$response" | jq -r '.metrics.false_positive_rate')
                
                if [ "$accuracy" != "null" ]; then
                    acc_pct=$(echo "$accuracy * 100" | bc -l)
                    printf "Accuracy:             %.4f (%.2f%%)\n" "$accuracy" "$acc_pct"
                    printf "Precision:            %.4f\n" "$precision"
                    printf "Recall:               %.4f\n" "$recall"
                    printf "F1 Score:             %.4f\n" "$f1_score"
                    fpr_pct=$(echo "$fpr * 100" | bc -l)
                    printf "False Positive Rate:  %.4f (%.2f%%)\n" "$fpr" "$fpr_pct"
                    
                    echo ""
                    echo "======================================================================"
                    
                    # Interpretation
                    fpr_threshold=$(echo "$fpr < 0.05" | bc -l)
                    if [ "$fpr_threshold" -eq 1 ]; then
                        print_success "✓ EXCELLENT: Very low false positive rate!"
                    else
                        fpr_threshold=$(echo "$fpr < 0.10" | bc -l)
                        if [ "$fpr_threshold" -eq 1 ]; then
                            print_success "✓ GOOD: Acceptable false positive rate"
                        else
                            fpr_threshold=$(echo "$fpr < 0.20" | bc -l)
                            if [ "$fpr_threshold" -eq 1 ]; then
                                print_warning "⚠ WARNING: High false positive rate - consider retraining"
                            else
                                print_error "✗ POOR: Very high false positive rate - retrain with more data"
                            fi
                        fi
                    fi
                else
                    print_warning "No classification metrics available (model may be from older version)"
                fi
                
                echo ""
            elif echo "$response" | jq -e '.has_metrics == false' >/dev/null; then
                print_warning "No metrics found for $log_type"
                echo "Train the model first:"
                echo "  ./lstm-control.sh enable-learn $log_type"
                echo "  (wait for data collection)"
                echo "  ./lstm-control.sh disable-learn $log_type"
            else
                print_error "Failed to get metrics: $(echo "$response" | jq -r '.error // .message // "Unknown error"')"
            fi
            ;;
            
        reload-model)
            log_type=${2:-$DEFAULT_LOG_TYPE}
            print_info "Reloading model for $log_type (no container restart needed)..."
            response=$(call_api "models/reload/$log_type" "POST")
            if echo "$response" | jq -e '.status == "success"' >/dev/null; then
                model_loaded=$(echo "$response" | jq -r '.model_loaded')
                if [ "$model_loaded" = "true" ]; then
                    print_success "Model reloaded successfully for $log_type"
                    model_path=$(echo "$response" | jq -r '.model_path')
                    echo "Model path: $model_path"
                else
                    print_warning "No trained model found for $log_type"
                fi
            else
                print_error "Failed to reload model: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        reload-all)
            print_info "Reloading all models (no container restart needed)..."
            response=$(call_api "models/reload" "POST")
            if echo "$response" | jq -e '.status == "success"' >/dev/null; then
                all_loaded=$(echo "$response" | jq -r '.all_models_loaded')
                echo ""
                print_success "Model Reload Results:"
                echo "$response" | jq -r '.results | to_entries[] | "  \(.key): \(if .value.model_loaded then "✅ Loaded" else "❌ Not found" end) - \(.value.model_path // "N/A")"'
                echo ""
                if [ "$all_loaded" = "true" ]; then
                    print_success "All models reloaded successfully!"
                else
                    print_warning "Some models could not be loaded (may not be trained yet)"
                fi
            else
                print_error "Failed to reload models: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        monitor-all)
            print_info "Starting comprehensive monitoring for all log types..."
            monitor_all_learning
            ;;

        topic-counts)
            check_kafka
            # Optional second arg "unique" => export unique rows only (option 13)
            if [ "${2:-}" = "unique" ]; then
                export TOPIC_EXPORT_UNIQUE=1
            else
                export TOPIC_EXPORT_UNIQUE=0
            fi
            print_info "Kafka topic record counts (latest offset sum per topic)"
            echo ""
            echo "======================================================================"
            print_message "$BLUE" "MALICIOUS TOPICS (LSTM unsupervised output)"
            echo "----------------------------------------------------------------------"
            for topic in malicious-conn malicious-http malicious-dns malicious-ssl; do
                count=$(get_topic_count "$topic")
                printf "  %-22s %d records\n" "$topic" "$count"
            done
            echo "----------------------------------------------------------------------"
            print_message "$BLUE" "SUPERVISED TOPIC (UNSW 7-class model output)"
            echo "----------------------------------------------------------------------"
            count=$(get_topic_count "supervised-unsw42")
            printf "  %-22s %d records\n" "supervised-unsw42" "$count"
            echo "======================================================================"
            echo ""
            # Export each topic to a separate text file (unique rows if topic-counts unique)
            export_topics_to_files "${TOPIC_EXPORT_DIR:-./topic-exports}"
            ;;

        *)
            show_usage
            ;;
    esac
}

# Run the main function with all arguments
main "$@" 