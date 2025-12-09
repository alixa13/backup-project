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

# Environment variables with defaults
LSTM_CONTAINER=${LSTM_CONTAINER:-"lstm-autoencoder"}
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
    echo "  disable-learn [log_type]  - Disable learning mode and train model"
    echo "  disable-learn-all         - Disable learning mode for ALL log types"
    echo "  status [log_type]         - Get current learning status"
    echo "  model-status [log_type]   - Get status of all models"
    echo "  monitor-all               - Monitor learning status of all log types (10s updates)"
    echo ""
    echo "Numeric shortcuts (equivalent to commands above):"
    echo "  1  -> health"
    echo "  2  -> enable-learn [log_type]"
    echo "  3  -> disable-learn [log_type]"
    echo "  4  -> status [log_type]"
    echo "  5  -> model-status [log_type]"
    echo "  6  -> monitor-all"
    echo "  7  -> enable-learn-all"
    echo "  8  -> disable-learn-all"
    echo ""
    echo "Options:"
    echo "  log_type                  - Type of logs to process (http, ssl, dns, conn)"
    echo "                            Default: $DEFAULT_LOG_TYPE"
    echo ""
    echo "Examples (named commands):"
    echo "  $0 health"
    echo "  $0 predict '{\"log_type\": \"http\", \"data\": [[1,2,3,4,5,6,7,8,9,10]]}'"
    echo "  $0 enable-learn http"
    echo "  $0 enable-learn-all              # Enable learning for all log types"
    echo "  $0 disable-learn ssl"
    echo "  $0 disable-learn-all             # Disable learning for all log types"
    echo "  $0 status dns"
    echo "  $0 monitor-all"
    echo ""
    echo "Examples (numeric shortcuts):"
    echo "  $0 1"
    echo "  $0 2 http"
    echo "  $0 3 http"
    echo "  $0 4 conn"
    echo "  $0 5 http"
    echo "  $0 6"
    echo "  $0 7                             # Enable learning for all"
    echo "  $0 8                             # Disable learning for all"
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
        echo -e "  ${GREEN}6${NC}) monitor-all"
        echo -e "  ${GREEN}7${NC}) enable-learn-all ${YELLOW}(ALL log types)${NC}"
        echo -e "  ${GREEN}8${NC}) disable-learn-all ${YELLOW}(ALL log types)${NC}"
        echo -e "  ${GREEN}9${NC}) exit"
        echo -e "${BLUE}===========================================================${NC}"
        echo ""
        # Allow input like: "2 conn" or just "2"
        local choice menu_log_type
        read -rp "Select an option [1-9] (you can also add log_type, e.g. '2 http'): " choice menu_log_type

        case "$choice" in
            1) set -- health ;;
            2) set -- enable-learn "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            3) set -- disable-learn "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            4) set -- status "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            5) set -- model-status "${menu_log_type:-$DEFAULT_LOG_TYPE}" ;;
            6) set -- monitor-all ;;
            7) set -- enable-learn-all ;;
            8) set -- disable-learn-all ;;
            9) echo "Exiting."; exit 0 ;;
            *) echo -e "${RED}Invalid choice.${NC}"; exit 1 ;;
        esac
    fi

    # Check if container is running before proceeding
    check_container

    # Allow numeric shortcuts even when arguments are provided
    case "$1" in
        1) shift; set -- health "$@" ;;
        2) shift; set -- enable-learn "$@" ;;
        3) shift; set -- disable-learn "$@" ;;
        4) shift; set -- status "$@" ;;
        5) shift; set -- model-status "$@" ;;
        6) shift; set -- monitor-all "$@" ;;
        7) shift; set -- enable-learn-all "$@" ;;
        8) shift; set -- disable-learn-all "$@" ;;
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
                # Show training results if available
                if echo "$response" | jq -e '.training_result' >/dev/null; then
                    print_info "Training Results:"
                    echo "$response" | jq -r '.training_result | to_entries[] | "\(.key): \(.value)"'
                fi
            else
                print_error "Failed to disable learning mode: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        disable-learn-all)
            print_info "Disabling learning mode for ALL log types (conn, dns, http, ssl)..."
            echo ""
            
            local all_success=true
            for log_type in conn dns http ssl; do
                print_info "Disabling learning for $log_type..."
                response=$(call_api "learning/disable/$log_type" "POST")
                if echo "$response" | jq -e '.learning_enabled == false' >/dev/null; then
                    print_success "  ✅ $log_type learning disabled"
                    
                    # Show training results if available
                    if echo "$response" | jq -e '.training_result' >/dev/null; then
                        echo "     Training Results:"
                        echo "$response" | jq -r '.training_result | to_entries[] | "       \(.key): \(.value)"'
                    fi
                else
                    print_error "  ❌ Failed to disable $log_type: $(echo "$response" | jq -r '.message // "Unknown error"')"
                    all_success=false
                fi
                echo ""
            done
            
            if $all_success; then
                print_success "All log types have been disabled and trained (if enough data)!"
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
            print_info "Getting model status..."
            url="models/status"
            if [ -n "$log_type" ]; then
                url="${url}?log_type=$log_type"
            fi
            response=$(call_api "$url")
            if echo "$response" | jq -e '.models' >/dev/null; then
                print_success "Model Status:"
                if [ -n "$log_type" ]; then
                    echo "$response" | jq -r '.log_types.'"$log_type"'.models[] | "Model: \(.filename)\nSize: \(.size_bytes) bytes\nCreated: \(.created_at)\nTraining Info: \(.training_info)\n"'
                else
                    echo "$response" | jq -r '.log_types | to_entries[] | "Log Type: \(.key)\nModels: \(.value.count)\nLatest Model: \(.value.latest_model.filename)\n"'
                fi
            else
                print_error "Failed to get model status: $(echo "$response" | jq -r '.message // "Unknown error"')"
            fi
            ;;
            
        monitor-all)
            print_info "Starting comprehensive monitoring for all log types..."
            monitor_all_learning
            ;;
            
        *)
            show_usage
            ;;
    esac
}

# Run the main function with all arguments
main "$@" 