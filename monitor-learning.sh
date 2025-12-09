#!/bin/bash

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Threshold for automatic training
TRAINING_THRESHOLD=20000

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to get current timestamp
get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

# Function to get learning status and data count
get_learning_status() {
    local status_output
    status_output=$(sudo ./lstm-control.sh status)
    local learning_mode=$(echo "$status_output" | grep "Learning mode" | sed 's/^[[:space:]]*//')
    local data_count=$(echo "$status_output" | grep "Data collected" | sed 's/^[[:space:]]*//' | grep -o '[0-9]*')
    echo "$learning_mode|$data_count"
}

# Function to disable learning and start training
disable_and_train() {
    print_message "$YELLOW" "=== Reached $TRAINING_THRESHOLD rows - Starting training ==="
    print_message "$YELLOW" "Disabling learning mode and starting training..."
    sudo ./lstm-control.sh disable-learn
    print_message "$GREEN" "Training initiated. Check model status for results."
    print_message "$BLUE" "================================"
}

# Clear screen and show header
clear
print_message "$BLUE" "=== LSTM Learning Mode Monitor ==="
print_message "$YELLOW" "Press Ctrl+C to stop monitoring"
print_message "$BLUE" "Training will start automatically at $TRAINING_THRESHOLD rows"
echo

# Main monitoring loop
while true; do
    # Get current timestamp
    timestamp=$(get_timestamp)
    
    # Clear previous status lines (4 lines)
    echo -e "\033[4A\033[K"
    
    # Print current status
    print_message "$GREEN" "=== Status Update at $timestamp ==="
    
    # Get and parse status
    status_info=$(get_learning_status)
    learning_mode=$(echo "$status_info" | cut -d'|' -f1)
    data_count=$(echo "$status_info" | cut -d'|' -f2)
    
    # Format the status output
    if echo "$learning_mode" | grep -q "enabled"; then
        print_message "$GREEN" "$learning_mode"
        print_message "$GREEN" "Data collected: $data_count rows"
        
        # Check if we've reached the threshold
        if [ "$data_count" -ge "$TRAINING_THRESHOLD" ]; then
            disable_and_train
            # Wait a bit longer after training starts
            sleep 10
            continue
        fi
    else
        print_message "$YELLOW" "$learning_mode"
        print_message "$YELLOW" "Data collected: $data_count rows"
    fi
    
    # Print progress towards threshold if learning is enabled
    if echo "$learning_mode" | grep -q "enabled"; then
        progress=$(( (data_count * 100) / TRAINING_THRESHOLD ))
        if [ "$progress" -gt 100 ]; then
            progress=100
        fi
        print_message "$BLUE" "Progress to training: $progress% ($data_count/$TRAINING_THRESHOLD)"
    fi
    
    # Print separator
    print_message "$BLUE" "================================"
    
    # Wait for 5 seconds
    sleep 5
done 