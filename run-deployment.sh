#!/bin/bash

# Deployment Script for Flink Kafka Project with LSTM Autoencoder
# This script runs the complete deployment pipeline

set -e  # Exit on any error

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

print_success() {
    print_message "$GREEN" "✅ $1"
}

print_info() {
    print_message "$BLUE" "ℹ️ $1"
}

print_warning() {
    print_message "$YELLOW" "⚠️ $1"
}

print_error() {
    print_message "$RED" "❌ $1"
}

# Function to wait for a specified number of seconds
wait_seconds() {
    local seconds=$1
    print_info "Waiting for $seconds seconds..."
    sleep $seconds
}

# Function to run enable-learn with timeout and then switch to monitor-all
run_lstm_learning() {
    print_info "Starting LSTM learning process..."
    
    # Start enable-learn for all log types in background
    print_info "Running: sudo ./lstm-control.sh enable-learn conn"
    sudo ./lstm-control.sh enable-learn conn &
    local lstm_conn_pid=$!
    
    print_info "Running: sudo ./lstm-control.sh enable-learn dns"
    sudo ./lstm-control.sh enable-learn dns &
    local lstm_dns_pid=$!
    
    print_info "Running: sudo ./lstm-control.sh enable-learn http"
    sudo ./lstm-control.sh enable-learn http &
    local lstm_http_pid=$!
    
    print_info "Running: sudo ./lstm-control.sh enable-learn ssl"
    sudo ./lstm-control.sh enable-learn ssl &
    local lstm_ssl_pid=$!
    
    # Wait for 120 seconds
    print_info "Waiting 120 seconds for learning to progress..."
    wait_seconds 120
    
    # Kill all the enable-learn processes
    print_info "Stopping enable-learn processes (Ctrl+C equivalent)..."
    kill $lstm_conn_pid 2>/dev/null || true
    kill $lstm_dns_pid 2>/dev/null || true
    kill $lstm_http_pid 2>/dev/null || true
    kill $lstm_ssl_pid 2>/dev/null || true
    
    # Wait a moment for the processes to stop
    sleep 2
    
    # Start monitor-all
    print_info "Starting monitor-all..."
    sudo ./lstm-control.sh monitor-all
}

# Main deployment process
main() {
    print_info "Starting deployment process..."
    echo "=========================================="
    
    # Step 1: Stop the system
    print_info "Step 1: Stopping the system..."
    sudo ./stop-system.sh
    print_success "System stopped successfully"
    echo
    
    # Step 2: Clean Flink logs
    print_info "Step 2: Cleaning Flink logs..."
    if [ -d "flink/logs" ]; then
        rm -rf flink/logs/*
        print_success "Flink logs cleaned"
    else
        print_info "No Flink logs directory found, skipping cleanup"
    fi
    # Also clean Docker container logs if needed
    if docker ps -a | grep -q flink; then
        print_info "Clearing Docker container logs for Flink..."
        docker logs flink --tail 0 2>/dev/null || true
    fi
    echo
    
    # Step 3: Build the Flink Kafka project
    print_info "Step 3: Building Flink Kafka project..."
    cd FlinkKafkaProject/
    print_info "Running: mvn clean package"
    rm -rf FlinkKafkaProject/target/*
    mvn clean package
    cd ..
    print_success "Flink Kafka project built successfully"
    echo
    
    # Step 4: Copy JAR files to flink/jars/
    print_info "Step 4: Copying JAR files to flink/jars/..."
    rm -rf flink/jars/*
    cp FlinkKafkaProject/target/flink-1.0-SNAPSHOT* flink/jars/
    print_success "JAR files copied successfully"
    echo
    
    # Step 5: Start the system
    print_info "Step 5: Starting the system..."
    sudo ./start-system.sh
    print_success "System started successfully"
    echo
    
    # Step 6: Wait for system to stabilize
    print_info "Step 6: Waiting 120 seconds for system to stabilize..."
    wait_seconds 120
    echo
    
    # Step 7: Run LSTM learning process
    print_info "Step 7: Running LSTM learning process..."
    run_lstm_learning
    
    print_success "Deployment process completed!"
}

# Run the main function
main "$@" 