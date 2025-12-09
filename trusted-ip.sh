#!/bin/bash

# Script to manage trusted IPs for the supervised ML API
# Usage:
#   ./trusted-ip.sh add 192.168.1.1     - Add an IP to trusted list
#   ./trusted-ip.sh remove 192.168.1.1  - Remove an IP from trusted list
#   ./trusted-ip.sh list                - List all trusted IPs

# Environment variables
SUPERVISED_CONTAINER=${SUPERVISED_CONTAINER:-"supervised"}
SUPERVISED_PORT=${SUPERVISED_PORT:-"8000"}

# Function to validate IP address
validate_ip() {
    local ip=$1
    if [[ ! $ip =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
        return 1
    fi
    IFS='.' read -r -a ip_parts <<< "$ip"
    for part in "${ip_parts[@]}"; do
        if [ "$part" -gt 255 ] || [ "$part" -lt 0 ]; then
            return 1
        fi
    done
    return 0
}

# Function to make API calls with error handling
call_api() {
    local endpoint=$1
    local method=${2:-GET}
    local data=$3
    
    if [ -n "$data" ]; then
        response=$(docker exec "$SUPERVISED_CONTAINER" curl -s -X "$method" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "http://localhost:$SUPERVISED_PORT/$endpoint")
    else
        response=$(docker exec "$SUPERVISED_CONTAINER" curl -s -X "$method" \
            "http://localhost:$SUPERVISED_PORT/$endpoint")
    fi
    
    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to communicate with $SUPERVISED_CONTAINER service"
        exit 1
    fi
    
    echo "$response"
}

# Check if supervised container is running
if ! docker ps | grep -q "$SUPERVISED_CONTAINER"; then
    echo "❌ Error: $SUPERVISED_CONTAINER container is not running"
    echo "Please start the system first with ./start-system.sh"
    exit 1
fi

# Command
CMD=$1
IP=$2

case $CMD in
    add)
        if [ -z "$IP" ]; then
            echo "Error: IP address is required"
            echo "Usage: $0 add <ip-address>"
            exit 1
        fi
        
        if ! validate_ip "$IP"; then
            echo "Error: Invalid IP address format"
            exit 1
        fi
        
        echo "Adding $IP to trusted IPs list..."
        call_api "trusted-ips" "POST" "{\"ip\":\"$IP\"}" | jq .
        ;;
        
    remove)
        if [ -z "$IP" ]; then
            echo "Error: IP address is required"
            echo "Usage: $0 remove <ip-address>"
            exit 1
        fi
        
        if ! validate_ip "$IP"; then
            echo "Error: Invalid IP address format"
            exit 1
        fi
        
        echo "Removing $IP from trusted IPs list..."
        call_api "trusted-ips/$IP" "DELETE" | jq .
        ;;
        
    list)
        echo "Listing all trusted IPs..."
        call_api "trusted-ips" | jq .
        ;;
        
    *)
        echo "Error: Unknown command '$CMD'"
        echo "Usage: $0 <add|remove|list> [ip-address]"
        exit 1
        ;;
esac 