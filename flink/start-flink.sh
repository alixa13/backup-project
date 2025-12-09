#!/bin/bash
set -e

# Create and set permissions for savepoint directories (checkpointing disabled)
echo "Setting up savepoint directories..."
mkdir -p /opt/flink/savepoints
chmod -R 777 /opt/flink/savepoints

echo "Starting Flink cluster..."

# Start the Flink cluster
/opt/flink/bin/start-cluster.sh

echo "Flink cluster started"

# Wait for the cluster to be fully started
sleep 10

echo "Starting Flink jobs using jobstarter.sh..."
/opt/flink/jobstarter.sh

# Keep the container running
tail -f /dev/null
