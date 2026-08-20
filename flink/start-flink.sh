#!/bin/bash
set -e

# Create and set permissions for checkpoint/savepoint directories
echo "Setting up checkpoint and savepoint directories..."
mkdir -p /opt/flink/savepoints /opt/flink/checkpoints
chmod -R 777 /opt/flink/savepoints /opt/flink/checkpoints

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
