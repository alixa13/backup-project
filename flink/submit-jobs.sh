#!/bin/bash
set -e

echo "Waiting for Flink to be ready..."
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if curl -s http://localhost:8081/jobs > /dev/null; then
        echo "Flink is ready. Submitting jobs..."
        
        # Submit supervised job (session assembly -> UNSW 42 from all logs). 16 slots (8 + 8 unused).
        echo "Submitting unified supervised job..."
        /opt/flink/bin/flink run -p 16 -d /opt/flink/jars/flink-1.0-SNAPSHOT-unified-supervised.jar
        
        # Submit unsupervised jobs with higher parallelism
        echo "Submitting unsupervised jobs..."
        echo "Submitting unsupervised conn job..."
        /opt/flink/bin/flink run -p 16 -d /opt/flink/jars/flink-1.0-SNAPSHOT-unsupervised-conn.jar
        
        echo "Submitting unsupervised dns job..."
        /opt/flink/bin/flink run -p 16 -d /opt/flink/jars/flink-1.0-SNAPSHOT-unsupervised-dns.jar
        
        echo "Submitting unsupervised http job..."
        /opt/flink/bin/flink run -p 16 -d /opt/flink/jars/flink-1.0-SNAPSHOT-unsupervised-http.jar
        
        echo "Submitting unsupervised ssl job..."
        /opt/flink/bin/flink run -p 16 -d /opt/flink/jars/flink-1.0-SNAPSHOT-unsupervised-ssl.jar
        
        echo "All jobs submitted successfully"
        exit 0
    fi
    
    echo "Attempt $attempt: Flink not ready yet, waiting..."
    sleep 5
    attempt=$((attempt + 1))
done

echo "Failed to connect to Flink after $max_attempts attempts"
exit 1 