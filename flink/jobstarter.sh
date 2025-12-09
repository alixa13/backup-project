#!/bin/bash
set -e

# Ensure the JAR files exist
JAR_DIR="/opt/flink/jars"

# Supervised job JARs
SUPERVISED_CONN_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-conn.jar"
SUPERVISED_DNS_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-dns.jar"
SUPERVISED_SSL_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-ssl.jar"
SUPERVISED_HTTP_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-http.jar"

# Unsupervised job JARs
UNSUPERVISED_CONN_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-unsupervised-conn.jar"
UNSUPERVISED_DNS_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-unsupervised-dns.jar"
UNSUPERVISED_SSL_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-unsupervised-ssl.jar"
UNSUPERVISED_HTTP_JAR="$JAR_DIR/flink-1.0-SNAPSHOT-unsupervised-http.jar"

echo "Waiting for JAR files to be available..."
MAX_RETRY=20
COUNT=0

while [ $COUNT -lt $MAX_RETRY ]; do
    if [ -f "$SUPERVISED_CONN_JAR" ] && [ -f "$SUPERVISED_DNS_JAR" ] && [ -f "$SUPERVISED_SSL_JAR" ] && [ -f "$SUPERVISED_HTTP_JAR" ] && \
       [ -f "$UNSUPERVISED_CONN_JAR" ] && [ -f "$UNSUPERVISED_DNS_JAR" ] && [ -f "$UNSUPERVISED_SSL_JAR" ] && [ -f "$UNSUPERVISED_HTTP_JAR" ]; then
        echo "All JAR files are available."
        break
    fi
    echo "Waiting for JAR files... (attempt $(($COUNT+1))/$MAX_RETRY)"
    sleep 15
    COUNT=$((COUNT+1))
done

if [ $COUNT -eq $MAX_RETRY ]; then
    echo "WARNING: Not all JAR files are available. Will try to submit available jobs only."
fi

# Wait for Flink to be fully ready
echo "Waiting for Flink cluster to be ready..."
MAX_FLINK_RETRY=30
FLINK_COUNT=0

while [ $FLINK_COUNT -lt $MAX_FLINK_RETRY ]; do
    if curl -s http://localhost:8081/jobs > /dev/null 2>&1; then
        echo "Flink cluster is ready."
        break
    fi
    echo "Waiting for Flink cluster... (attempt $(($FLINK_COUNT+1))/$MAX_FLINK_RETRY)"
    sleep 10
    FLINK_COUNT=$((FLINK_COUNT+1))
done

if [ $FLINK_COUNT -eq $MAX_FLINK_RETRY ]; then
    echo "ERROR: Flink cluster is not ready after $MAX_FLINK_RETRY attempts"
    exit 1
fi

# Wait for ML services to be ready
echo "Waiting for ML services to be ready..."
sleep 30

# Function to submit a job if the JAR exists
submit_job() {
    local jar_path=$1
    local job_name=$2
    local parallelism=$3
    
    if [ -f "$jar_path" ]; then
        echo "Submitting $job_name job with parallelism $parallelism..."
        flink run -p "$parallelism" -d "$jar_path"
        if [ $? -eq 0 ]; then
            echo "$job_name job submitted successfully"
            # Wait a bit between job submissions to avoid overwhelming the cluster
            sleep 5
        else
            echo "ERROR: Failed to submit $job_name job"
        fi
    else
        echo "WARNING: $jar_path not found, skipping $job_name job"
    fi
}

# Submit supervised jobs (moderate parallelism)
echo "Submitting supervised jobs..."
SUP_PAR=4
submit_job "$SUPERVISED_CONN_JAR" "Supervised Connection" "$SUP_PAR"
submit_job "$SUPERVISED_DNS_JAR" "Supervised DNS" "$SUP_PAR"
submit_job "$SUPERVISED_SSL_JAR" "Supervised SSL" "$SUP_PAR"
submit_job "$SUPERVISED_HTTP_JAR" "Supervised HTTP" "$SUP_PAR"

# Wait a bit before submitting unsupervised jobs
echo "Waiting before submitting unsupervised jobs..."
sleep 10

# Submit unsupervised jobs (higher parallelism to keep up with ingestion)
echo "Submitting unsupervised jobs..."
# Increase unsupervised parallelism to push ingestion ≈ processing
UNSUP_PAR=16
submit_job "$UNSUPERVISED_CONN_JAR" "Unsupervised Connection" "$UNSUP_PAR"
submit_job "$UNSUPERVISED_DNS_JAR" "Unsupervised DNS" "$UNSUP_PAR"
submit_job "$UNSUPERVISED_SSL_JAR" "Unsupervised SSL" "$UNSUP_PAR"
submit_job "$UNSUPERVISED_HTTP_JAR" "Unsupervised HTTP" "$UNSUP_PAR"

echo "All available Flink jobs have been submitted"
