#!/bin/bash
set -e

# One shaded jar holds every job; the job is chosen with -c <main class>.
# Previously each job had its own 147MB uber-jar of the same code (882MB total).
JAR="/opt/flink/jars/flink-1.0-SNAPSHOT-all.jar"

# "<main class>|<display name>|<parallelism>"
JOBS=(
  "com.example.UnifiedSupervisedFlinkJob|Unified Supervised (UNSW42)|16"
  "com.example.UnsupervisedFlinkKafkaConsumerConnImproved|Unsupervised Connection|16"
  "com.example.UnsupervisedFlinkKafkaConsumerDNSImproved|Unsupervised DNS|16"
  "com.example.UnsupervisedFlinkKafkaConsumerSSLImproved|Unsupervised SSL|16"
  "com.example.UnsupervisedFlinkKafkaConsumerHTTPImproved|Unsupervised HTTP|16"
)

echo "Waiting for $JAR ..."
for i in $(seq 1 20); do
    [ -f "$JAR" ] && { echo "JAR is available."; break; }
    echo "Waiting for JAR... (attempt $i/20)"
    sleep 15
done
if [ ! -f "$JAR" ]; then
    echo "ERROR: $JAR not found. Build it with 'mvn package' and copy target/*-all.jar into flink/jars/."
    exit 1
fi

echo "Waiting for Flink cluster to be ready..."
for i in $(seq 1 30); do
    curl -s http://localhost:8081/jobs > /dev/null 2>&1 && { echo "Flink cluster is ready."; break; }
    echo "Waiting for Flink cluster... (attempt $i/30)"
    sleep 10
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Flink cluster is not ready after 30 attempts"
        exit 1
    fi
done

echo "Waiting for ML services to be ready..."
sleep 30

failed=0
for entry in "${JOBS[@]}"; do
    IFS='|' read -r main_class name parallelism <<< "$entry"
    echo "Submitting $name (parallelism $parallelism)..."
    if flink run -c "$main_class" -p "$parallelism" -d "$JAR"; then
        echo "$name submitted successfully"
        # Let the Kafka consumer group settle before starting the next job.
        sleep 30
    else
        echo "ERROR: Failed to submit $name"
        failed=$((failed + 1))
    fi
done

if [ "$failed" -gt 0 ]; then
    echo "$failed job(s) failed to submit"
    exit 1
fi
echo "All Flink jobs have been submitted"
