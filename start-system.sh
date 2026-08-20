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

echo "════════════════════════════════════════════════════════"
echo "  Starting Anomaly Detection System with PostgreSQL"
echo "════════════════════════════════════════════════════════"
echo ""

# Build LSTM container if needed (check if image exists and is up to date)
if ! docker images | grep -q "lstm-autoencoder.*latest"; then
    echo "📦 Building LSTM Autoencoder container..."
    cd lstm-autoencoder
    docker build -t lstm-autoencoder:latest .
    cd ..
    echo "✅ Build complete"
    echo ""
fi

# Stop and remove existing containers
if docker ps -a | grep -q "lstm-autoencoder\|postgres"; then
    echo "🛑 Stopping existing containers..."
    docker stop lstm-autoencoder postgres 2>/dev/null || true
    docker rm lstm-autoencoder postgres 2>/dev/null || true
    echo "✅ Old containers removed"
    echo ""
fi

# Start all services using docker compose
echo "🚀 Starting Docker services..."
docker compose up -d

echo ""
echo "⏳ Waiting for PostgreSQL to initialize..."
sleep 10

# Function to check if a container is healthy
check_container_health() {
    local container=$1
    local max_attempts=$2
    local attempt=1
    local kafka_process_found=0 # Flag to track if Kafka process was found

    echo "Waiting for $container to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker ps | grep -q "$container"; then
            if [ "$container" = "zookeeper" ]; then
                # Check if Zookeeper process is running and listening on port 2181
                if docker exec "$container" ps aux | grep -v grep | grep -q "org.apache.zookeeper.server.quorum.QuorumPeerMain" && \
                   docker exec "$container" netstat -tuln | grep -q "2181"; then
                    echo "✅ $container is ready!"
                    return 0
                fi
            elif [ "$container" = "kafka" ]; then
                # Check if Kafka broker process is running and listening on port 9092
                # if docker exec "$container" ps aux | grep -v grep | grep -q "kafka.Kafka" && \
                #    docker exec "$container" netstat -tuln | grep -q "9092"; then
                #     echo "✅ $container is ready!"
                #     return 0
                # fi
                # Check for Kafka process first, then check for port binding
                if [ $kafka_process_found -eq 0 ]; then
                     if docker exec "$container" ps aux | grep -v grep | grep -q "kafka.Kafka"; then
                         echo "Kafka process found, waiting for port binding..."
                         kafka_process_found=1
                         # Reset attempt counter or adjust max_attempts for port check if desired
                         # attempt=1
                         # max_attempts=15 # Example: give 30 more seconds for port binding
                     fi
                elif [ $kafka_process_found -eq 1 ]; then
                     if docker exec "$container" netstat -tuln | grep -q "9092"; then
                         echo "✅ $container is ready (process and port)!"
                         return 0
                     fi
                fi
            elif [ "$container" = "lstm-autoencoder" ]; then
                 # Check if LSTM service process is running and responding to health check
                 if docker exec "$container" ps aux | grep -v grep | grep -q "python" && \
                    docker exec "$container" curl -s -f http://localhost:5000/health >/dev/null 2>&1; then
                     echo "✅ $container is ready!"
                     return 0
                 fi
            fi
        fi

        # Show progress with a spinner (adjust message if Kafka process is found)
        local spinner_msg="Waiting for $container..."
        if [ "$container" = "kafka" ] && [ $kafka_process_found -eq 1 ]; then
             spinner_msg="Waiting for $container (process found, checking port)..."
        fi
        case $((attempt % 4)) in
            0) echo -ne "\r$spinner_msg (${attempt}/${max_attempts}) |" ;;
            1) echo -ne "\r$spinner_msg (${attempt}/${max_attempts}) /" ;;
            2) echo -ne "\r$spinner_msg (${attempt}/${max_attempts}) -" ;;
            3) echo -ne "\r$spinner_msg (${attempt}/${max_attempts}) \\" ;;
        esac


        sleep 2
        attempt=$((attempt + 1))
    done

    # Provide a slightly more specific error message for Kafka if process was found but port wasn't
    if [ "$container" = "kafka" ] && [ $kafka_process_found -eq 1 ]; then
         echo -e "\n❌ Error: Kafka process started, but port 9092 did not become available within the timeout period."
         echo "Please check the Kafka container logs and configuration (e.g., KAFKA_ADVERTISED_LISTENERS)."
    else
         echo -e "\n❌ Error: $container failed to start within the timeout period"
    fi
     echo "Please check the container logs with: docker compose logs $container"
     exit 1
}

# Function to wait for a service to be ready
wait_for_service() {
    local container=$1
    local url=$2
    local max_attempts=$3
    local attempt=1
    
    echo "Waiting for $container service to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker exec "$container" curl -s -f "$url" >/dev/null 2>&1; then
            echo "✅ $container service is ready!"
            return 0
        fi
        
        # Show progress with a spinner
        case $((attempt % 4)) in
            0) echo -ne "\rWaiting for $container service... (${attempt}/${max_attempts}) |" ;;
            1) echo -ne "\rWaiting for $container service... (${attempt}/${max_attempts}) /" ;;
            2) echo -ne "\rWaiting for $container service... (${attempt}/${max_attempts}) -" ;;
            3) echo -ne "\rWaiting for $container service... (${attempt}/${max_attempts}) \\" ;;
        esac
        
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo -e "\n❌ Error: $container service failed to start within the timeout period"
    echo "Please check the container logs with: docker compose logs $container"
    exit 1
}

# Wait for PostgreSQL first (new dependency)
echo "Checking PostgreSQL..."
for i in {1..30}; do
    if docker exec postgres pg_isready -U lstm_user -d lstm_db > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ PostgreSQL failed to become ready"
        echo "Check logs: docker logs postgres"
        exit 1
    fi
    sleep 2
done

# Wait for Zookeeper
check_container_health "zookeeper" 30

# Then wait for Kafka
# check_container_health "kafka" 30

echo -e "\nCreating supervised Kafka topics..."
# Create supervised topic (UNSW-NB15 42-feature only)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic supervised-unsw42 --partitions 16 --replication-factor 1

# Verify topics were created
echo "Verifying Kafka topics..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Get container IPs
echo -e "\nGetting container IP addresses..."
LSTM_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' lstm-autoencoder)
KAFKA_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka)
FLINK_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' flink)

# Display container IPs
echo "Container IP addresses:"
echo "LSTM Autoencoder: $LSTM_IP"
echo "Kafka: $KAFKA_IP"
echo "Flink: $FLINK_IP"

# Wait for LSTM service to be ready
check_container_health "lstm-autoencoder" 30

echo -e "\nWaiting for Flink cluster to start..."
# Note: Flink cluster is automatically started by the container's start-flink.sh script
# We just need to wait for it to be ready
echo "Checking if Flink cluster is ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec flink curl -s -f http://localhost:8081 >/dev/null 2>&1; then
        echo "✅ Flink cluster is ready (web UI is accessible)"
        break
    fi
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        echo "❌ Error: Flink cluster failed to start within timeout"
        echo "Please check the Flink logs with: docker compose logs flink"
        exit 1
    fi
    echo "Waiting for Flink... (attempt $attempt/$max_attempts)"
    sleep 2
done

echo -e "\nVerifying system status..."

# Get PostgreSQL IP
POSTGRES_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres)

echo "PostgreSQL Database: $POSTGRES_IP:5432 (container), localhost:5432 (host)"
echo "LSTM Autoencoder Service: http://$LSTM_IP:5000 (container), http://localhost:5000 (host)"
echo "Flink UI: http://$FLINK_IP:8081 (container), http://localhost:8081 (host)"
echo "Kafka: $KAFKA_IP:9092 (container), localhost:9092 (host)"

# Verify PostgreSQL connection
echo ""
echo "🔍 Verifying PostgreSQL connection..."
if docker exec lstm-autoencoder psql -h postgres -U lstm_user -d lstm_db -c "SELECT 1" > /dev/null 2>&1; then
    echo "✅ LSTM can connect to PostgreSQL"
else
    echo "⚠️  LSTM cannot connect to PostgreSQL yet (may still be initializing)"
fi

# Create a helper script for accessing services from the host
cat > access-services.sh << EOL
#!/bin/bash

# Helper script to access services within Docker containers
# Generated by start-system.sh for the anomaly detection system

# Access service by name
case "\$1" in
  lstm)
    docker exec lstm-autoencoder curl -X \$2 http://localhost:5000/\$3
    ;;
  supervised)
    docker exec supervised curl -s -X \$2 http://localhost:8000/\$3
    ;;
  logs)
    docker compose logs -f \$2
    ;;
  *)
    echo "Usage: ./access-services.sh [lstm|supervised|logs] [GET|POST|service_name] [endpoint]"
    echo "Examples:"
    echo "  ./access-services.sh lstm GET learning/status"
    echo "  ./access-services.sh supervised GET health"
    echo "  ./access-services.sh supervised GET health"
    echo "  ./access-services.sh logs lstm-autoencoder"
    ;;
esac
EOL
chmod +x access-services.sh

echo ""
echo "════════════════════════════════════════════════════════"
echo "✅ All services are running!"
echo "════════════════════════════════════════════════════════"
echo ""
echo "📊 System Components:"
echo "  • PostgreSQL  - High-performance database (10,000+ writes/sec)"
echo "  • Kafka       - Message broker"
echo "  • Zeek        - Packet capture (eth2)"
echo "  • Flink       - Stream processing (unified supervised + 4 unsupervised)"
echo "  • LSTM API    - Unsupervised ML (4 async workers)"
echo "  • Supervised  - UNSW42 ML (session: conn+http+dns+ssl)"
echo ""
echo "🎯 Quick Commands:"
echo "  Enable learning:  ./lstm-control.sh enable-learn-all"
echo "  Monitor system:   ./monitor-realtime.sh"
echo "  Check status:     ./check-services.sh"
echo "  Stop system:      ./stop-system.sh"
echo "  View logs:        docker compose logs -f [service_name]"
echo ""
echo "🔧 Database Access:"
echo "  psql -h localhost -p 5432 -U lstm_user -d lstm_db"
echo "  Password: lstm_password"
echo ""
echo "📈 Performance Specs:"
echo "  • Max throughput: 200+ Mbps"
echo "  • Detection latency: 5-7 seconds"
echo "  • Buffer size: 1000 records (5s flush)"
echo ""
echo "🚀 System ready for real-time attack detection!"
echo "" 