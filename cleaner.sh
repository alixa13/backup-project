#!/usr/bin/env bash
set -euo pipefail

# Containers
PG_CTN=${PG_CTN:-postgres}
KAFKA_CTN=${KAFKA_CTN:-kafka}

# Kafka topics to purge (adjust if needed)
TOPICS=(
  "zeek-conn" "zeek-dns" "zeek-http" "zeek-ssl"
  "malicious-conn" "malicious-dns" "malicious-http" "malicious-ssl"
  "supervised-conn" "supervised-dns" "supervised-http" "supervised-ssl"
)

echo "Cleaning DB tables and purging Kafka topics (topics are kept)..."
read -r -p "Proceed? [y/N] " ans
[[ "${ans,,}" == "y" || "${ans,,}" == "yes" ]] || exit 1

echo "Checking containers..."
docker ps --format '{{.Names}}' | grep -qx "$PG_CTN"    || { echo "Postgres container $PG_CTN not running"; exit 1; }
docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CTN" || { echo "Kafka container $KAFKA_CTN not running"; exit 1; }

echo "Cleaning PostgreSQL data..."
docker exec "$PG_CTN" psql -U lstm_user -d lstm_db -c "
  TRUNCATE TABLE
    collected_data_rows,
    collected_data,
    model_info,
    total_rows,
    buffer_status
  RESTART IDENTITY;
  UPDATE learning_status SET enabled=false, last_updated=NOW();
"

echo "Purging Kafka topics (retention.ms trick)..."
for t in "${TOPICS[@]}"; do
  echo "  Purging $t ..."
  docker exec "$KAFKA_CTN" kafka-configs.sh --bootstrap-server localhost:9092 \
    --alter --entity-type topics --entity-name "$t" --add-config retention.ms=1000 >/dev/null
done

sleep 3

for t in "${TOPICS[@]}"; do
  docker exec "$KAFKA_CTN" kafka-configs.sh --bootstrap-server localhost:9092 \
    --alter --entity-type topics --entity-name "$t" --delete-config retention.ms >/dev/null
done

echo "Done. DB emptied; topics retained but messages purged."
