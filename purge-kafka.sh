#!/usr/bin/env bash
set -euo pipefail

KAFKA_CTN=${KAFKA_CTN:-kafka}
BOOTSTRAP=${BOOTSTRAP:-localhost:9092}

TOPICS=(
  "zeek-conn" "zeek-dns" "zeek-http" "zeek-ssl"
  "malicious-conn" "malicious-dns" "malicious-http" "malicious-ssl"
  "supervised-unsw42"
)

echo "Purging records (not deleting topics) on $BOOTSTRAP via $KAFKA_CTN ..."
for t in "${TOPICS[@]}"; do
  echo "  Topic: $t"
  parts=$(docker exec "$KAFKA_CTN" kafka-topics.sh --describe --topic "$t" --bootstrap-server "$BOOTSTRAP" 2>/dev/null | awk '/Partition:/{print $2}')
  if [ -z "$parts" ]; then
    echo "    (topic not found or no partitions)"
    continue
  fi

  tmp=$(mktemp)
  {
    echo '{ "partitions": ['
    first=1
    for p in $parts; do
      if [ $first -eq 0 ]; then echo ","; fi
      first=0
      # offset -1 = truncate to latest (delete all existing records)
      echo "  { \"topic\": \"$t\", \"partition\": $p, \"offset\": -1 }"
    done
    echo '], "version":1 }'
  } > "$tmp"

  docker cp "$tmp" "$KAFKA_CTN:/tmp/delete-$t.json"
  docker exec "$KAFKA_CTN" kafka-delete-records.sh --bootstrap-server "$BOOTSTRAP" --offset-json-file "/tmp/delete-$t.json"
  docker exec "$KAFKA_CTN" rm "/tmp/delete-$t.json"
  rm "$tmp"
done

echo "Done. Topics retained; records truncated to latest offset."
