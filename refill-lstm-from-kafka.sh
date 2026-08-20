#!/usr/bin/env bash
#
# refill-lstm-from-kafka.sh — Re-fill LSTM DB after training cleared it (normal data flow)
#
# Data flow is always: Kafka → Flink (consumes) → LSTM API (HTTP). LSTM does NOT consume Kafka.
# After training, the LSTM DB (collected_data_rows) is cleared. This script restores refill by:
#  1. Enabling learning on the LSTM API so it stores data when Flink POSTs to it.
#  2. Optionally resetting Flink consumer groups to earliest so Flink re-reads zeek-* from the start.
#  3. Flink (already running) keeps consuming from Kafka and POSTing 33-feature vectors to the
#     LSTM API; with learning enabled, LSTM saves them to PostgreSQL.
#
# Usage:
#   ./refill-lstm-from-kafka.sh                    # All log types (conn, dns, http, ssl)
#   ./refill-lstm-from-kafka.sh conn               # Only conn
#   ./refill-lstm-from-kafka.sh http --no-reset    # Only http, do not reset Kafka offsets
#   ./refill-lstm-from-kafka.sh --no-reset         # All types, only enable learning
#
# After running: wait until enough data is collected (e.g. ./lstm-control.sh status conn), then
# run ./lstm-control.sh 9 (disable-learn-all) to train again.
#

set -e

LSTM_CONTAINER=${LSTM_CONTAINER:-lstm-autoencoder}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
LSTM_PORT=${LSTM_PORT:-5000}
API_BASE_URL="http://localhost:${LSTM_PORT}"

# Unsupervised Flink consumer groups and their topics (must match Flink job code)
declare -A GROUP_TOPIC=(
  [unsupervised-conn-improved-consumer-group]=zeek-conn
  [unsupervised-http-improved-consumer-group]=zeek-http
  [unsupervised-dns-improved-consumer-group]=zeek-dns
  [unsupervised-ssl-improved-consumer-group]=zeek-ssl
)
VALID_LOG_TYPES="conn dns http ssl"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}ℹ️  $*${NC}"; }
ok()    { echo -e "${GREEN}✅ $*${NC}"; }
warn()  { echo -e "${YELLOW}⚠️  $*${NC}"; }
err()   { echo -e "${RED}❌ $*${NC}"; exit 1; }

# Check containers
if ! docker ps --format '{{.Names}}' | grep -qx "$LSTM_CONTAINER"; then
  err "$LSTM_CONTAINER is not running. Start the system with ./start-system.sh"
fi
if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
  err "$KAFKA_CONTAINER is not running. Start the system with ./start-system.sh"
fi

# Check LSTM API (call from inside container so it works regardless of host port mapping)
resp=$(docker exec "$LSTM_CONTAINER" curl -s -o /dev/null -w "%{http_code}" "$API_BASE_URL/health" 2>/dev/null || true)
if [ "$resp" != "200" ]; then
  err "LSTM API not reachable (got $resp). Is $LSTM_CONTAINER running?"
fi

RESET_OFFSETS=true
SELECTED_LOGS=""
for arg in "$@"; do
  case "$arg" in
    --no-reset) RESET_OFFSETS=false ;;
    -h|--help)
      sed -n '1,19p' "$0" | sed 's/^# \?//'
      echo "Valid log types: conn, dns, http, ssl"
      exit 0
      ;;
    conn|dns|http|ssl)
      SELECTED_LOGS="$SELECTED_LOGS $arg"
      ;;
    *)
      warn "Unknown option or log type: $arg (valid: conn, dns, http, ssl)"
      exit 1
      ;;
  esac
done
SELECTED_LOGS=$(echo "$SELECTED_LOGS" | xargs)
[ -z "$SELECTED_LOGS" ] && SELECTED_LOGS="conn dns http ssl"

echo ""
info "Re-fill LSTM DB: enable learning so when Flink sends data to LSTM it is stored (Kafka → Flink → LSTM)."
info "Log type(s): $SELECTED_LOGS"
echo ""

# 1. Enable learning for selected log types only (use docker exec so it works from any host)
info "Enabling learning for: $SELECTED_LOGS"
for log_type in $SELECTED_LOGS; do
  r=$(docker exec "$LSTM_CONTAINER" curl -s -X POST "$API_BASE_URL/learning/enable/$log_type" 2>/dev/null || echo "{}")
  if echo "$r" | jq -e '.learning_enabled == true' >/dev/null 2>&1; then
    ok "  $log_type learning enabled"
  else
    warn "  $log_type enable failed: $(echo "$r" | jq -r '.message // .error // .')"
  fi
done
echo ""

# 2. When resetting offsets: stop Flink (so consumer groups are inactive), reset, then start Flink
if [ "$RESET_OFFSETS" = true ]; then
  if docker ps --format '{{.Names}}' | grep -qx "flink"; then
    info "Stopping Flink so consumer groups can be reset..."
    docker compose stop flink 2>/dev/null || docker stop flink 2>/dev/null || true
    sleep 3
    ok "Flink stopped"
  fi

  info "Resetting Flink consumer groups to earliest (selected logs only)..."
  reset_failed=0
  for log_type in $SELECTED_LOGS; do
    case "$log_type" in
      conn) group="unsupervised-conn-improved-consumer-group"; topic="zeek-conn" ;;
      http) group="unsupervised-http-improved-consumer-group"; topic="zeek-http" ;;
      dns)  group="unsupervised-dns-improved-consumer-group";  topic="zeek-dns"  ;;
      ssl)  group="unsupervised-ssl-improved-consumer-group"; topic="zeek-ssl"  ;;
      *)    warn "  Skip unknown: $log_type"; continue ;;
    esac
    out=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --group "$group" \
      --topic "$topic" \
      --reset-offsets \
      --to-earliest \
      --execute 2>&1) || true
    # Success: Kafka may print "Executing" or a table with NEW-OFFSET (wurstmeister / older Kafka)
    if echo "$out" | grep -qE "Executing|NEW-OFFSET"; then
      ok "  $group → $topic reset to earliest"
    else
      warn "  $group → $topic reset failed"
      [ -n "$out" ] && echo "$out" | sed 's/^/     /'
      reset_failed=1
    fi
  done
  if [ "$reset_failed" = 1 ]; then
    echo ""
    warn "Some resets failed. See errors above."
  fi

  info "Starting Flink again..."
  docker compose start flink 2>/dev/null || docker start flink 2>/dev/null || true
  ok "Flink started (jobs will auto-submit via jobstarter)"
  echo ""
else
  info "Skipping Kafka offset reset (--no-reset). Current offsets will be used."
  echo ""
fi

ok "Done. Normal flow: Flink consumes Kafka → POSTs to LSTM API → LSTM (learning on) saves to DB."
echo ""
info "Next steps:"
echo "  • Wait for data to accumulate (Flink reads zeek-* from Kafka and sends to LSTM)."
echo "  • Check counts:  ./lstm-control.sh status <log_type>"
echo "  • When ready:    ./lstm-control.sh 9   (disable-learn-all) to train again"
echo ""
