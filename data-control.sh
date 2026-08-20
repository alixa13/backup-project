#!/bin/bash
#
# data-control.sh — Data and system control (monitoring, purge, refill, configure)
# Similar to lstm-control.sh but for: monitor-realtime, purge-kafka, purge-db, refill-lstm-kafka, configure-resources.
#
# Usage:
#   ./data-control.sh                          # Interactive menu
#   ./data-control.sh monitor-realtime         # Run monitor-realtime.sh
#   ./data-control.sh purge-kafka              # Purge Kafka topic records
#   ./data-control.sh purge-db                 # Clear LSTM PostgreSQL collected data
#   ./data-control.sh refill-lstm-kafka [args] # Run refill-lstm-from-kafka.sh (stop Flink, reset, start Flink)
#   ./data-control.sh configure-resources     # Run configure-resources.sh
#
# Numeric shortcuts: 1=monitor-realtime, 2=purge-kafka, 3=purge-db, 4=refill-lstm-kafka, 5=configure-resources
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()   { echo -e "${BLUE}ℹ️  $*${NC}"; }
ok()     { echo -e "${GREEN}✅ $*${NC}"; }
warn()   { echo -e "${YELLOW}⚠️  $*${NC}"; }
err()    { echo -e "${RED}❌ $*${NC}"; exit 1; }

show_usage() {
  echo "Usage: $0 [command] [options]"
  echo ""
  echo "Commands:"
  echo "  monitor-realtime       Run ./monitor-realtime.sh (Kafka/LSTM/Postgres stats)"
  echo "  purge-kafka            Run ./purge-kafka.sh (truncate topic records)"
  echo "  purge-db               Clear LSTM PostgreSQL collected_data_rows and total_rows"
  echo "  refill-lstm-kafka      Run ./refill-lstm-from-kafka.sh (stop Flink, reset offsets, enable learning, start Flink)"
  echo "  configure-resources    Run ./configure-resources.sh (regenerate override + Flink config)"
  echo ""
  echo "Numeric shortcuts:  1=monitor-realtime  2=purge-kafka  3=purge-db  4=refill-lstm-kafka  5=configure-resources"
  echo ""
  echo "Examples:"
  echo "  $0"
  echo "  $0 1"
  echo "  $0 refill-lstm-kafka"
  echo "  $0 refill-lstm-kafka --no-reset"
}

# Require docker
if ! command -v docker &>/dev/null; then
  err "docker not found. Run this on a host with Docker."
fi

main() {
  # No args: show menu and prompt
  if [ $# -eq 0 ]; then
    echo ""
    echo -e "${BLUE}==================== Data & System Control ====================${NC}"
    echo ""
    echo -e "  ${GREEN}1${NC}) monitor-realtime    (Kafka/LSTM/Postgres real-time monitor)"
    echo -e "  ${GREEN}2${NC}) purge-kafka         (truncate zeek/malicious/supervised topic records)"
    echo -e "  ${GREEN}3${NC}) purge-db           (clear LSTM collected_data_rows + total_rows)"
    echo -e "  ${GREEN}4${NC}) refill-lstm-kafka  (stop Flink → reset offsets → enable learning → start Flink)"
    echo -e "  ${GREEN}5${NC}) configure-resources (regenerate docker-compose.override + flink-conf.yaml)"
    echo -e "  ${GREEN}6${NC}) exit"
    echo -e "${BLUE}===============================================================${NC}"
    echo ""
    read -rp "Select [1-6]: " choice
    case "$choice" in
      1) set -- monitor-realtime ;;
      2) set -- purge-kafka ;;
      3) set -- purge-db ;;
      4) set -- refill-lstm-kafka ;;
      5) set -- configure-resources ;;
      6) echo "Bye."; exit 0 ;;
      *) err "Invalid choice." ;;
    esac
  fi

  # Numeric shortcut
  case "$1" in
    1) shift; set -- monitor-realtime "$@" ;;
    2) shift; set -- purge-kafka "$@" ;;
    3) shift; set -- purge-db "$@" ;;
    4) shift; set -- refill-lstm-kafka "$@" ;;
    5) shift; set -- configure-resources "$@" ;;
  esac

  case "$1" in
    help|-h|--help)
      show_usage
      exit 0
      ;;

    monitor-realtime)
      if [ ! -f "$SCRIPT_DIR/monitor-realtime.sh" ]; then
        err "monitor-realtime.sh not found in $SCRIPT_DIR"
      fi
      exec "$SCRIPT_DIR/monitor-realtime.sh"
      ;;

    purge-kafka)
      if [ ! -f "$SCRIPT_DIR/purge-kafka.sh" ]; then
        err "purge-kafka.sh not found in $SCRIPT_DIR"
      fi
      info "Running purge-kafka.sh ..."
      "$SCRIPT_DIR/purge-kafka.sh"
      ok "purge-kafka done"
      ;;

    purge-db)
      POSTGRES_CONTAINER=${POSTGRES_CONTAINER:-postgres}
      if ! docker ps --format '{{.Names}}' | grep -qx "$POSTGRES_CONTAINER"; then
        err "Postgres container '$POSTGRES_CONTAINER' is not running. Start the system with ./start-system.sh"
      fi
      warn "This will clear LSTM collected_data_rows and total_rows (learning_status is kept)."
      read -rp "Continue? [y/N]: " confirm
      if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        info "Aborted."
        exit 0
      fi
      info "Purging LSTM DB (collected_data_rows, collected_data, total_rows)..."
      export PGPASSWORD="${PGPASSWORD:-lstm_password}"
      docker exec -e PGPASSWORD "$POSTGRES_CONTAINER" psql -U lstm_user -d lstm_db -v ON_ERROR_STOP=1 -c "
        TRUNCATE collected_data_rows;
        DELETE FROM collected_data;
        UPDATE total_rows SET count = 0;
      "
      unset PGPASSWORD
      ok "LSTM DB purged"
      ;;

    refill-lstm-kafka)
      shift
      if [ ! -f "$SCRIPT_DIR/refill-lstm-from-kafka.sh" ]; then
        err "refill-lstm-from-kafka.sh not found in $SCRIPT_DIR"
      fi
      info "Running refill-lstm-from-kafka.sh (will stop Flink, reset offsets, enable learning, start Flink) ..."
      "$SCRIPT_DIR/refill-lstm-from-kafka.sh" "$@"
      ok "refill-lstm-kafka done"
      ;;

    configure-resources)
      if [ ! -f "$SCRIPT_DIR/configure-resources.sh" ]; then
        err "configure-resources.sh not found in $SCRIPT_DIR"
      fi
      info "Running configure-resources.sh ..."
      "$SCRIPT_DIR/configure-resources.sh"
      ok "configure-resources done. Run: docker compose up -d"
      ;;

    *)
      show_usage
      err "Unknown command: $1"
      ;;
  esac
}

main "$@"
