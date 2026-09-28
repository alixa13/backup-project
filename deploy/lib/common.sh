#!/usr/bin/env bash
# Shared helpers for deploy.sh: paths, logging, the deploy/.env loader and
# editor, the compose wrapper, polling, and the ClickHouse and Flink REST
# clients. Sourced by deploy.sh and the tests, never executed on its own.

# Absolute paths to deploy/ and the repository root, from this file's location,
# so every command works from any working directory.
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC2034  # read by the modules that source this file
REPO_ROOT="$(cd "${DEPLOY_DIR}/.." && pwd)"
ENV_FILE="${ENV_FILE:-${DEPLOY_DIR}/.env}"
COMPOSE_PROJECT="netsec-ml"

# Logging: stdout for progress, stderr for warnings and errors.
log()  { printf '[netsec-ml] %s\n' "$*"; }
warn() { printf '[netsec-ml] WARNING: %s\n' "$*" >&2; }
die()  { printf '[netsec-ml] ERROR: %s\n' "$*" >&2; exit 1; }

# Load deploy/.env, exporting every variable so docker compose and the helpers
# see the same values, then resolve the data directory to an absolute path.
load_env() {
  [ -f "$ENV_FILE" ] || die "deploy/.env not found: run './deploy/deploy.sh install --interface <if>' first"
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  NETSEC_DATA_DIR_ABS="$(data_dir_abs "${NETSEC_DATA_DIR:-./data}")"
  export NETSEC_DATA_DIR_ABS
}

# NETSEC_DATA_DIR is relative to deploy/ (as compose resolves it) unless absolute.
data_dir_abs() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *)  printf '%s\n' "${DEPLOY_DIR}/${1#./}" ;;
  esac
}

# Read KEY's value from an env-style file without sourcing it (last one wins).
env_value() {
  [ -f "$1" ] || return 0
  sed -n "s/^$2=//p" "$1" | tail -n 1
}

# Set KEY=VALUE in an env-style file: replace the line if present, else append.
# '|' is the sed delimiter; no value this tool writes ever contains one.
env_set() {
  if grep -q "^$2=" "$1"; then
    sed -i "s|^$2=.*|$2=$3|" "$1"
  else
    printf '%s=%s\n' "$2" "$3" >> "$1"
  fi
}

# A setting from deploy/.env if it has one, else the template's default -- for
# commands that must work before 'install' (doctor, build).
setting() {
  local value
  value="$(env_value "$ENV_FILE" "$1")"
  [ -n "$value" ] || value="$(env_value "${DEPLOY_DIR}/.env.template" "$1")"
  printf '%s\n' "$value"
}

# docker compose, always pinned to this project's name, file and env file, so
# no command here can act on another compose project on the host.
compose() {
  docker compose -p "$COMPOSE_PROJECT" -f "${DEPLOY_DIR}/docker-compose.yml" \
    --env-file "$ENV_FILE" "$@"
}

# Poll until a command succeeds, or fail after TIMEOUT seconds.
# usage: wait_for TIMEOUT DESCRIPTION COMMAND [ARGS...]
wait_for() {
  local timeout="$1" what="$2" waited=0
  shift 2
  until "$@" >/dev/null 2>&1; do
    [ "$waited" -ge "$timeout" ] && die "timed out after ${timeout}s waiting for ${what}"
    sleep 3
    waited=$((waited + 3))
  done
  log "${what}: ready"
}

# The address host-side tools use to reach our published ports: the bind
# address itself, or loopback when bound to every interface.
host_addr() {
  case "${BIND_ADDRESS:-127.0.0.1}" in
    0.0.0.0) printf '127.0.0.1\n' ;;
    *) printf '%s\n' "${BIND_ADDRESS:-127.0.0.1}" ;;
  esac
}

# ClickHouse over its published HTTP port with the deployment's credentials.
# usage: ch_query SQL [EXTRA_URL_PARAMS]   (prints TabSeparated rows)
ch_query() {
  curl --fail --silent --show-error \
    --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    --data-binary "$1" \
    "http://$(host_addr):${CLICKHOUSE_HTTP_PORT}/?database=${CLICKHOUSE_DATABASE}${2:+&$2}"
}

# The Flink REST API over its published port. usage: flink_rest /jobs/overview
flink_rest() {
  curl --fail --silent --show-error "http://$(host_addr):${FLINK_UI_PORT}$1"
}
