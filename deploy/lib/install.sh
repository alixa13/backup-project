#!/usr/bin/env bash
# deploy.sh install / uninstall (design §8).

# Letters and digits only: safe unquoted in .env, YAML and a URL. Reads a fixed
# 512 bytes first, so no pipe is cut short under 'set -o pipefail'.
random_token() {
  local pool
  pool="$(head -c 512 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9')"
  printf '%s\n' "${pool:0:$1}"
}

# A KRaft cluster id: 16 random bytes, base64url without padding (22 chars).
kafka_cluster_id() {
  head -c 16 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n'
  printf '\n'
}

# Install OS packages with whichever package manager this host has.
install_os_packages() {
  local sudo=""
  [ "$(id -u)" -ne 0 ] && sudo=sudo
  if command -v apt-get >/dev/null 2>&1; then $sudo apt-get update -q && $sudo apt-get install -y -q "$@"
  elif command -v dnf >/dev/null 2>&1; then $sudo dnf install -y -q "$@"
  elif command -v yum >/dev/null 2>&1; then $sudo yum install -y -q "$@"
  else die "no apt-get, dnf or yum here: install $* by hand"; fi
}

# curl and jq, then Docker and its compose plugin -- each only if missing.
install_packages() {
  local missing=() tool
  for tool in curl jq; do command -v "$tool" >/dev/null 2>&1 || missing+=("$tool"); done
  if [ "${#missing[@]}" -gt 0 ]; then
    log "installing ${missing[*]}"
    install_os_packages "${missing[@]}"
  fi
  if ! command -v docker >/dev/null 2>&1; then
    log "installing Docker Engine and the compose plugin (get.docker.com)"
    local sudo=""
    [ "$(id -u)" -ne 0 ] && sudo=sudo
    curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
    $sudo sh /tmp/get-docker.sh
    rm -f /tmp/get-docker.sh
  elif ! docker compose version >/dev/null 2>&1; then
    log "installing the docker compose plugin"
    install_os_packages docker-compose-plugin
  else
    log "Docker $(docker version --format '{{.Server.Version}}' 2>/dev/null) and compose already installed"
  fi
}

# deploy/.env from the template (mode 600) with its generated values; an
# existing file keeps every value. A non-empty interface is always recorded.
create_env_file() {
  local interface="$1"
  if [ -f "$ENV_FILE" ]; then
    log "deploy/.env exists: keeping its values"
  else
    log "creating deploy/.env from .env.template"
    (umask 077 && cp "${DEPLOY_DIR}/.env.template" "$ENV_FILE")
    chmod 600 "$ENV_FILE"
    env_set "$ENV_FILE" SENSOR_ID "$(hostname -s)"
    env_set "$ENV_FILE" CLICKHOUSE_PASSWORD "$(random_token 32)"
    env_set "$ENV_FILE" KAFKA_CLUSTER_ID "$(kafka_cluster_id)"
  fi
  [ -n "$interface" ] && env_set "$ENV_FILE" ZEEK_INTERFACE "$interface"
  if [ -z "$(env_value "$ENV_FILE" ZEEK_INTERFACE)" ]; then
    warn "ZEEK_INTERFACE is empty: set it with 'install --interface <name>' before 'up' ('doctor' lists the interfaces)"
  fi
}

# The data folders, owned by the uid each image runs as: Kafka 1000 (appuser),
# ClickHouse 101, Flink 9999. The chown runs in a container, so no sudo.
prepare_data_dirs() {
  local d="$NETSEC_DATA_DIR_ABS"
  mkdir -p "${d}/kafka" "${d}/clickhouse" "${d}/flink/checkpoints" "${d}/flink/savepoints"
  docker run --rm --user 0 --entrypoint sh -v "${d}:/data" "$FLINK_IMAGE" -c \
    'chown -R 1000:1000 /data/kafka && chown -R 101:101 /data/clickhouse && chown -R 9999:9999 /data/flink'
}

# deploy.sh install [--interface IF]
install_run() {
  local interface=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --interface) interface="${2:?--interface needs a name}"; shift 2 ;;
      *) die "install: unknown option $1" ;;
    esac
  done
  install_packages
  create_env_file "$interface"
  load_env
  prepare_data_dirs
  tune_run
  log "install complete. Next: ./deploy/deploy.sh build, then ./deploy/deploy.sh up"
}

# deploy.sh uninstall [--purge]: stop (with savepoints), remove our containers,
# network and the built Zeek image; --purge also deletes every byte of data.
uninstall_run() {
  local purge=0 answer
  [ "${1:-}" = --purge ] && purge=1
  load_env
  if [ -n "$(docker ps -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}")" ]; then
    stack_down
  fi
  log "removing the netsec-ml containers and network, and ${ZEEK_IMAGE}"
  compose down --remove-orphans
  docker image rm -f "$ZEEK_IMAGE" >/dev/null 2>&1 || true
  if [ "$purge" -eq 1 ]; then
    printf 'This deletes ALL netsec-ml data in %s (Kafka topics, ClickHouse tables, Flink state).\nType "delete netsec-ml data" to confirm: ' "$NETSEC_DATA_DIR_ABS"
    read -r answer
    [ "$answer" = "delete netsec-ml data" ] || die "not confirmed; data kept"
    docker run --rm --user 0 --entrypoint sh -v "${NETSEC_DATA_DIR_ABS}:/data" "$FLINK_IMAGE" -c \
      'rm -rf /data/kafka /data/clickhouse /data/flink'
    log "data deleted; deploy/.env kept (delete it by hand to start completely fresh)"
  fi
}
