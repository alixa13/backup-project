#!/usr/bin/env bash
# deploy.sh doctor: read-only checks of this host (design §8). One line per
# check -- PASS, WARN, FAIL or INFO -- and exit status 1 if anything FAILed.

# True when something already listens on TCP port $1.
port_in_use() {
  ss -Hltn "sport = :$1" 2>/dev/null | grep -q .
}

# True when our own stack is running (then it is the one holding our ports).
own_stack_running() {
  [ -n "$(docker ps -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}" 2>/dev/null)" ]
}

# This host's network interface names, one per line ("veth1@if5" -> "veth1").
list_interfaces() {
  ip -o link show | awk -F': ' '{print $2}' | cut -d@ -f1
}

doctor_run() {
  local fails=0
  # Print one check; count the failures.
  _check() {
    printf '  %-5s %s\n' "$1" "$2"
    if [ "$1" = FAIL ]; then fails=$((fails + 1)); fi
  }
  log "doctor: checking this host (read-only)"

  # Operating system.
  local os=unknown
  if [ -r /etc/os-release ]; then os="$(. /etc/os-release && printf '%s' "${PRETTY_NAME:-unknown}")"; fi
  _check INFO "OS: ${os} ($(uname -m))"

  # Docker engine and compose plugin, and the two host tools the scripts use.
  if ! command -v docker >/dev/null 2>&1; then
    _check FAIL "Docker is not installed ('deploy.sh install' installs it)"
  elif ! docker info >/dev/null 2>&1; then
    _check FAIL "Docker is installed but this user cannot reach it: add the user to the 'docker' group, or run as root"
  else
    local version; version="$(docker version --format '{{.Server.Version}}')"
    if [ "${version%%.*}" -ge 24 ]; then _check PASS "Docker ${version}"; else _check FAIL "Docker ${version}: 24 or newer is needed"; fi
    if docker compose version >/dev/null 2>&1; then _check PASS "docker compose $(docker compose version --short)"
    else _check FAIL "the docker compose plugin is missing ('deploy.sh install' installs it)"; fi
  fi
  local tool
  for tool in curl jq; do
    if command -v "$tool" >/dev/null 2>&1; then _check PASS "$tool"; else _check FAIL "${tool} is missing ('deploy.sh install' installs it)"; fi
  done

  # Hardware, against the design's recommended minimums.
  local cores mem disk
  cores="$(nproc)"
  mem="$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)"
  disk="$(df -Pm "$DEPLOY_DIR" | awk 'NR == 2 {print $4}')"
  if [ "$cores" -ge 4 ]; then _check PASS "${cores} CPU cores"; else _check WARN "${cores} CPU cores (4 or more recommended)"; fi
  if [ "$mem" -ge 7800 ]; then _check PASS "${mem} MiB RAM"; else _check WARN "${mem} MiB RAM (8 GB or more recommended)"; fi
  if [ "$disk" -ge 51200 ]; then _check PASS "$((disk / 1024)) GiB free disk"; else _check WARN "$((disk / 1024)) GiB free disk (50 GB or more recommended)"; fi

  # Our published ports must be free, unless our own stack is what holds them.
  local var port
  for var in FLINK_UI_PORT KAFKA_HOST_PORT CLICKHOUSE_HTTP_PORT; do
    port="$(setting "$var")"
    if ! command -v ss >/dev/null 2>&1; then _check WARN "cannot check port ${port} (no 'ss' command)"
    elif port_in_use "$port" && ! own_stack_running; then _check FAIL "port ${port} (${var}) is taken by another process: change ${var} in deploy/.env"
    else _check PASS "port ${port} (${var})"; fi
  done

  # The capture interface Zeek will sniff.
  local iface interfaces
  iface="$(setting ZEEK_INTERFACE)"
  interfaces="$(list_interfaces | tr '\n' ' ')"
  if [ -z "$iface" ]; then _check WARN "ZEEK_INTERFACE is not set; this host has: ${interfaces}"
  elif list_interfaces | grep -qxF "$iface"; then _check PASS "capture interface ${iface}"
  else _check FAIL "capture interface ${iface} does not exist; this host has: ${interfaces}"; fi

  # Internet, for pulling images and building (401 = reachable, auth needed).
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 https://registry-1.docker.io/v2/ || true)"
  if [ "$code" = 401 ] || [ "$code" = 200 ]; then _check PASS "Docker Hub reachable"; else _check FAIL "Docker Hub unreachable (HTTP '${code}')"; fi

  if [ "$fails" -gt 0 ]; then
    log "doctor: ${fails} check(s) failed"
    return 1
  fi
  log "doctor: no failures"
}
