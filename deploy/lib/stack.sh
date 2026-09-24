#!/usr/bin/env bash
# deploy.sh up / down / restart / status (design §8).

# --- small filters, pinned by tests/test_stack.sh ---

# Names of RUNNING jobs, from a /jobs/overview document on stdin.
running_job_names() { jq -r '.jobs[] | select(.state == "RUNNING") | .name' | sort -u; }

# "jid name" for each RUNNING job, from a /jobs/overview document on stdin.
running_jobs() { jq -r '.jobs[] | select(.state == "RUNNING") | "\(.jid) \(.name)"'; }

# Total records ever written to a topic, from kafka-get-offsets output
# (topic:partition:offset lines) on stdin.
sum_offsets() { awk -F: 'NF >= 3 { s += $NF } END { print s + 0 }'; }

# Seconds since the newest completed checkpoint, from a /jobs/<id>/checkpoints
# document on stdin; "none" before the first one.
checkpoint_age() {
  jq -r --argjson now "$1" \
    'if .latest.completed == null then "none"
     else (($now - .latest.completed.latest_ack_timestamp) / 1000 | floor | tostring) end'
}

# True when both jobs are RUNNING on the cluster.
jobs_running() {
  [ "$(flink_rest /jobs/overview | running_job_names | grep -cxE 'online-feature-job|archive-job')" -eq 2 ]
}

# True when at least one TaskManager has registered.
taskmanager_registered() { [ "$(flink_rest /overview | jq '.taskmanagers')" -ge 1 ]; }

# --- up ---

# Refuse to start anything that cannot work: missing JARs or Zeek image, an
# unset or unknown capture interface (Review Focus 2), no resources block.
stack_preflight() {
  [ -f "${DEPLOY_DIR}/jars/online-feature-job.jar" ] && [ -f "${DEPLOY_DIR}/jars/archive-job.jar" ] \
    || die "job JARs missing: run 'deploy.sh build' first"
  docker image inspect "$ZEEK_IMAGE" >/dev/null 2>&1 || die "Zeek image ${ZEEK_IMAGE} missing: run 'deploy.sh build' first"
  local interfaces
  interfaces="$(list_interfaces | tr '\n' ' ')"
  [ -n "${ZEEK_INTERFACE:-}" ] || die "ZEEK_INTERFACE is not set: run 'deploy.sh install --interface <name>' (this host has: ${interfaces})"
  list_interfaces | grep -qxF "$ZEEK_INTERFACE" \
    || die "capture interface ${ZEEK_INTERFACE} does not exist (this host has: ${interfaces})"
  tune_check_drift
}

# Every topic both jobs subscribe to, created before they start: a missing
# topic crash-loops a whole job. '</dev/null' keeps 'compose exec' from
# swallowing the rest of topics.conf (Review Focus 3).
create_topics() {
  local var partitions hours topic
  while read -r var partitions hours; do
    case "$var" in ''|'#'*) continue ;; esac
    topic="${!var:?topics.conf names ${var}, which deploy/.env does not set}"
    compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
      --topic "$topic" --partitions "$partitions" --replication-factor 1 \
      --config "retention.ms=$(( hours * 3600000 ))" </dev/null >/dev/null
  done < "${DEPLOY_DIR}/kafka/topics.conf"
  log "topics: $(compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list </dev/null | tr '\n' ' ')"
}

# The ClickHouse tables, through the repository's own idempotent DDL script.
apply_ddl() {
  CLICKHOUSE_HOST="$(host_addr)" CLICKHOUSE_PORT="$CLICKHOUSE_HTTP_PORT" \
    bash "${REPO_ROOT}/scripts/database/apply-ddl.sh"
}

stack_up() {
  load_env
  stack_preflight
  prepare_data_dirs

  # Storage first, then its schema.
  log "starting Kafka and ClickHouse"
  compose up -d kafka clickhouse
  wait_for 180 "Kafka" compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
  wait_for 180 "ClickHouse" curl -fsS "http://$(host_addr):${CLICKHOUSE_HTTP_PORT}/ping"
  create_topics
  apply_ddl

  # Then Flink, and the supervisor that submits (or resumes) both jobs.
  log "starting Flink"
  compose up -d flink-jobmanager flink-taskmanager
  wait_for 180 "Flink JobManager" flink_rest /overview
  wait_for 180 "Flink TaskManager" taskmanager_registered
  log "starting the job supervisor (it submits both jobs, resuming any saved state)"
  compose up -d job-submitter
  wait_for 300 "both jobs RUNNING" jobs_running

  # The sensor last, so its first records find the jobs running.
  log "starting Zeek on ${ZEEK_INTERFACE}"
  compose up -d zeek
  stack_status
}

# --- down ---

stack_down() {
  load_env
  # The sensor and the supervisor first: no new input, and nobody to resubmit
  # the jobs being stopped.
  compose stop zeek job-submitter >/dev/null 2>&1 || true
  if flink_rest /overview >/dev/null 2>&1; then
    local jid name
    while read -r jid name; do
      [ -n "$jid" ] || continue
      log "stopping ${name} with a savepoint"
      compose exec -T flink-jobmanager flink stop \
        --savepointPath "file:///flink-data/savepoints/${name}" "$jid" </dev/null \
        || warn "the savepoint for ${name} failed: it will resume from its newest retained checkpoint"
    done < <(flink_rest /jobs/overview | running_jobs)
  fi
  compose down
  log "stopped; data kept in ${NETSEC_DATA_DIR_ABS}"
}

# --- status ---

stack_status() {
  load_env
  log "containers:"
  compose ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}' || true

  # Jobs, and how old each running job's newest checkpoint is.
  if flink_rest /overview >/dev/null 2>&1; then
    log "Flink jobs:"
    local now jid name
    now="$(date +%s%3N)"
    while read -r jid name; do
      [ -n "$jid" ] || continue
      printf '  %-20s RUNNING   last checkpoint %ss ago\n' "$name" "$(flink_rest "/jobs/${jid}/checkpoints" | checkpoint_age "$now")"
    done < <(flink_rest /jobs/overview | running_jobs)
    flink_rest /jobs/overview | jq -r '.jobs[] | select(.state != "RUNNING") | "  \(.name)  \(.state)"' | sort -u || true
  else
    warn "the Flink JobManager is not reachable"
  fi

  # Traffic: records ever written to each raw topic.
  local var
  log "raw topics (records written since the topic was created):"
  for var in MODBUS_RAW_TOPIC S7COMM_RAW_TOPIC; do
    printf '  %-26s %s\n' "${!var}" \
      "$(compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic "${!var}" </dev/null 2>/dev/null | sum_offsets)"
  done

  # What reached ClickHouse lately, and what was rejected.
  log "feature vectors archived in the last 5 minutes:"
  ch_query "SELECT log_type, count() AS rows FROM feature_vectors WHERE archived_at > now64(3) - INTERVAL 5 MINUTE GROUP BY log_type ORDER BY log_type FORMAT PrettyCompactMonoBlock" \
    || warn "ClickHouse is not reachable"
  log "DLQ rows in the last hour, by reason:"
  ch_query "SELECT log_type, reason_code, count() AS rows FROM invalid_events WHERE received_at > now64(3) - INTERVAL 1 HOUR GROUP BY log_type, reason_code ORDER BY rows DESC LIMIT 10 FORMAT PrettyCompactMonoBlock" \
    || true
}
