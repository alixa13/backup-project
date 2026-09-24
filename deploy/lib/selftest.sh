#!/usr/bin/env bash
# deploy.sh selftest and zeek-check (design §8, ruling P8).

# --- selftest ---

# Fill a selftest template: __UID__, __TS_REQ__, __TS_RESP__ (epoch seconds).
render_selftest() {
  sed -e "s/__UID__/$2/g" -e "s/__TS_REQ__/$3/g" -e "s/__TS_RESP__/$4/g" "$1"
}

# Epoch seconds with microseconds, like Zeek's own timestamps. Cut from %N
# (nanoseconds) rather than asked for as %6N: some date implementations
# ignore the width and print all nine digits.
now_epoch() {
  local ns
  ns="$(date +%s%N)"
  printf '%s.%s\n' "${ns:0:10}" "${ns:10:6}"
}

# TS plus MS milliseconds, to the microsecond.
plus_ms() { awk -v t="$1" -v ms="$2" 'BEGIN { printf "%.6f\n", t + ms / 1000 }'; }

# Remove every row a selftest run left in ClickHouse.
selftest_cleanup() {
  ch_query "ALTER TABLE feature_vectors DELETE WHERE connection_uid IN ('$2', '$3')" mutations_sync=1 >/dev/null \
    || warn "could not delete the selftest's feature rows"
  ch_query "ALTER TABLE invalid_events DELETE WHERE event_id LIKE '%$1%'" mutations_sync=1 >/dev/null \
    || warn "could not delete the selftest's DLQ rows"
}

# One Modbus and one S7comm request/response pair, in exactly the JSON Zeek
# writes, through Kafka, both jobs and ClickHouse. Documentation-only
# addresses and SELFTEST- uids keep it apart from real devices; its rows are
# deleted afterwards.
selftest_run() {
  load_env
  local run uid_m uid_s ts_req ts_resp work got dlq deadline
  run="SELFTEST-$(date -u +%Y%m%d%H%M%S)"
  uid_m="${run}-M"
  uid_s="${run}-S"
  ts_req="$(now_epoch)"
  ts_resp="$(plus_ms "$ts_req" 4)"

  # Render and publish the two pairs to the raw topics.
  work="$(mktemp -d)"
  render_selftest "${DEPLOY_DIR}/selftest/modbus.jsonl.template" "$uid_m" "$ts_req" "$ts_resp" > "${work}/modbus.jsonl"
  render_selftest "${DEPLOY_DIR}/selftest/s7comm.jsonl.template" "$uid_s" "$ts_req" "$ts_resp" > "${work}/s7comm.jsonl"
  log "selftest ${run}: publishing one Modbus and one S7comm request/response pair"
  compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic "$MODBUS_RAW_TOPIC" < "${work}/modbus.jsonl"
  compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic "$S7COMM_RAW_TOPIC" < "${work}/s7comm.jsonl"
  rm -rf "$work"

  # The archive job writes on each 30 s checkpoint: allow a few of them.
  deadline=$(( $(date +%s) + 180 ))
  while :; do
    got="$(ch_query "SELECT countIf(log_type = 'modbus'), countIf(log_type = 's7comm') FROM feature_vectors WHERE connection_uid IN ('${uid_m}', '${uid_s}')" || true)"
    dlq="$(ch_query "SELECT count() FROM invalid_events WHERE event_id LIKE '%${run}%'" || true)"
    if [ "$got" = "$(printf '2\t2')" ] && [ "$dlq" = 0 ]; then
      break
    fi
    if [ "${dlq:-0}" != 0 ] || [ "$(date +%s)" -ge "$deadline" ]; then
      warn "selftest FAILED: feature vectors (modbus, s7comm) = '${got}', want 2 and 2; DLQ rows = '${dlq}', want 0"
      ch_query "SELECT log_type, reason_code, detail FROM invalid_events WHERE event_id LIKE '%${run}%' FORMAT PrettyCompactMonoBlock" >&2 || true
      selftest_cleanup "$run" "$uid_m" "$uid_s"
      return 1
    fi
    sleep 5
  done
  selftest_cleanup "$run" "$uid_m" "$uid_s"
  log "selftest PASSED: 2 Modbus and 2 S7comm feature vectors reached ClickHouse, no DLQ rows (test rows removed)"
}

# --- zeek-check ---

# The newest N records of a single-partition topic, one JSON object per line.
live_records() {
  local topic="$1" n="$2" end start
  end="$(compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic "$topic" </dev/null | sum_offsets)"
  [ "$end" -gt 0 ] || return 0
  start=$(( end > n ? end - n : 0 ))
  compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic "$topic" \
    --partition 0 --offset "$start" --max-messages $(( end - start )) --timeout-ms 20000 </dev/null 2>/dev/null
}

# ZeekRecordCheck from the online JAR, on the Flink image's Java 21, over
# DIR/modbus_detailed.jsonl and DIR/s7comm.jsonl. Its exit status is ours.
run_record_check() {
  docker run --rm --entrypoint java -v "${DEPLOY_DIR}/jars:/jars:ro" -v "$1:/in:ro" "$FLINK_IMAGE" \
    -cp /jars/online-feature-job.jar io.netsecml.platform.bootstrap.online.ZeekRecordCheck \
    --modbus /in/modbus_detailed.jsonl --s7comm /in/s7comm.jsonl
}

# deploy.sh zeek-check [--live N]: do the sensor's records fit our parsers?
# Offline: our Zeek image over ICSNPP's own sample traces. --live N: the
# newest N records on each raw topic, i.e. what this sensor really wrote.
zeek_check_run() {
  load_env
  [ -f "${DEPLOY_DIR}/jars/online-feature-job.jar" ] || die "job JARs missing: run 'deploy.sh build' first"
  local work="${DEPLOY_DIR}/.cache/zeek-check"
  rm -rf "$work"
  mkdir -p "$work"
  if [ "${1:-}" = --live ]; then
    local n="${2:-500}"
    log "zeek-check: the newest ${n} records on each raw topic"
    live_records "$MODBUS_RAW_TOPIC" "$n" > "${work}/modbus_detailed.jsonl"
    live_records "$S7COMM_RAW_TOPIC" "$n" > "${work}/s7comm.jsonl"
  else
    docker image inspect "$ZEEK_IMAGE" >/dev/null 2>&1 || die "Zeek image ${ZEEK_IMAGE} missing: run 'deploy.sh build' first"
    log "zeek-check: ${ZEEK_IMAGE} over ICSNPP's own sample traces"
    fetch_traces "${DEPLOY_DIR}/.cache/traces"
    zeek_offline_records "$ZEEK_IMAGE" "${DEPLOY_DIR}/.cache/traces" "$work"
  fi
  run_record_check "$work"
}
