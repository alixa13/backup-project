#!/usr/bin/env bash
# Pins the Compose definition statically (`docker compose config`: nothing
# starts): services, restart policy, port bindings (P6), Zeek's capture
# rights, memory/CPU limits from the tune block, and the jobs' settings.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# A test env: the template, generated values, and host B's tune block.
make_env() {
  cp "${DEPLOY_DIR}/.env.template" "$tmp/env"
  env_set "$tmp/env" SENSOR_ID sensor-test
  env_set "$tmp/env" ZEEK_INTERFACE eth9
  env_set "$tmp/env" KAFKA_CLUSTER_ID bmV0c2VjLW1sLXRlc3QxMg
  env_set "$tmp/env" CLICKHOUSE_PASSWORD testpassword
  env_set "$tmp/env" BIND_ADDRESS "$1"
  write_tune_block "$tmp/env" "$(tune_compute 4 8192 6144 0 0 0 0)"
}

# The rendered project as JSON.
config() {
  docker compose -p netsec-ml -f "${DEPLOY_DIR}/docker-compose.yml" --env-file "$tmp/env" config --format json
}

make_env 127.0.0.1
json="$(config)"; status=$?
assert_eq 0 "$status" "compose config renders"
q() { jq -r "$1" <<< "$json"; }

assert_eq netsec-ml "$(q .name)" "project name"
assert_eq "clickhouse flink-jobmanager flink-taskmanager job-submitter kafka zeek" \
  "$(q '.services | keys | join(" ")')" "the six services"
assert_eq unless-stopped "$(q '[.services[].restart] | unique | join(",")')" "every service restarts unless stopped"
assert_eq 127.0.0.1 "$(q '[.services[].ports[]?.host_ip] | unique | join(",")')" "every port on loopback by default"
assert_eq "19092 18081 18123" \
  "$(q '[.services.kafka.ports[0].published, .services["flink-jobmanager"].ports[0].published, .services.clickhouse.ports[0].published] | map(tostring) | join(" ")')" \
  "published ports"
assert_eq host "$(q '.services.zeek.network_mode')" "zeek on the host network"
assert_eq "NET_ADMIN,NET_RAW" "$(q '.services.zeek.cap_add | sort | join(",")')" "zeek capture rights"
assert_eq eth9 "$(q '.services.zeek.environment.ZEEK_INTERFACE')" "zeek interface"
assert_eq 127.0.0.1:19092 "$(q '.services.zeek.environment.NETSEC_KAFKA_BROKERS')" "zeek reaches Kafka on loopback"
assert_eq "$((1382 * 1048576))" "$(q '.services["flink-taskmanager"].mem_limit')" "taskmanager memory from tune"
assert_eq "$((640 * 1048576))" "$(q '.services["job-submitter"].mem_limit')" "job supervisor memory from tune"
assert_eq 1 "$(q '.services["flink-taskmanager"].environment.FLINK_PROPERTIES' | grep -c '^taskmanager.memory.process.size: 1318m$')" "taskmanager process size (P5)"
assert_eq 0.45 "$(q '.services.kafka.cpus')" "kafka CPUs from tune"
assert_eq "-Xms384m -Xmx384m" "$(q '.services.kafka.environment.KAFKA_HEAP_OPTS')" "kafka heap"
assert_eq kafka:29092 "$(q '.services["job-submitter"].environment.KAFKA_BOOTSTRAP_SERVERS')" "jobs read Kafka inside the network"
assert_eq clickhouse "$(q '.services["job-submitter"].environment.CLICKHOUSE_HOST')" "archive job reaches ClickHouse by name"
assert_eq sensor-test "$(q '.services["job-submitter"].environment.SENSOR_ID')" "sensor id reaches the jobs"
assert_eq netsec.s7comm.raw.v1 "$(q '.services["job-submitter"].environment.S7COMM_RAW_TOPIC')" "topics reach the jobs"
# Each OT feature state's idle TTL reaches the online job's main(), which reads
# both; the template's defaults are an hour each.
assert_eq 60 "$(q '.services["job-submitter"].environment.MODBUS_STATE_TTL_MINUTES')" "modbus state TTL reaches the jobs"
assert_eq 60 "$(q '.services["job-submitter"].environment.S7COMM_STATE_TTL_MINUTES')" "s7comm state TTL reaches the jobs"
# A deploy/.env written before MODBUS_STATE_TTL_MINUTES existed (install never
# rewrites an existing one) must still hand main() a number: an empty value
# would fail the online job's Long.parseLong at startup.
sed -i '/^MODBUS_STATE_TTL_MINUTES=/d' "$tmp/env"
assert_eq 60 "$(config | jq -r '.services["job-submitter"].environment.MODBUS_STATE_TTL_MINUTES')" \
  "an older deploy/.env without the modbus TTL still gets the one-hour default"

# Modbus scoring (spec section 7): the pin and the models mount reach the jobs.
assert_eq modbus-stage1-detector/v1 "$(q '.services["job-submitter"].environment.MODBUS_DETECTOR_BUNDLE')" "the detector pin reaches the jobs"
assert_eq /opt/netsec/models "$(q '.services["job-submitter"].environment.NETSEC_MODELS_DIR')" "the models dir"
assert_eq netsec.modbus.prediction.v1 "$(q '.services["job-submitter"].environment.MODBUS_PREDICTION_TOPIC')" "the prediction topic"
# S7comm scoring (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md section 7).
assert_eq s7comm-stage1-detector/v2 "$(q '.services["job-submitter"].environment.S7COMM_DETECTOR_BUNDLE')" "the S7 detector pin reaches the jobs"
assert_eq netsec.s7comm.prediction.v1 "$(q '.services["job-submitter"].environment.S7COMM_PREDICTION_TOPIC')" "the S7 prediction topic"
for svc in job-submitter flink-taskmanager; do
  assert_eq "/opt/netsec/models:true" "$(q ".services[\"$svc\"].volumes[] | select(.target == \"/opt/netsec/models\") | \"\(.target):\(.read_only)\"")" "$svc mounts models/ read-only"
done
# Review Focus 1: unset (an older .env) scores with the default; set empty turns scoring off.
sed -i '/^MODBUS_DETECTOR_BUNDLE=/d; /^MODBUS_PREDICTION_TOPIC=/d' "$tmp/env"
assert_eq modbus-stage1-detector/v1 "$(config | jq -r '.services["job-submitter"].environment.MODBUS_DETECTOR_BUNDLE')" "an older .env scores with the default bundle"
assert_eq netsec.modbus.prediction.v1 "$(config | jq -r '.services["job-submitter"].environment.MODBUS_PREDICTION_TOPIC')" "and gets the default topic"
printf 'MODBUS_DETECTOR_BUNDLE=\n' >> "$tmp/env"
assert_eq "" "$(config | jq -r '.services["job-submitter"].environment.MODBUS_DETECTOR_BUNDLE')" "an empty pin turns scoring off"
make_env 127.0.0.1
# Review Focus 1: the same for the S7comm pin.
sed -i '/^S7COMM_DETECTOR_BUNDLE=/d; /^S7COMM_PREDICTION_TOPIC=/d' "$tmp/env"
assert_eq s7comm-stage1-detector/v2 "$(config | jq -r '.services["job-submitter"].environment.S7COMM_DETECTOR_BUNDLE')" "an older .env scores S7 with the default bundle"
assert_eq netsec.s7comm.prediction.v1 "$(config | jq -r '.services["job-submitter"].environment.S7COMM_PREDICTION_TOPIC')" "and gets the default S7 topic"
printf 'S7COMM_DETECTOR_BUNDLE=\n' >> "$tmp/env"
assert_eq "" "$(config | jq -r '.services["job-submitter"].environment.S7COMM_DETECTOR_BUNDLE')" "an empty S7 pin turns S7 scoring off"
make_env 127.0.0.1

# P6: widening BIND_ADDRESS moves the UI and ClickHouse, never Kafka.
make_env 0.0.0.0
json="$(config)"
assert_eq 127.0.0.1 "$(q '.services.kafka.ports[0].host_ip')" "kafka stays on loopback"
assert_eq 0.0.0.0 "$(q '.services["flink-jobmanager"].ports[0].host_ip')" "flink UI follows BIND_ADDRESS"

# setting() falls back to the template when deploy/.env lacks the key.
assert_eq 18081 "$(ENV_FILE="$tmp/none" setting FLINK_UI_PORT)" "setting falls back to .env.template"

finish
