#!/usr/bin/env bash
# Entrypoint of the netsec-ml Zeek image. Writes the Kafka settings this
# container was given into a Zeek script, then runs Zeek with the sensor
# policy: live on ZEEK_INTERFACE, or over ZEEK_READ_FILE (a pcap; checks only).
set -euo pipefail

: "${NETSEC_KAFKA_BROKERS:?NETSEC_KAFKA_BROKERS must be host:port[,host:port]}"
modbus_topic="${MODBUS_RAW_TOPIC:-netsec.modbus.raw.v1}"
s7comm_topic="${S7COMM_RAW_TOPIC:-netsec.s7comm.raw.v1}"

# The values land inside Zeek string literals: allow only what a broker list
# and a Kafka topic name can contain, so none can break out of the quotes.
if ! [[ "$NETSEC_KAFKA_BROKERS" =~ ^[A-Za-z0-9.:,_-]+$ ]]; then
  echo "invalid NETSEC_KAFKA_BROKERS: ${NETSEC_KAFKA_BROKERS}" >&2
  exit 2
fi
for topic in "$modbus_topic" "$s7comm_topic"; do
  if ! [[ "$topic" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "invalid topic name: ${topic}" >&2
    exit 2
  fi
done

# The per-container settings, as Zeek redefs loaded after the policy.
env_script=/tmp/netsec-env.zeek
cat > "$env_script" <<EOF
redef Kafka::kafka_conf = table(["metadata.broker.list"] = "${NETSEC_KAFKA_BROKERS}", ["client.id"] = "netsec-zeek");
redef NetSec::modbus_topic = "${modbus_topic}";
redef NetSec::s7comm_topic = "${s7comm_topic}";
EOF

# Zeek writes no log files (the policy sends every stream to Kafka or
# nowhere); /tmp keeps any stray state out of the image's own directories.
cd /tmp
if [ -n "${ZEEK_READ_FILE:-}" ]; then
  exec zeek -C -r "$ZEEK_READ_FILE" /opt/netsec/netsec.zeek "$env_script"
fi
: "${ZEEK_INTERFACE:?ZEEK_INTERFACE must name the capture interface}"
exec zeek -C -i "$ZEEK_INTERFACE" /opt/netsec/netsec.zeek "$env_script"
