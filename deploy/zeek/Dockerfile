# netsec-ml Zeek sensor (design §4): Zeek 7.0 LTS plus exactly the three
# packages the platform's parsers were verified against. Every version is
# pinned, deliberately: icsnpp-modbus's current release (v2.0.0) writes
# modbus_detailed as ONE record per request/response pair, which neither the
# frozen Modbus model nor JsonZeekModbusParser can read; v1.0.0 writes one
# record per packet.
ARG ZEEK_BASE_IMAGE=zeek/zeek:7.0.9

# --- build stage: compile the two C++ plugins and install all three packages ---
FROM ${ZEEK_BASE_IMAGE} AS build
ARG ICSNPP_MODBUS_VERSION=v1.0.0
ARG ICSNPP_S7COMM_VERSION=7ebeb03a0f954541369361651d1c27d09a64b5a3
ARG ZEEK_KAFKA_VERSION=v1.2.0
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential cmake git librdkafka-dev libpcap-dev libssl-dev zlib1g-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN zkg autoconfig --force \
 && zkg install --force --skiptests https://github.com/cisagov/icsnpp-modbus --version "${ICSNPP_MODBUS_VERSION}" \
 && zkg install --force --skiptests https://github.com/cisagov/icsnpp-s7comm --version "${ICSNPP_S7COMM_VERSION}" \
 && zkg install --force --skiptests --user-var LIBRDKAFKA_ROOT=/usr \
      https://github.com/SeisoLLC/zeek-kafka --version "${ZEEK_KAFKA_VERSION}" \
 && zkg list

# --- runtime stage: the same Zeek, the installed packages, librdkafka only ---
# zeek-kafka links the C++ wrapper (librdkafka++.so.1) as well as the C library.
FROM ${ZEEK_BASE_IMAGE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends librdkafka1 librdkafka++1 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/zeek /usr/local/zeek
COPY netsec.zeek offline-json.zeek /opt/netsec/
COPY --chmod=0755 run-zeek.sh /opt/netsec/run-zeek.sh
ENTRYPOINT ["/opt/netsec/run-zeek.sh"]
