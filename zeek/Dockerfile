# Use the official Zeek image as the base
#FROM zeek/zeek:latest
FROM zeek/zeek:5.2.0

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    python3-pip python3-setuptools git cmake make gcc g++ \
    libssl-dev libpcap-dev net-tools curl dnsutils && \
    rm -rf /var/lib/apt/lists/*

# Install librdkafka from source (newer version for zeek-kafka v1.2.0 compatibility)
RUN curl -L https://github.com/edenhill/librdkafka/archive/v1.9.2.tar.gz | \
    tar xvz && \
    cd librdkafka-1.9.2/ && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf librdkafka-1.9.2

# Install Zeek Package Manager (zkg)
RUN pip3 install --no-cache-dir zkg 
RUN ldconfig
RUN zkg autoconfig --force

# Set environment variables for plugin compatibility
ENV LIBRDKAFKA_ROOT=/usr/local
ENV ZEEK_ROOT=/usr/local/zeek
ENV CMAKE_PREFIX_PATH=/usr/local/zeek
ENV ZEEK_PLUGIN_PATH="/usr/local/zeek/lib/zeek/plugins"

# Install Kafka plugin using zkg (log output on failure)
RUN zkg install --skiptests --force https://github.com/SeisoLLC/zeek-kafka.git || \
    (cat /usr/local/zeek/var/lib/zkg/logs/zeek-kafka.git-build.log && exit 1)

COPY zeek-config/. /usr/local/zeek/share/zeek/site/
COPY zeek-etc/. /usr/local/zeek/etc/

# Verify the Kafka plugin installation
RUN zeek -N Seiso::Kafka

# Default command to start Zeek
CMD ["zeekctl", "deploy"]
