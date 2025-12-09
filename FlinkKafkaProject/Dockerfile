FROM flink:1.18.0-java11

# Set working directory
WORKDIR /opt/flink

# Create config directory
RUN mkdir -p /opt/flink/config

# Create mount points for jars and config
VOLUME ["/opt/flink/jars", "/opt/flink/config"]

# Copy default configuration
COPY src/main/resources/api-config.properties /opt/flink/config/

# Use the default entrypoint from Flink image
ENTRYPOINT ["/docker-entrypoint.sh"] 