# Base image for Flink
FROM flink:1.18.0-java11

# Set working directory
WORKDIR /opt/flink

RUN apt-get update && \
    apt-get install -y curl net-tools iputils-ping procps librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flink/jars /opt/flink/config

# Configure checkpoint/savepoint directories with proper permissions
RUN mkdir -p /opt/flink/savepoints /opt/flink/checkpoints && \
    chmod -R 777 /opt/flink/savepoints /opt/flink/checkpoints && \
    chown -R flink:flink /opt/flink/savepoints /opt/flink/checkpoints

COPY start-flink.sh /opt/flink/start-flink.sh
COPY jobstarter.sh /opt/flink/jobstarter.sh

RUN chmod +x /opt/flink/start-flink.sh /opt/flink/jobstarter.sh && \
    chmod +x /opt/flink/bin/jobmanager.sh /opt/flink/bin/taskmanager.sh && \
    chmod +x /opt/flink/bin/*.sh

# Configure Flink
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin

ENV FLINK_CHECKPOINTS_DIR=/opt/flink/checkpoints
ENV FLINK_SAVEPOINTS_DIR=/opt/flink/savepoints

# Expose Flink's web UI port
EXPOSE 8081

# Set Flink Volumes
VOLUME ["/opt/flink/jars", "/opt/flink/config", "/opt/flink/savepoints", "/opt/flink/checkpoints"]

# supervisord was configured to run /opt/flink/bin/start-flink.sh, a path that does not
# exist (the script lives at /opt/flink/start-flink.sh), and compose overrode the command
# anyway - so it never ran. Start the cluster directly.
CMD ["/bin/bash", "-c", "/opt/flink/start-flink.sh"]
