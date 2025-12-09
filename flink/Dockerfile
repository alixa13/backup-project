# Base image for Flink
FROM flink:1.18.0-java11

# Set working directory
WORKDIR /opt/flink

# Install supervisord and additional dependencies
RUN apt-get update && \
    apt-get install -y supervisor curl net-tools iputils-ping procps librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /opt/flink/jars /opt/flink/config /var/log/supervisor && \
    chmod -R 777 /var/log/supervisor

# Configure directories with proper permissions (checkpointing disabled)
RUN mkdir -p /opt/flink/savepoints && \
    chmod -R 777 /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/savepoints

# Copy configuration files
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY start-flink.sh /opt/flink/start-flink.sh
COPY jobstarter.sh /opt/flink/jobstarter.sh
COPY submit-jobs.sh /opt/flink/submit-jobs.sh

# Make all scripts executable
RUN chmod +x /opt/flink/start-flink.sh /opt/flink/jobstarter.sh /opt/flink/submit-jobs.sh && \
    chmod +x /opt/flink/bin/jobmanager.sh /opt/flink/bin/taskmanager.sh && \
    chmod +x /opt/flink/bin/*.sh

# Configure Flink
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin

# Configure directories (checkpointing disabled)
# ENV FLINK_CHECKPOINTS_DIR=/opt/flink/checkpoints
ENV FLINK_SAVEPOINTS_DIR=/opt/flink/savepoints

# Expose Flink's web UI port
EXPOSE 8081

# Set Flink Volumes
VOLUME ["/opt/flink/jars", "/opt/flink/config", "/opt/flink/savepoints"]

# Start supervisord, which will handle both processes
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
