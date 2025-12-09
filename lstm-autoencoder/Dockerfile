# Use an official Python base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Insert a new ENV line (after the WORKDIR /app) to set PYTHONPATH
ENV PYTHONPATH=/app

# Install PostgreSQL client for pg_isready command
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create necessary directories with appropriate permissions
RUN mkdir -p /app/data /app/models && \
    chmod 777 /app/data && \
    chmod 777 /app/models

# Create app directory for volume mount
RUN mkdir -p /app/app && \
    chmod 777 /app/app

# Make entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Expose the port that your Flask app listens on
EXPOSE 5000

# Use the entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"] 