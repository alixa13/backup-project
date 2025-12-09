# Project Overview
Modular anomaly-detection stack with Zeek packet logs, Flink stream processing (supervised and unsupervised pipelines), an LSTM autoencoder API, a supervised ML API, PostgreSQL, Kafka, and Zeek capture services. Docker Compose is provided to run the full stack.

# Components
- docker-compose.yml orchestrates: postgres, kafka+zookeeper, flink, zeek, supervised API, lstm-autoencoder API.
- flink/: Java jobs (supervised and unsupervised) and container setup.
- FlinkKafkaProject/: Java sources and Maven config for shaded JARs.
- lstm-autoencoder/: Flask + Gunicorn LSTM autoencoder service using PostgreSQL.
- supervised/API/: Flask + Gunicorn supervised models. Large model artifacts are ignored from Git for size.
- zeek/: Zeek container config and capture settings.
- helper scripts: monitor, cleanup, control, deployment helpers.

# Prerequisites
- Docker and docker compose.
- Java 11, Maven (for rebuilding Flink JARs).
- Python 3.9+ if running services outside containers.

# Running with Docker Compose
1) Build and start: `docker compose up -d --build`
2) Flink UI: http://localhost:8081
3) Supervised API: http://localhost:8000
4) LSTM Autoencoder API: http://localhost:5000
5) Zeek runs in host network mode; ensure interface names match zeek command in docker-compose.

# Rebuilding Flink JARs
```
cd FlinkKafkaProject
mvn clean package
```
Shaded JARs will appear under target/ and should be mounted to flink/jars/.

# Local service runs (optional)
- LSTM autoencoder: `cd lstm-autoencoder && pip install -r requirements.txt && python run.py`
- Supervised API: `cd supervised/API && pip install -r requirements.txt && gunicorn --bind 0.0.0.0:8000 run:app`

# Database
PostgreSQL is used by lstm-autoencoder; schema is auto-initialized on start. Connection defaults are in environment variables within docker-compose.yml.

# Kafka Topics
Default topics (created by Kafka container): zeek-conn, zeek-http, zeek-dns, zeek-ssl, plus malicious-* outputs and supervised-* outputs.

# Models and Large Files
- Model artifacts (.joblib, .h5, etc.) are intentionally git-ignored to keep the repo under GitHub size limits. Place needed models into `supervised/API/app/models/` (or bind-mount via volumes) before running.
- PCAP samples are also ignored; mount your own captures to feed Zeek.

# Git / Push Notes
- Remote (SSH): git@github.com:alixa13/anomaly-with-correct-unsupervised.git
- If you amend history to drop large files, force push: `git push -u origin main -f`

# Quick Troubleshooting
- Permission denied (publickey) on push: ensure ssh-agent has your key (`ssh-add -l`), and GitHub has the public key.
- Large file push rejected: ensure models/pcaps remain untracked (see .gitignore).

