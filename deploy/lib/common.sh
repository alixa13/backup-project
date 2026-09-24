#!/usr/bin/env bash
# Shared helpers for deploy.sh: paths, logging, the deploy/.env loader and
# editor, the compose wrapper, polling, and the ClickHouse and Flink REST
# clients. Sourced by deploy.sh and the tests, never executed on its own.

# Absolute paths to deploy/ and the repository root, from this file's location,
# so every command works from any working directory.
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${DEPLOY_DIR}/.." && pwd)"
ENV_FILE="${ENV_FILE:-${DEPLOY_DIR}/.env}"
COMPOSE_PROJECT="netsec-ml"

# Logging: stdout for progress, stderr for warnings and errors.
log()  { printf '[netsec-ml] %s\n' "$*"; }
warn() { printf '[netsec-ml] WARNING: %s\n' "$*" >&2; }
die()  { printf '[netsec-ml] ERROR: %s\n' "$*" >&2; exit 1; }
