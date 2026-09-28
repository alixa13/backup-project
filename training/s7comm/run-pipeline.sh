#!/usr/bin/env bash
# One run end to end (spec section 6): acquire.sh (captures -> Zeek -> the platform's features),
# then the training pipeline in the throwaway CPU-PyTorch image, limited to 16 CPUs and 24 GB so
# the server's other work keeps running. Runs on server3.
# Usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TRAINING="$(cd "$HERE/.." && pwd)"
IMAGE="${S7_TRAIN_IMAGE:-netsec-ml/s7-train:1}"
data="${1:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"
jar="${2:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"
config="${3:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"
run="${4:?usage: run-pipeline.sh DATA_DIR ONLINE_JAR CONFIG RUN_NAME}"

# The features first; then the image, built once from training/docker/s7comm.Dockerfile.
bash "$HERE/acquire.sh" "$data" "$jar"
docker image inspect "$IMAGE" >/dev/null 2>&1 \
  || docker build -t "$IMAGE" -f "$TRAINING/docker/s7comm.Dockerfile" "$TRAINING/docker"

# The pipeline: config path relative to training/, data at /data, no network.
data="$(cd "$data" && pwd)"
docker run --rm --network none --cpus 16 --memory 24g --user "$(id -u):$(id -g)" \
  -v "$TRAINING:/work:ro" -v "$data:/data" -w /work "$IMAGE" \
  python -m netsec_ml.s7comm.pipeline --config "$config" --out "/data/$run"
