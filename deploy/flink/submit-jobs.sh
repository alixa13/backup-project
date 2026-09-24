#!/usr/bin/env bash
# netsec-ml job supervisor, run by the job-submitter container (design §6,
# ruling P9). Every 60 s: each job the session cluster is not running is
# submitted again from its newest restore point. So 'deploy.sh up', a server
# reboot and a JobManager restart all end with both jobs running on their
# saved state. Both jobs' main() runs here, reading this container's env.
set -euo pipefail

DATA="${NETSEC_FLINK_DATA:-/flink-data}"
JARS="${NETSEC_JARS:-/opt/netsec/jars}"
REST="${NETSEC_FLINK_REST:-http://flink-jobmanager:8081}"
# name  jar  main class
JOBS=(
  "online-feature-job online-feature-job.jar io.netsecml.platform.bootstrap.online.OnlineFeatureJob"
  "archive-job archive-job.jar io.netsecml.platform.bootstrap.archive.ArchiveJob"
)

log() { printf '%s [job-submitter] %s\n' "$(date -u +%FT%TZ)" "$*"; }

# The newest restore point of a job: a savepoint written by 'deploy.sh down' or
# a checkpoint retained after a crash, whichever completed last. A restore
# point is a directory holding a _metadata file. Prints nothing if none exists.
newest_restore_point() {
  # '|| true': before a job's first savepoint or checkpoint its folders do not
  # exist, and under 'set -e -o pipefail' a failing find would end the supervisor.
  { find "${DATA}/savepoints/$1" "${DATA}/checkpoints/$1" -name _metadata -type f -printf '%T@ %h\n' 2>/dev/null || true; } \
    | sort -n | tail -n 1 | cut -d' ' -f2-
}

# Names of the jobs the cluster runs or is about to run, from a /jobs/overview
# document on stdin. Terminal states (FINISHED, FAILED, CANCELED) are not active.
active_job_names() {
  tr '{' '\n' \
    | sed -n 's/.*"name":"\([^"]*\)".*"state":"\(RUNNING\|RESTARTING\|CREATED\|INITIALIZING\|RECONCILING\|FAILING\|CANCELLING\)".*/\1/p' \
    | sort -u
}

# Submit one job, detached, with its own checkpoint directory (which is what
# makes "the newest checkpoint of this job" answerable) and retained
# checkpoints; resume from its newest restore point when there is one.
submit() {
  local name="$1" jar="$2" class="$3" restore
  restore="$(newest_restore_point "$name")"
  local args=(run -d -c "$class"
    "-Dexecution.checkpointing.dir=file://${DATA}/checkpoints/${name}"
    "-Dexecution.checkpointing.savepoint-dir=file://${DATA}/savepoints/${name}"
    -Dexecution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION
    -Dexecution.checkpointing.num-retained=3)
  if [ -n "$restore" ]; then
    log "submitting ${name}, resuming from ${restore}"
    args+=(-s "file://${restore}")
  else
    log "submitting ${name} with no saved state (first start)"
  fi
  flink "${args[@]}" "${JARS}/${jar}"
}

# Keep the three newest savepoints and checkpoint runs of a job; the one just
# restored from and the new run's own directory are always among them.
prune_restore_points() {
  local dir
  for dir in "${DATA}/savepoints/$1" "${DATA}/checkpoints/$1"; do
    # '|| true' for the same reason as in newest_restore_point.
    { find "$dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null || true; } \
      | sort -rn | tail -n +4 | cut -d' ' -f2- | xargs -r rm -rf
  done
}

# One pass: submit every job that is not active. A failed submission is
# logged with the way out, and the next job is still tried (Review Focus 1).
supervise_once() {
  local active entry name jar class
  active="$(curl -fsS "${REST}/jobs/overview" | active_job_names)" || return 0
  for entry in "${JOBS[@]}"; do
    read -r name jar class <<< "$entry"
    grep -qxF "$name" <<< "$active" && continue
    if submit "$name" "$jar" "$class"; then
      prune_restore_points "$name"
    else
      log "submitting ${name} failed; retrying in 60 s. If it keeps failing while restoring, its saved state no longer fits the job: move ${DATA}/savepoints/${name} and ${DATA}/checkpoints/${name} aside to start it fresh"
    fi
  done
}

main() {
  # Wait for the JobManager, then supervise forever (or once, for a check).
  until curl -fsS "${REST}/overview" >/dev/null 2>&1; do
    log "waiting for the JobManager at ${REST}"
    sleep 5
  done
  while true; do
    supervise_once
    [ "${1:-supervise}" = once ] && return 0
    sleep 60
  done
}

# Run only when executed, so the tests can source the functions.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
