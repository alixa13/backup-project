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
# checkpoints; resume from RESTORE (default: the job's newest restore point),
# or start fresh when it is empty.
submit() {
  local name="$1" jar="$2" class="$3"
  local restore="${4-$(newest_restore_point "$name")}"
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

# True when a savepoint or checkpoint-run folder holds a completed restore point.
has_metadata() {
  [ -n "$(find "$1" -name _metadata -type f -print -quit 2>/dev/null)" ]
}

# Prune a job's savepoints and checkpoint runs, newest first. Always kept: the
# newest folder (the run just submitted, which may not have checkpointed yet),
# the folder holding KEEP (the restore point that run is still reading), and
# the three newest folders that hold a completed restore point. Folders of
# runs that never completed a checkpoint go, so a string of failing runs can
# never push the last good restore point out (final review, Critical 1).
prune_restore_points() {
  local job="$1" keep="${2:-}" dir path good newest
  for dir in "${DATA}/savepoints/${job}" "${DATA}/checkpoints/${job}"; do
    good=0
    newest=1
    # '|| true' for the same reason as in newest_restore_point.
    while read -r path; do
      [ -n "$path" ] || continue
      if has_metadata "$path"; then good=$((good + 1)); fi
      if [ "$newest" -eq 1 ]; then newest=0; continue; fi
      if [ -n "$keep" ] && { [ "$keep" = "$path" ] || [[ "$keep" == "$path"/* ]]; }; then continue; fi
      if has_metadata "$path" && [ "$good" -le 3 ]; then continue; fi
      rm -rf "$path"
    done < <({ find "$dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null || true; } \
               | sort -rn | cut -d' ' -f2-)
  done
}

# How many submissions in a row have resumed a job from this same restore
# point (1 for a new one), kept in a file so it survives the supervisor's own
# restarts. A fresh start (empty RESTORE) resets it.
count_same_restore() {
  local name="$1" restore="$2" file="${DATA}/supervisor/$1.restore" previous="" count=0
  mkdir -p "${DATA}/supervisor"
  if [ -f "$file" ]; then read -r previous count < "$file" || true; fi
  if [ -n "$restore" ] && [ "$previous" = "$restore" ]; then
    count=$((count + 1))
  else
    count=1
  fi
  printf '%s %s\n' "${restore:--}" "$count" > "$file"
  printf '%s\n' "$count"
}

# How to start a job fresh, for the operator reading this container's log.
way_out() {
  printf "'deploy.sh down', move %s and %s aside (on the host they are under NETSEC_DATA_DIR/flink -- deploy/data/flink by default -- owned by uid 9999, so use sudo), then 'deploy.sh up'. The job then starts with no saved state, and the online job re-reads its raw topics from the oldest retained record (up to 7 days)" \
    "${DATA}/savepoints/$1" "${DATA}/checkpoints/$1"
}

# The jid of NAME's newest FAILED run (by end-time), from a /jobs/overview
# document on stdin. Prints nothing if it has none. This container has no jq,
# so the JSON is split per object, like active_job_names does.
newest_failed_run() {
  tr '{' '\n' \
    | sed -n 's/.*"jid":"\([^"]*\)","name":"'"$1"'".*"end-time":\([0-9]*\).*"state":"FAILED".*/\2 \1/p' \
    | sort -n | tail -n 1 | cut -d' ' -f2
}

# The newest entry's stack trace, with real newlines, from a
# /jobs/<jid>/exceptions document on stdin. Escaped quotes inside messages
# become single quotes first, so they cannot end the JSON string early.
failure_trace() {
  sed "s/\\\\\"/'/g" \
    | grep -o '"stacktrace":"[^"]*"' | head -n 1 \
    | sed 's/^"stacktrace":"//; s/"$//; s/\\n/\n/g; s/\\t/\t/g'
}

# The root cause of a stack trace on stdin: its last 'Caused by:' line, or its
# first line when it has none; cut short for a log line.
root_cause() {
  awk '/^Caused by: / { cause = substr($0, 12) } NR == 1 { first = $0 }
       END { print (cause != "" ? cause : first) }' | cut -c1-300
}

# True when a stack trace on stdin is a failure to restore saved state into the
# job: state that no longer fits the code, not an outage around it.
is_restore_failure() {
  grep -qE 'StreamOperatorStateContext|Could not restore (keyed|operator) state backend|StateMigrationException|Cannot map checkpoint/savepoint state|Failed to rollback to checkpoint/savepoint'
}

# One pass: submit every job that is not active. A failed submission is
# logged with the way out, and the next job is still tried (Review Focus 1).
supervise_once() {
  local active entry name jar class restore same
  active="$(curl -fsS "${REST}/jobs/overview" | active_job_names)" || return 0
  for entry in "${JOBS[@]}"; do
    read -r name jar class <<< "$entry"
    grep -qxF "$name" <<< "$active" && continue
    restore="$(newest_restore_point "$name")"
    if submit "$name" "$jar" "$class" "$restore"; then
      prune_restore_points "$name" "$restore"
      # A restore that fails on the TaskManager does not make 'flink run' fail,
      # so a job sent back to the same restore point again and again is the
      # signal that its state no longer fits (final review, Important 2).
      same="$(count_same_restore "$name" "$restore")"
      if [ -n "$restore" ] && [ "$same" -ge 3 ]; then
        # The newest failed run's own stack trace says whether the state is at
        # fault. An outage (ClickHouse or Kafka unreachable) fails a job the
        # same way over and over, and moving its state aside would not help.
        local failed trace=""
        failed="$(curl -fsS "${REST}/jobs/overview" 2>/dev/null | newest_failed_run "$name")" || true
        if [ -n "$failed" ]; then
          trace="$(curl -fsS "${REST}/jobs/${failed}/exceptions" 2>/dev/null | failure_trace)" || true
        fi
        if [ -z "$trace" ]; then
          log "${name} has been submitted ${same} times in a row from the same restore point (${restore}) without completing a new checkpoint. Check 'deploy.sh logs flink-taskmanager'. If its saved state no longer fits the job: $(way_out "$name")"
        elif is_restore_failure <<< "$trace"; then
          log "${name} has been submitted ${same} times in a row from the same restore point (${restore}) without completing a new checkpoint. Its last run failed with: $(root_cause <<< "$trace"). That is a state-restore failure: its saved state no longer fits the job. $(way_out "$name")"
        else
          log "${name} has been submitted ${same} times in a row from the same restore point (${restore}) without completing a new checkpoint. Its last run failed with: $(root_cause <<< "$trace"). That is not a state-restore failure, so leave its saved state alone: fix that cause (ClickHouse or Kafka unreachable, for example) and the job resumes from this restore point by itself."
        fi
      fi
    else
      log "submitting ${name} failed; retrying in 60 s. If it keeps failing while restoring, its saved state no longer fits the job: $(way_out "$name")"
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
