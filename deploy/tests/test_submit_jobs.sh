#!/usr/bin/env bash
# Pins the supervisor's decisions with stubbed curl and flink: which restore
# point it picks, which jobs it thinks are running, what it submits, what it
# prunes, and what it says when a submission fails (Review Focus 1).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export NETSEC_FLINK_DATA="$tmp/data"
. "$HERE/../flink/submit-jobs.sh"
# The script sets -e for itself; a test must survive a failing check.
set +e

# Make a restore point of a given age: the _metadata file (what
# newest_restore_point reads) and its folder (what prune_restore_points sorts).
restore_point() { mkdir -p "$1"; touch -d "$2" "$1/_metadata" "$1"; }

# None yet: nothing printed.
assert_eq "" "$(newest_restore_point online-feature-job)" "no restore point"

# A savepoint alone is chosen.
restore_point "$DATA/savepoints/online-feature-job/savepoint-aaa" "2026-09-20 10:00"
assert_eq "$DATA/savepoints/online-feature-job/savepoint-aaa" "$(newest_restore_point online-feature-job)" "savepoint chosen"

# A newer retained checkpoint wins over it; another job's points never count.
restore_point "$DATA/checkpoints/online-feature-job/0123abcd/chk-7" "2026-09-21 10:00"
restore_point "$DATA/checkpoints/archive-job/ffff/chk-9" "2026-09-22 10:00"
assert_eq "$DATA/checkpoints/online-feature-job/0123abcd/chk-7" "$(newest_restore_point online-feature-job)" "newer checkpoint wins"

# A directory without _metadata (an incomplete checkpoint) is ignored.
mkdir -p "$DATA/checkpoints/online-feature-job/0123abcd/chk-8"
assert_eq "$DATA/checkpoints/online-feature-job/0123abcd/chk-7" "$(newest_restore_point online-feature-job)" "incomplete ignored"

# Running and about-to-run jobs count as active; finished and failed ones do not.
overview='{"jobs":[{"jid":"a1","name":"online-feature-job","start-time":1,"state":"RUNNING"},
{"jid":"b2","name":"archive-job","start-time":2,"state":"FAILED"},
{"jid":"c3","name":"archive-job","start-time":3,"state":"FINISHED"}]}'
assert_eq online-feature-job "$(active_job_names <<< "$overview")" "active job names"
assert_eq "" "$(active_job_names <<< '{"jobs":[]}')" "no jobs"

# submit: per-job checkpoint dir always; -s only when a restore point exists.
flink() { printf '%s\n' "$*" > "$tmp/flink-args"; return 0; }
submit online-feature-job online-feature-job.jar io.netsecml.platform.bootstrap.online.OnlineFeatureJob >/dev/null
args="$(cat "$tmp/flink-args")"
assert_eq 1 "$(grep -c -- "-Dexecution.checkpointing.dir=file://$DATA/checkpoints/online-feature-job" <<< "$args")" "per-job checkpoint dir"
assert_eq 1 "$(grep -c -- "-s file://$DATA/checkpoints/online-feature-job/0123abcd/chk-7" <<< "$args")" "resumes from the newest point"
assert_eq 1 "$(grep -c -- '-c io.netsecml.platform.bootstrap.online.OnlineFeatureJob' <<< "$args")" "names the main class"
assert_eq 1 "$(grep -c -- "/opt/netsec/jars/online-feature-job.jar\$" <<< "$args")" "submits the job's JAR"
rm -rf "$DATA/checkpoints/archive-job"
submit archive-job archive-job.jar io.netsecml.platform.bootstrap.archive.ArchiveJob >/dev/null
assert_eq 0 "$(grep -c -- ' -s ' "$tmp/flink-args")" "first start has no -s"

# prune keeps the three newest savepoints.
for i in 1 2 3 4 5; do restore_point "$DATA/savepoints/archive-job/savepoint-$i" "2026-09-0$i 10:00"; done
prune_restore_points archive-job
assert_eq "savepoint-3 savepoint-4 savepoint-5" "$(cd "$DATA/savepoints/archive-job" && printf '%s\n' * | sort | tr '\n' ' ' | sed 's/ $//')" "prune keeps the 3 newest"

# Review Focus 1: a failed submission logs how to start that job fresh, and the
# supervisor carries on to the next job.
curl() { printf '{"jobs":[]}'; }
flink() { printf '%s\n' "$*" >> "$tmp/calls"; return 1; }
out="$(supervise_once 2>&1)"
assert_eq 1 "$(grep -c "submitting online-feature-job failed" <<< "$out")" "failure is logged"
assert_eq 1 "$(grep -c "move $DATA/savepoints/online-feature-job and $DATA/checkpoints/online-feature-job aside" <<< "$out")" "the way out is named"
assert_eq 2 "$(grep -c 'run -d' "$tmp/calls")" "both jobs were still attempted"
unset -f curl flink

# Final review, Critical 1: runs that fail before their first checkpoint each
# leave an empty checkpoint folder behind. Three of those in a row must never
# cost the job its only good restore point -- which the newest run is still
# restoring from while the supervisor prunes.
DATA_SAVED="$DATA"
DATA="$tmp/crit1"
restore_point "$DATA/checkpoints/archive-job/good/chk-40" "2026-09-20 10:00"
touch -d "2026-09-20 10:00" "$DATA/checkpoints/archive-job/good"
runs=0
# flink: each submission creates the new run's empty checkpoint folder (as the
# JobManager does while initialising the job) and succeeds.
flink() {
  runs=$((runs + 1))
  case "$*" in
    *archive-job.jar)
      mkdir -p "$DATA/checkpoints/archive-job/failed-$runs/shared"
      touch -d "@$(( $(date +%s) + runs ))" "$DATA/checkpoints/archive-job/failed-$runs" ;;
  esac
  return 0
}
curl() { printf '{"jobs":[]}'; }
for _ in 1 2 3 4; do supervise_once >/dev/null 2>&1; done
assert_eq "$DATA/checkpoints/archive-job/good/chk-40" "$(newest_restore_point archive-job)" \
  "the only good restore point survives four runs that never checkpointed"
assert_eq 2 "$(find "$DATA/checkpoints/archive-job" -mindepth 1 -maxdepth 1 -type d | wc -l)" \
  "failed runs' empty folders are pruned, except the newest run's own"

# Final review, Important 2: a restore that fails on the TaskManager does not
# make 'flink run' fail, so the supervisor itself must notice being sent back
# to the same restore point again and again -- and say how to start fresh.
DATA="$tmp/imp2"
restore_point "$DATA/savepoints/online-feature-job/savepoint-x" "2026-09-20 10:00"
flink() { return 0; }
first="$(supervise_once 2>&1)"
supervise_once >/dev/null 2>&1
third="$(supervise_once 2>&1)"
assert_eq 0 "$(grep -c 'from the same restore point' <<< "$first")" "one resubmission is not yet a pattern"
assert_eq 1 "$(grep -c 'online-feature-job has been submitted 3 times in a row from the same restore point' <<< "$third")" \
  "the third resubmission from one restore point says so"
assert_eq 1 "$(grep -c "move $DATA/savepoints/online-feature-job and $DATA/checkpoints/online-feature-job aside" <<< "$third")" \
  "and names the way out"
unset -f flink curl
DATA="$DATA_SAVED"

# Server test, 2026-09-25: through a ClickHouse outage the archive job failed
# run after run from one restore point, and the third resubmission's warning
# sent the operator to move its saved state aside -- which was fine. The warning
# must quote the newest failed run's actual cause (Flink's exception history,
# JSON-escaped, read without jq), and offer the state way out only when that
# cause is a state-restore failure.
DATA_SAVED="$DATA"
# Two failed runs of archive-job, the NEWER one listed first: the cause must
# come from the newest by end-time, not from list order.
cat > "$tmp/overview.json" <<'EOF'
{"jobs":[{"jid":"new2","name":"archive-job","start-time":300,"end-time":400,"duration":100,"state":"FAILED","last-modification":400,"tasks":{"running":0,"total":32,"failed":32},"pending-operators":0},{"jid":"old1","name":"archive-job","start-time":100,"end-time":200,"duration":100,"state":"FAILED","last-modification":200,"tasks":{"running":0,"total":32,"failed":32},"pending-operators":0}]}
EOF
cat > "$tmp/exc-old.json" <<'EOF'
{"exceptionHistory":{"entries":[{"exceptionName":"java.lang.RuntimeException","stacktrace":"java.lang.RuntimeException: OLD CAUSE\n\tat x.y(Z.java:1)\n","timestamp":200,"failureLabels":{},"concurrentExceptions":[]}],"truncated":false}}
EOF
cat > "$tmp/exc-clickhouse.json" <<'EOF'
{"exceptionHistory":{"entries":[{"exceptionName":"org.apache.flink.runtime.JobException","stacktrace":"org.apache.flink.runtime.JobException: Recovery is suppressed by ExponentialDelayRestartBackoffTimeStrategy(currentRestartAttempt=11)\n\tat org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:219)\nCaused by: java.io.IOException: insert into \"feature_vectors\" failed\n\tat io.netsecml.platform.adapter.clickhouse.writer.ClientV2Inserter.insert(ClientV2Inserter.java:80)\nCaused by: java.net.ConnectException: Connection refused\n\tat java.base/sun.nio.ch.Net.pollConnect(Native Method)\n","timestamp":400,"failureLabels":{},"concurrentExceptions":[]}],"truncated":false}}
EOF
cat > "$tmp/exc-restore.json" <<'EOF'
{"exceptionHistory":{"entries":[{"exceptionName":"org.apache.flink.runtime.JobException","stacktrace":"org.apache.flink.runtime.JobException: Recovery is suppressed by ExponentialDelayRestartBackoffTimeStrategy(currentRestartAttempt=11)\n\tat org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler.handleFailure(ExecutionFailureHandler.java:219)\nCaused by: java.lang.Exception: Exception while creating StreamOperatorStateContext.\n\tat org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl.streamOperatorStateContext(StreamTaskStateInitializerImpl.java:330)\nCaused by: org.apache.flink.util.FlinkException: Could not restore keyed state backend for KeyedProcessOperator_0a1b(1/4) from any of the 1 provided restore options.\n\tat org.apache.flink.streaming.api.operators.BackendRestorerProcedure.createAndRestore(BackendRestorerProcedure.java:165)\nCaused by: com.esotericsoftware.kryo.KryoException: Encountered unregistered class ID: 13\n\tat com.esotericsoftware.kryo.util.DefaultClassResolver.readClass(DefaultClassResolver.java:137)\n","timestamp":400,"failureLabels":{},"concurrentExceptions":[]}],"truncated":false}}
EOF
flink() { return 0; }
# curl: the overview, and each failed run's exception history; the newest run's
# history is whichever file exc-new.json currently points at.
curl() {
  case "$*" in
    */jobs/overview) cat "$tmp/overview.json" ;;
    */jobs/new2/exceptions) cat "$tmp/exc-new.json" ;;
    */jobs/old1/exceptions) cat "$tmp/exc-old.json" ;;
  esac
}
# Three rounds from one restore point; the third one's output.
third_round() {
  DATA="$tmp/$1"
  restore_point "$DATA/checkpoints/archive-job/run1/chk-212" "2026-09-20 10:00"
  supervise_once >/dev/null 2>&1
  supervise_once >/dev/null 2>&1
  supervise_once 2>&1
}
way_out_archive="move $tmp/%s/savepoints/archive-job and $tmp/%s/checkpoints/archive-job aside"

# A ClickHouse outage: the cause is quoted, and the state is left alone.
cp "$tmp/exc-clickhouse.json" "$tmp/exc-new.json"
third="$(third_round outage)"
assert_eq 1 "$(grep -c 'archive-job has been submitted 3 times in a row from the same restore point' <<< "$third")" "outage: the pattern is still reported"
assert_eq 1 "$(grep -c 'Its last run failed with: java.net.ConnectException: Connection refused' <<< "$third")" "outage: the newest run's root cause is quoted"
assert_eq 0 "$(grep -c 'OLD CAUSE' <<< "$third")" "outage: an older run's cause is not"
assert_eq 1 "$(grep -c 'not a state-restore failure' <<< "$third")" "outage: it says this is not a state problem"
# shellcheck disable=SC2059  # the format is ours
assert_eq 0 "$(grep -cF "$(printf "$way_out_archive" outage outage)" <<< "$third")" "outage: no advice to move the state aside"

# A restore that no longer fits: the cause is quoted, and the way out given.
cp "$tmp/exc-restore.json" "$tmp/exc-new.json"
third="$(third_round restore)"
assert_eq 1 "$(grep -c 'Its last run failed with: com.esotericsoftware.kryo.KryoException: Encountered unregistered class ID: 13' <<< "$third")" "restore: the root cause is quoted"
assert_eq 1 "$(grep -c 'state-restore failure: its saved state no longer fits the job' <<< "$third")" "restore: it is named a state problem"
# shellcheck disable=SC2059  # the format is ours
assert_eq 1 "$(grep -cF "$(printf "$way_out_archive" restore restore)" <<< "$third")" "restore: the way out is given"
unset -f flink curl third_round

# The supervisor runs under 'set -e -o pipefail': a failed run with an empty
# exception history (nothing for grep to match) must fall back to the old
# warning, never end the supervisor. Run it as its container does.
printf '{"exceptionHistory":{"entries":[],"truncated":false}}' > "$tmp/exc-new.json"
restore_point "$tmp/strict/checkpoints/archive-job/run1/chk-212" "2026-09-20 10:00"
strictbin="$tmp/strictbin"
mkdir -p "$strictbin"
printf '#!/bin/sh\nexit 0\n' > "$strictbin/flink"
cat > "$strictbin/curl" <<EOF
#!/bin/sh
case "\$*" in
  */jobs/overview) cat "$tmp/overview.json" ;;
  */exceptions) cat "$tmp/exc-new.json" ;;
esac
EOF
chmod +x "$strictbin/curl" "$strictbin/flink"
out="$(PATH="$strictbin:$PATH" NETSEC_FLINK_DATA="$tmp/strict" bash -c '. "$1"; supervise_once; supervise_once; supervise_once' _ "$HERE/../flink/submit-jobs.sh" 2>&1)"; status=$?
assert_eq 0 "$status" "an empty exception history does not end the supervisor"
assert_eq 1 "$(grep -c "If its saved state no longer fits the job: 'deploy.sh down', move $tmp/strict/savepoints/archive-job" <<< "$out")" \
  "and falls back to the warning without a cause"
DATA="$DATA_SAVED"

# Under the script's own 'set -e -o pipefail', a first start -- no savepoint or
# checkpoint folder exists yet -- must submit both jobs, not die: run it the
# way its container does, with stub curl and flink on the PATH.
bin="$tmp/bin"
mkdir -p "$bin"
printf '#!/bin/sh\nprintf %s\n' "'{\"jobs\":[]}'" > "$bin/curl"
printf '#!/bin/sh\nexit 0\n' > "$bin/flink"
chmod +x "$bin/curl" "$bin/flink"
out="$(PATH="$bin:$PATH" NETSEC_FLINK_DATA="$tmp/fresh" bash "$HERE/../flink/submit-jobs.sh" once 2>&1)"; status=$?
assert_eq 0 "$status" "a first start survives set -e"
assert_eq 2 "$(grep -c 'with no saved state (first start)' <<< "$out")" "both jobs submitted fresh"

finish
