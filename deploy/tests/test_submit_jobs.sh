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
