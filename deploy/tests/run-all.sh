#!/usr/bin/env bash
# Every deploy/ check that needs no running stack (the development machine
# never runs one): the bash unit tests, the compose definition, the shaded JARs,
# the offline Zeek policy, and shellcheck (in a container).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
status=0

# The unit tests, one file at a time; a failing file fails the run.
for test in test_common.sh test_tune.sh test_compose.sh test_submit_jobs.sh \
            test_install_doctor.sh test_build.sh test_stack.sh test_selftest.sh check-jars.sh test_zeek_policy.sh; do
  bash "${HERE}/${test}" || status=1
done

# The shellcheck linter, pinned, over every script; not assumed on the host.
# The globs expand in deploy/ (the subshell's directory), so the relative
# paths they produce are valid inside the container's /mnt too -- the image
# has no shell of its own to expand them.
(cd "${HERE}/.." && docker run --rm -v "$PWD:/mnt:ro" -w /mnt koalaman/shellcheck:v0.10.0 \
  deploy.sh lib/*.sh flink/submit-jobs.sh zeek/run-zeek.sh tests/*.sh) || status=1

[ "$status" -eq 0 ] && echo "run-all: every check passed" || echo "run-all: FAILURES above"
exit "$status"
