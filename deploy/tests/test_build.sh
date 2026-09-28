#!/usr/bin/env bash
# Pins build.sh's retry: a download cut off mid-way (seen on a real server:
# Maven Central ended a 52 MB transfer after 2.7 MB) must not end the build on
# the first attempt; a failure that persists must still end it, clearly.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/build.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export NETSEC_RETRY_PAUSE=0

# A step that fails twice, then succeeds, is retried until it succeeds.
calls=0
flaky() { calls=$((calls + 1)); [ "$calls" -ge 3 ]; }
out="$(with_retries 3 "building the job JARs" flaky 2>&1)"; status=$?
assert_eq 0 "$status" "a flaky step succeeds within 3 attempts"
assert_eq 2 "$(grep -c 'retrying (attempt [23] of 3)' <<< "$out")" "each retry is announced"
calls=0
with_retries 3 "building the job JARs" flaky >/dev/null 2>&1
assert_eq 3 "$calls" "the step ran three times"

# A step that keeps failing ends the build after the last attempt, saying so.
broken() { printf '%s\n' "$*" >> "$tmp/broken-calls"; return 1; }
out="$(with_retries 3 "building the Zeek image" broken a b 2>&1)"; status=$?
assert_eq 1 "$status" "a persistent failure still fails"
assert_eq 3 "$(wc -l < "$tmp/broken-calls")" "after exactly 3 attempts"
assert_eq "a b" "$(head -n 1 "$tmp/broken-calls")" "with the step's own arguments"
assert_eq 1 "$(grep -c 'building the Zeek image failed 3 times' <<< "$out")" "the last failure is named"

# A step that succeeds at once runs once and says nothing about retries.
calls=0
once() { calls=$((calls + 1)); }
out="$(with_retries 3 "anything" once 2>&1)"
assert_eq 0 "$(grep -c 'retrying' <<< "$out")" "no retry when the first attempt works"

finish
