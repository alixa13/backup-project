#!/usr/bin/env bash
# Minimal assertions for the deploy/ bash tests (no bats on the target hosts).
# Each test file sources this, calls assert_eq, and ends with `finish`.

TESTS_RUN=0
TESTS_FAILED=0

# assert_eq EXPECTED ACTUAL MESSAGE -- record one check; print it only on failure.
assert_eq() {
  TESTS_RUN=$((TESTS_RUN + 1))
  if [ "$1" != "$2" ]; then
    TESTS_FAILED=$((TESTS_FAILED + 1))
    printf 'FAIL: %s\n  expected: %s\n  actual:   %s\n' "$3" "$1" "$2"
  fi
}

# Print the file's tally; the exit status is non-zero if any check failed.
finish() {
  printf '%s: %d checks, %d failed\n' "$(basename "$0")" "$TESTS_RUN" "$TESTS_FAILED"
  [ "$TESTS_FAILED" -eq 0 ]
}
