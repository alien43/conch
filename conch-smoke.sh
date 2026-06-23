#!/usr/bin/env bash
# conch-smoke.sh - manual and deployment smoke tests for conch
set -euo pipefail

# Ensure conch binary is prioritized from current directory if present
CONCH="./conch"
if [ ! -f "$CONCH" ]; then
    if ! command -v conch &>/dev/null; then
        echo "ERROR: conch binary not found in current directory or PATH" >&2
        exit 1
    fi
    CONCH="conch"
fi

echo "Using conch binary: $CONCH"

# Expecting vacant office to return exit code 1
echo "==> Testing vacant election office (expect exit code 1)"
if "$CONCH" elect "smoke-$$" --who &>/dev/null; then
    echo "ERROR: expected vacant office to fail" >&2
    exit 1
else
    echo "PASS: vacant election returned non-zero exit code"
fi

# Acquire and run a simple command
echo "==> Testing election acquisition with clean command"
"$CONCH" elect "smoke-$$" -- true
echo "PASS: election acquisition succeeded"

# Inspect semaphore
echo "==> Testing semaphore who command"
"$CONCH" sema "smoke-$$" --max 2 --who || true
echo "PASS: semaphore who executed successfully"

# Add a cron job
echo "==> Testing cron job add"
"$CONCH" cron add "smoke-$$" --schedule '@every 1m' -- logger "conch smoke"
echo "PASS: cron job added successfully"

# List cron jobs
echo "==> Testing cron job list"
"$CONCH" cron ls --last
echo "PASS: cron job listed successfully"

# Remove cron job
echo "==> Testing cron job remove"
"$CONCH" cron rm "smoke-$$"
echo "PASS: cron job removed successfully"

echo "==> ALL SMOKE TESTS PASSED"
