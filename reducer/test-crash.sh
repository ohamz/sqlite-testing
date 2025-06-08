#!/bin/bash

QUERY_FILE="$1"

# Fallback realpath implementation for Alpine or busybox
if ! command -v realpath &> /dev/null; then
  realpath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
  }
fi

TMP_DIR=$(mktemp -d)
cp "$QUERY_FILE" "$TMP_DIR/query.sql"

IMAGE="theosotr/sqlite3-test"
DB="/data/test.db"
QUERY="/data/query.sql"
OUT1="$TMP_DIR/out1.txt"
OUT2="$TMP_DIR/out2.txt"

# Make sure query.sql exists
if [ ! -f "$TMP_DIR/query.sql" ]; then
    echo "ERROR: Failed to copy query file"
    exit 1
fi

# Run the query with sqlite3-3.26.0 and capture the output
docker run --rm -v "$(realpath "$TMP_DIR")":/data "$IMAGE" \
    /usr/bin/sqlite3-3.26.0 "$DB" < "$QUERY" > "$OUT1" 2>&1
EXIT_CODE=$?

# Function to check if exit code indicates a crash
is_crash() {
    local code=$1
    case $code in
        132) return 0 ;; # SIGILL - Illegal instruction
        134) return 0 ;; # SIGABRT - Abort signal
        136) return 0 ;; # SIGFPE - Floating point exception
        137) return 0 ;; # SIGKILL - Killed (often OOM)
        139) return 0 ;; # SIGSEGV - Segmentation fault
        143) return 0 ;; # SIGTERM - Terminated
    esac
}

# Also check for crash-related keywords in output
has_crash_keywords() {
    local output_file=$1
    if [ -f "$output_file" ]; then
        grep -qi -E "(segmentation fault|segfault|core dumped|abort|fatal|crashed)" "$output_file"
        return $?
    fi
    return 1
}

if is_crash "$EXIT_CODE" || has_crash_keywords "$OUT1"; then
    echo "CRASH detected (exit code: $EXIT_CODE)"
    rm -rf "$TMP_DIR"
    exit 0  # Crash detected
else
    echo "No crash detected (exit code: $EXIT_CODE)"
    rm -rf "$TMP_DIR"
    exit 1  # No crash
fi