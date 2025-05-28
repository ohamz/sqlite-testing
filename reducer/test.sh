#!/bin/bash

QUERY_FILE="$1"
MODE="$2"  # "crash" or "diff"

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

if [ "$MODE" = "crash" ]; then
    docker run --rm -v "$(realpath "$TMP_DIR")":/data "$IMAGE" \
        /usr/bin/sqlite3-3.26.0 "$DB" < "$QUERY" > "$OUT1" 2>&1
    EXIT_CODE=$?

    if [ "$EXIT_CODE" -eq 139 ]; then
        rm -rf "$TMP_DIR"
        exit 0
    else
        rm -rf "$TMP_DIR"
        exit 1
    fi

elif [ "$MODE" = "diff" ]; then
    docker run --rm -v "$(realpath "$TMP_DIR")":/data "$IMAGE" \
        sh -c "cat $QUERY | /usr/bin/sqlite3-3.26.0 $DB" > "$OUT1" 2>&1
    docker run --rm -v "$(realpath "$TMP_DIR")":/data "$IMAGE" \
        sh -c "cat $QUERY | /usr/bin/sqlite3-3.39.4 $DB" > "$OUT2" 2>&1

    if ! diff "$OUT1" "$OUT2" >/dev/null; then
        rm -rf "$TMP_DIR"
        exit 0
    else
        rm -rf "$TMP_DIR"
        exit 1
    fi

else
    echo "Unknown mode: $MODE"
    rm -rf "$TMP_DIR"
    exit 1
fi
