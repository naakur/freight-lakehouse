#!/usr/bin/env bash
# bash version of upload_stream.ps1:   scripts/upload_stream.sh <catalog> <dir> [files] [skip]
set -euo pipefail
CATALOG="$1"; DIR="$2"; FILES="${3:-0}"; SKIP="${4:-0}"
list=$(ls "$DIR"/kafka_batch_*.json | sort | tail -n +$((SKIP + 1)))
[ "$FILES" -gt 0 ] && list=$(echo "$list" | head -n "$FILES")
for f in $list; do databricks fs cp "$f" "dbfs:/Volumes/$CATALOG/landing/raw/stream/$(basename "$f")" --overwrite; done
