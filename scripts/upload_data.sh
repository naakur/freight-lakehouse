#!/usr/bin/env bash
# Upload CSV files into the INBOX folders of one environment.
#
# Usage:
#   scripts/upload_data.sh <catalog> <folder_with_csvs>            # good files
#   scripts/upload_data.sh <catalog> sample_inbox_bad --bad        # practice files that must be rejected
set -euo pipefail
CATALOG="$1"; DIR="$2"; MODE="${3:-}"
BASE="dbfs:/Volumes/${CATALOG}/landing/raw/inbox"

if [ "$MODE" = "--bad" ]; then
  # file -> source folder it is dropped into
  databricks fs cp "$DIR/invoices_wrong_header.csv" "$BASE/invoices/invoices_wrong_header.csv" --overwrite
  databricks fs cp "$DIR/invoices_notes.txt"        "$BASE/invoices/invoices_notes.txt"        --overwrite
  databricks fs cp "$DIR/invoices_bad_rows.csv"     "$BASE/invoices/invoices_bad_rows.csv"     --overwrite
  databricks fs cp "$DIR/shipments_empty.csv"       "$BASE/shipments/shipments_empty.csv"      --overwrite
  databricks fs cp "$DIR/gl_codes_header_only.csv"  "$BASE/gl_codes/gl_codes_header_only.csv"  --overwrite
else
  for src in carriers rate_cards accessorial_codes gl_codes shipments invoices; do
    databricks fs cp "$DIR/${src}.csv" "$BASE/${src}/${src}.csv" --overwrite
  done
fi
echo "Uploaded to $BASE"
