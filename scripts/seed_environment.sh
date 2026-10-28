#!/usr/bin/env bash
# Puts the sample data into ONE environment (used by CI for the test environment; also handy by hand).
# IDEMPOTENT: a file that is already there is skipped, so running it twice never creates duplicates.
#
#   scripts/seed_environment.sh <catalog> <generated_data_dir>
#   (generated_data_dir contains batch/, stream/ and truth/ - made by data_generator/generate_freight_data.py)
set -euo pipefail
CATALOG="$1"; DATA="$2"
ROOT="dbfs:/Volumes/${CATALOG}/landing/raw"

put() {  # put <local file> <remote path>   -> uploads only if the remote file is missing
  if databricks fs ls "$2" >/dev/null 2>&1; then echo "skip   $2 (already there)"; else
    databricks fs cp "$1" "$2" && echo "upload $2"; fi
}

# batch CSVs: a file is "there" once preflight has moved it to batch/<source>/ (or while it waits in inbox/)
for src in carriers rate_cards accessorial_codes gl_codes shipments invoices; do
  f="$DATA/batch/${src}.csv"
  if databricks fs ls "$ROOT/batch/$src/${src}.csv" >/dev/null 2>&1; then echo "skip   $src (already ingested)"; else
    put "$f" "$ROOT/inbox/$src/${src}.csv"; fi
done

# carrier change feed: all three files (the late record is handled by sequence_by)
for f in sample_cdc/carriers_cdc_000*.csv; do
  name=$(basename "$f")
  if databricks fs ls "$ROOT/batch/carriers_cdc/$name" >/dev/null 2>&1; then echo "skip   $name (already ingested)"; else
    put "$f" "$ROOT/inbox/carriers_cdc/$name"; fi
done

# stream messages and ground truth
for f in "$DATA"/stream/kafka_batch_*.json; do put "$f" "$ROOT/stream/$(basename "$f")"; done
put "$DATA/truth/invoice_anomaly_labels.csv" "$ROOT/truth/invoice_anomaly_labels.csv"
echo "seed complete for $CATALOG"
