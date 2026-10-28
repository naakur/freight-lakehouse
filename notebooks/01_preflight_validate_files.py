# Databricks notebook source
# MAGIC %md
# MAGIC ## Preflight: validate files BEFORE Auto Loader sees them
# MAGIC
# MAGIC New files land in `inbox/<source>/`. For each file we check:
# MAGIC 1. not empty  2. is a `.csv`  3. header matches the expected columns  4. has at least one data row
# MAGIC
# MAGIC * PASS -> moved to `batch/<source>/`  (Auto Loader reads from there)
# MAGIC * FAIL -> moved to `quarantine_files/<source>/`  and logged with the reason
# MAGIC
# MAGIC A bad file never blocks the good ones.

# COMMAND ----------
# MAGIC %run ./ops_common

# COMMAND ----------

log_event("preflight_validate_files", "STARTED")


from utilities.preflight import check_file   # rules live in src/freight_etl/utilities/preflight.py


# COMMAND ----------

passed, failed = 0, 0
log_rows = []

for source in SOURCES:
    inbox = f"{LANDING_ROOT}/inbox/{source}/"
    for f in dbutils.fs.ls(inbox):
        if f.isDir():
            continue

        # Read only the first 4 KB - enough to see the header and the first row
        head = dbutils.fs.head(f.path, 4096) if f.size > 0 else ""
        reason = check_file(source, f.name, f.size, head)

        if reason is None:
            dest = f"{LANDING_ROOT}/batch/{source}/{f.name}"
            status = "PASSED"
            passed += 1
        else:
            dest = f"{LANDING_ROOT}/quarantine_files/{source}/{f.name}"
            status = "QUARANTINED"
            failed += 1

        dbutils.fs.mv(f.path, dest)
        log_rows.append((RUN_ID, source, f.name, dest, f.size, status, reason))

# COMMAND ----------

from pyspark.sql import functions as F

# Save the per-file results
if log_rows:
    cols = "run_id string, source string, file_name string, file_path string, size_bytes long, status string, reason string"
    (spark.createDataFrame(log_rows, cols)
          .withColumn("event_ts", F.current_timestamp())
          .write.mode("append").saveAsTable(f"{CATALOG}.ops.file_validation_log"))

# A quarantined file is not a failure of the job, but someone should look: WARN
final_status = "WARN" if failed else "SUCCESS"
log_event("preflight_validate_files", final_status,
          message=f"{passed} file(s) passed, {failed} quarantined", rows_in=passed + failed, rows_out=passed)
