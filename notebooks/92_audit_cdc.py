# Databricks notebook source
# MAGIC %md
# MAGIC ## Audit the CDC / SCD results (runs after the batch audit)
# MAGIC
# MAGIC Checks that do NOT depend on trusting the AUTO CDC flow - each one is an independent calculation:
# MAGIC
# MAGIC | Check | Meaning |
# MAGIC |---|---|
# MAGIC | `scd1_vs_latest_feed` | carriers whose LATEST change is not a delete = rows in `dim_carrier_current` |
# MAGIC | `scd2_open_vs_scd1` | rows still open (`__END_AT` is NULL) in the history table = rows in the current table |
# MAGIC | `scd2_one_open_row_per_key` | no carrier has two "current" versions |
# MAGIC | `scd2_no_gaps_or_overlaps` | each version ends exactly when the next one starts |

# COMMAND ----------
# MAGIC %run ./ops_common

# COMMAND ----------

from pyspark.sql import functions as F

log_event("audit_cdc", "STARTED")
results = []   # (run_id, check_name, entity, expected, actual, status)


def add(check, expected, actual):
    results.append((RUN_ID, check, "dim_carrier", int(expected), int(actual), "PASS" if expected == actual else "FAIL"))


current = spark.table(f"{CATALOG}.gold.dim_carrier_current").count()

# expected number of carriers today = carriers whose newest change in the feed is not a delete
expected_current = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
        SELECT carrier_id, op,
               ROW_NUMBER() OVER (PARTITION BY carrier_id ORDER BY change_ts DESC) AS rn
        FROM {CATALOG}.silver.carriers_cdc)
    WHERE rn = 1 AND op <> 'D'
""").first()["n"]
add("scd1_vs_latest_feed", expected_current, current)

hist = f"{CATALOG}.gold.dim_carrier"
open_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {hist} WHERE `__END_AT` IS NULL").first()["n"]
add("scd2_open_vs_scd1", current, open_rows)

two_open = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
        SELECT carrier_id FROM {hist} WHERE `__END_AT` IS NULL GROUP BY carrier_id HAVING COUNT(*) > 1)
""").first()["n"]
add("scd2_one_open_row_per_key", 0, two_open)

broken_chain = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
        SELECT `__END_AT` AS end_at,
               LEAD(`__START_AT`) OVER (PARTITION BY carrier_id ORDER BY `__START_AT`) AS next_start
        FROM {hist})
    WHERE next_start IS NOT NULL AND (end_at IS NULL OR end_at <> next_start)
""").first()["n"]
add("scd2_no_gaps_or_overlaps", 0, broken_chain)

# COMMAND ----------

cols = "run_id string, check_name string, entity string, expected long, actual long, status string"
res_df = spark.createDataFrame(results, cols).withColumn("checked_at", F.lit(datetime.now(timezone.utc)))
res_df.write.mode("append").saveAsTable(f"{CATALOG}.ops.reconciliation_results")
display(res_df)

failed = [r for r in results if r[5] == "FAIL"]
if failed:
    log_event("audit_cdc", "FAILED", message=f"{len(failed)} check(s) failed")
    raise Exception(f"CDC audit FAILED: {[(r[1], r[3], r[4]) for r in failed]}")
log_event("audit_cdc", "SUCCESS", message=f"{len(results)} checks passed")
