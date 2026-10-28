# Databricks notebook source
# MAGIC %md
# MAGIC ## Audit and reconcile (runs AFTER the pipeline)
# MAGIC
# MAGIC "Did every row we received end up somewhere we can explain?"
# MAGIC
# MAGIC | Check | Meaning |
# MAGIC |---|---|
# MAGIC | `landed_vs_bronze` | rows in the validated CSV files = rows in bronze |
# MAGIC | `bronze_vs_silver_plus_quarantine` | bronze = silver + quarantine (nothing silently lost) |
# MAGIC | `quarantine_rate` | WARN if more than 5% of rows were quarantined |
# MAGIC
# MAGIC Any FAIL stops the job (so it alerts); results are saved in `ops.reconciliation_results`.

# COMMAND ----------
# MAGIC %run ./ops_common

# COMMAND ----------

from pyspark.sql import functions as F

log_event("audit_and_reconcile", "STARTED")
MAX_QUARANTINE_PCT = 5.0
results = []   # (run_id, check_name, entity, expected, actual, status)


def count_table(name):
    """Row count of a table, or None if the table does not exist (pipeline problem)."""
    try:
        return spark.table(f"{CATALOG}.{name}").count()
    except Exception:
        return None


def count_landed_rows(source):
    """Rows in all validated CSV files of one source."""
    try:
        return (spark.read.format("csv").option("header", "true")
                .load(f"{LANDING_ROOT}/batch/{source}/").count())
    except Exception:
        return 0     # empty folder = zero rows landed


for source in SOURCES:
    landed = count_landed_rows(source)
    bronze = count_table(f"bronze.{source}")
    silver = count_table(f"silver.{source}")
    quarantine = count_table(f"quarantine.{source}")

    # Check 1: landed files vs bronze
    status = "PASS" if bronze == landed else "FAIL"
    results.append((RUN_ID, "landed_vs_bronze", source, landed, bronze, status))

    # Check 2: bronze vs silver + quarantine
    if None in (bronze, silver, quarantine):
        results.append((RUN_ID, "bronze_vs_silver_plus_quarantine", source, bronze, None, "FAIL"))
    else:
        status = "PASS" if bronze == silver + quarantine else "FAIL"
        results.append((RUN_ID, "bronze_vs_silver_plus_quarantine", source, bronze, silver + quarantine, status))

        # Check 3: how much was quarantined? (only a warning)
        pct = (quarantine / bronze * 100) if bronze else 0.0
        status = "WARN" if pct > MAX_QUARANTINE_PCT else "PASS"
        results.append((RUN_ID, "quarantine_rate", source, int(MAX_QUARANTINE_PCT), int(round(pct)), status))

# COMMAND ----------

cols = "run_id string, check_name string, entity string, expected long, actual long, status string"
res_df = spark.createDataFrame(results, cols).withColumn("checked_at", F.lit(datetime.now(timezone.utc)))
res_df.write.mode("append").saveAsTable(f"{CATALOG}.ops.reconciliation_results")
display(res_df)

# COMMAND ----------

failed = [r for r in results if r[5] == "FAIL"]
warned = [r for r in results if r[5] == "WARN"]

if failed:
    log_event("audit_and_reconcile", "FAILED", message=f"{len(failed)} reconciliation check(s) failed")
    # Raising an error makes the job task (and the job) fail, which triggers alerts.
    raise Exception(f"Reconciliation FAILED: {[(r[1], r[2]) for r in failed]}")

log_event("audit_and_reconcile", "WARN" if warned else "SUCCESS",
          message=f"{len(results)} checks, {len(warned)} warning(s)")
