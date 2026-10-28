# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1 - Set up one environment (run once per target)
# MAGIC Creates: catalog, schemas, the landing volume, folders, and the `ops` tables.
# MAGIC It is safe to run again (everything is `IF NOT EXISTS`).

# COMMAND ----------

import sys
dbutils.widgets.text("catalog", "freight_dev")
dbutils.widgets.text("src_path", "")
catalog = dbutils.widgets.get("catalog")
src_path = dbutils.widgets.get("src_path")
if src_path:
    sys.path.append(src_path)
from utilities.schemas import SOURCES

# COMMAND ----------
# MAGIC %md ### 1. Catalog and schemas (one schema per layer)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

#   landing    : raw files (stored in a Volume)
#   bronze     : raw rows as received
#   silver     : clean, typed, validated
#   gold       : business-ready facts, dimensions, summaries
#   quarantine : rows that failed data-quality rules
#   ops        : our own logs and reconciliation results
for schema in ["landing", "bronze", "silver", "gold", "quarantine", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------
# MAGIC %md ### 2. Landing volume and folders
# MAGIC * `inbox/<source>`            : where new files are dropped
# MAGIC * `batch/<source>`            : files that PASSED validation (Auto Loader reads here)
# MAGIC * `quarantine_files/<source>` : files that FAILED validation

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.raw")
root = f"/Volumes/{catalog}/landing/raw"

for source in SOURCES:
    for area in ["inbox", "batch", "quarantine_files"]:
        dbutils.fs.mkdirs(f"{root}/{area}/{source}")

dbutils.fs.mkdirs(f"{root}/stream")           # used in Step 4 (Kafka-style files)
dbutils.fs.mkdirs(f"{root}/stream_staging")
dbutils.fs.mkdirs(f"{root}/truth")             # ground-truth labels (evaluation only, not part of the pipelines)
dbutils.fs.mkdirs(f"{root}/stream_manifest")   # producer manifests (what was really sent)

# COMMAND ----------
# MAGIC %md ### 3. Ops tables (our own audit trail)

# COMMAND ----------

# One line per task event of every job run
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.pipeline_run_log (
  run_id   STRING, task STRING, status STRING, event_ts TIMESTAMP,
  message  STRING, rows_in BIGINT, rows_out BIGINT
)""")

# One line per file we looked at before ingesting it
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.file_validation_log (
  run_id STRING, source STRING, file_name STRING, file_path STRING,
  size_bytes BIGINT, status STRING, reason STRING, event_ts TIMESTAMP
)""")

# One line per reconciliation check
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.reconciliation_results (
  run_id STRING, check_name STRING, entity STRING,
  expected BIGINT, actual BIGINT, status STRING, checked_at TIMESTAMP
)""")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
print("Environment ready:", catalog)
