# Databricks notebook source
# MAGIC %md
# MAGIC ### Shared helpers for the ops notebooks
# MAGIC Every ops notebook starts with `%run ./ops_common`. It gives you:
# MAGIC * `CATALOG`, `RUN_ID`, `LANDING_ROOT`, `SOURCES`
# MAGIC * `log_event(...)` to write a line to `ops.pipeline_run_log`

# COMMAND ----------

import sys
from datetime import datetime, timezone

# Parameters arrive from the job (see resources/jobs.yml -> base_parameters)
dbutils.widgets.text("catalog", "freight_dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("src_path", "")

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = dbutils.widgets.get("run_id")          # the job run id, so every log line can be traced
SRC_PATH = dbutils.widgets.get("src_path")      # where the bundle put src/freight_etl

# Re-use the SAME source/column definitions as the pipelines (single source of truth)
if SRC_PATH and SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
from utilities.schemas import SOURCES

LANDING_ROOT = f"/Volumes/{CATALOG}/landing/raw"

# COMMAND ----------

def log_event(task, status, message="", rows_in=None, rows_out=None):
    """
    Append ONE line to ops.pipeline_run_log.
      status: STARTED | SUCCESS | WARN | FAILED
    """
    row = [(RUN_ID, task, status, datetime.now(timezone.utc), message, rows_in, rows_out)]
    cols = "run_id string, task string, status string, event_ts timestamp, message string, rows_in long, rows_out long"
    spark.createDataFrame(row, cols).write.mode("append").saveAsTable(f"{CATALOG}.ops.pipeline_run_log")
    print(f"[{status}] {task}: {message}")
