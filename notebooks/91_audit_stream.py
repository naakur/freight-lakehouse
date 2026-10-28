# Databricks notebook source
# MAGIC %md
# MAGIC ## Audit and reconcile the Kafka flow (runs AFTER the stream pipeline)
# MAGIC
# MAGIC | Check | Meaning |
# MAGIC |---|---|
# MAGIC | `sent_vs_bronze` | messages that should have arrived (file lines, or producer manifest) = messages in bronze |
# MAGIC | `silver_vs_distinct_valid_ids` | silver = number of different business ids among the messages that were NOT quarantined (so duplicates were removed, nothing else lost) |
# MAGIC | `quarantine_rate` | WARN if more than 5% of a topic was quarantined |
# MAGIC | `unrouted_messages` | WARN if a topic arrived that has no definition |
# MAGIC
# MAGIC Any FAIL stops the job. Results go to `ops.reconciliation_results`.

# COMMAND ----------
# MAGIC %run ./ops_common

# COMMAND ----------

import json
from pyspark.sql import functions as F
from utilities.topics import TOPICS

log_event("audit_stream", "STARTED")
MAX_QUARANTINE_PCT = 5.0
results = []   # (run_id, check_name, entity, expected, actual, status)

# COMMAND ----------
# MAGIC %md ### How many messages SHOULD have arrived?
# MAGIC * source = `files`: count the lines in the stream files
# MAGIC * source = `kafka`: sum the producer manifests (what the producer says it really delivered)

# COMMAND ----------

dbutils.widgets.text("stream_source", "files")
STREAM_SOURCE = dbutils.widgets.get("stream_source")

sent = {}
try:
    if STREAM_SOURCE == "files":
        rows = spark.read.json(f"{LANDING_ROOT}/stream/").groupBy("topic").count().collect()
        sent = {r["topic"]: r["count"] for r in rows}
    else:
        for f in dbutils.fs.ls(f"{LANDING_ROOT}/stream_manifest/"):
            if not f.name.endswith(".json"):
                continue
            manifest = json.loads(dbutils.fs.head(f.path, 65536))
            if manifest.get("dry_run"):
                continue
            for topic, n in manifest["sent"].items():
                sent[topic] = sent.get(topic, 0) + n
except Exception as e:
    print("could not determine expected counts:", e)
print(f"expected per topic ({STREAM_SOURCE}):", sent)

# COMMAND ----------

bronze_tbl = f"{CATALOG}.bronze.kafka_multiplex"

for topic, cfg in TOPICS.items():
    entity, id_col = cfg["entity"], cfg["id_col"]
    bronze_n = spark.table(bronze_tbl).where(F.col("topic") == topic).count()
    silver_n = spark.table(f"{CATALOG}.silver.{entity}").count()
    quarantine_n = spark.table(f"{CATALOG}.quarantine.{entity}").count()

    # Check 1: expected arrivals vs bronze
    #   files source : a topic that is absent from the files means 0 messages are expected
    #   kafka source : the expected count comes from the producer manifests (none uploaded -> WARN)
    expected = sent.get(topic, 0) if STREAM_SOURCE == "files" else sent.get(topic)
    if expected is not None:
        status = "PASS" if expected == bronze_n else "FAIL"
        results.append((RUN_ID, "sent_vs_bronze", entity, expected, bronze_n, status))
    else:
        results.append((RUN_ID, "sent_vs_bronze", entity, None, bronze_n, "WARN"))

    # Check 2: silver = distinct ids among messages that are not in quarantine
    distinct_valid = spark.sql(f"""
        SELECT COUNT(DISTINCT get_json_object(b.value, '$.{id_col}')) AS n
        FROM {bronze_tbl} b
        LEFT ANTI JOIN {CATALOG}.quarantine.{entity} q
          ON b.topic = '{topic}' AND b.partition = q.partition AND b.offset = q.offset
        WHERE b.topic = '{topic}'
    """).first()["n"]
    status = "PASS" if distinct_valid == silver_n else "FAIL"
    results.append((RUN_ID, "silver_vs_distinct_valid_ids", entity, distinct_valid, silver_n, status))

    # Check 3: how much was quarantined? (warning only)
    pct = (quarantine_n / bronze_n * 100) if bronze_n else 0.0
    status = "WARN" if pct > MAX_QUARANTINE_PCT else "PASS"
    results.append((RUN_ID, "quarantine_rate", entity, int(MAX_QUARANTINE_PCT), int(round(pct)), status))

# Check 4: topics we do not know about
unrouted = spark.table(f"{CATALOG}.quarantine.kafka_unrouted").count()
results.append((RUN_ID, "unrouted_messages", "kafka_unrouted", 0, unrouted, "PASS" if unrouted == 0 else "WARN"))

# COMMAND ----------

cols = "run_id string, check_name string, entity string, expected long, actual long, status string"
res_df = spark.createDataFrame(results, cols).withColumn("checked_at", F.lit(datetime.now(timezone.utc)))
res_df.write.mode("append").saveAsTable(f"{CATALOG}.ops.reconciliation_results")
display(res_df)

# COMMAND ----------

failed = [r for r in results if r[5] == "FAIL"]
warned = [r for r in results if r[5] == "WARN"]

if failed:
    log_event("audit_stream", "FAILED", message=f"{len(failed)} check(s) failed")
    raise Exception(f"Stream reconciliation FAILED: {[(r[1], r[2]) for r in failed]}")

log_event("audit_stream", "WARN" if warned else "SUCCESS", message=f"{len(results)} checks, {len(warned)} warning(s)")
