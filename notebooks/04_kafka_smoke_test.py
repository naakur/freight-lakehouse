# Databricks notebook source
# MAGIC %md
# MAGIC ## Kafka smoke test (run by hand, BEFORE the pipeline)
# MAGIC Answers three questions in one minute:
# MAGIC 1. Can Databricks read my secrets?  2. Can it reach Confluent?  3. Is there data in the topics?
# MAGIC
# MAGIC Run it after `scripts/setup_secrets.ps1` and after the producer has sent a few messages.

# COMMAND ----------

import sys
dbutils.widgets.text("secret_scope", "freight-kafka")
dbutils.widgets.text("src_path", "")
scope = dbutils.widgets.get("secret_scope")
if dbutils.widgets.get("src_path"):
    sys.path.append(dbutils.widgets.get("src_path"))

from utilities.kafka_conn import kafka_options
from utilities.topics import TOPICS

# COMMAND ----------
# MAGIC %md ### 1. Secrets: only checks that they exist (values are never printed)

# COMMAND ----------

for key in ["kafka-bootstrap", "kafka-api-key", "kafka-api-secret"]:
    dbutils.secrets.get(scope, key)        # raises an error if the secret is missing
    print("found secret:", key)

# COMMAND ----------
# MAGIC %md ### 2. Read every topic once (batch read) and count the messages

# COMMAND ----------

options = kafka_options(lambda key: dbutils.secrets.get(scope, key))

df = (spark.read.format("kafka")
      .options(**options)
      .option("subscribe", ",".join(TOPICS))
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())

display(df.groupBy("topic").count().orderBy("topic"))

# COMMAND ----------
# MAGIC %md ### 3. Peek at a few messages

# COMMAND ----------

display(df.selectExpr("topic", "partition", "offset", "cast(key as string) as key",
                      "cast(value as string) as value", "timestamp").limit(5))
