"""
BRONZE (streaming): ONE multiplexed table for ALL Kafka topics.

"Multiplex" = every topic lands in the same table, one row per Kafka message, and the
`topic` column tells them apart. The message body (`value`) is kept as raw TEXT.

Why one table?
  * A new topic needs no new ingestion code - add it to the topic list and it flows.
  * The raw messages stay replayable: if a parsing rule was wrong, fix it and reprocess.
  * One stream, one checkpoint to operate.

Parsing and routing per topic happen in silver (silver_stream.py).
Bronze never filters or fixes anything.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import conf, table
from utilities.kafka_conn import kafka_options


@dp.table(
    name=table("bronze", "kafka_multiplex"),
    comment="Raw Kafka messages from all freight topics (multiplexed), payload untouched",
    table_properties={"quality": "bronze"},
)
def kafka_multiplex():
    scope = conf("freight.secret_scope")   # name of the secret scope (set in the bundle)
    topics = conf("freight.topics")        # comma-separated topic list (set in the bundle)

    return (
        spark.readStream.format("kafka")
        .options(**kafka_options(lambda key: dbutils.secrets.get(scope, key)))
        .option("subscribe", topics)               # several topics in ONE stream
        .option("startingOffsets", "earliest")     # first run: read from the beginning
        .option("includeHeaders", "true")
        .load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),            # raw JSON text, NOT parsed here
            F.col("timestamp").alias("kafka_timestamp"),             # time the broker stored it
            # our producer sends a schema_version header (1 or 2)
            F.expr("cast(filter(headers, h -> h.key = 'schema_version')[0].value AS STRING)")
             .alias("schema_version"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
