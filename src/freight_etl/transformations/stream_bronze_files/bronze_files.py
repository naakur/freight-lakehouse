"""
BRONZE (streaming), FILE VERSION: the same multiplexed table as the Kafka version, fed from files.

Each line of a file in  stream/  is one Kafka-style message:
    {"topic": ..., "partition": ..., "offset": ..., "key": ..., "value": "<json text>", "timestamp": ..., "headers": {...}}

The output table has EXACTLY the same name and columns as bronze_kafka.py produces, so silver,
gold and the audits cannot tell the difference. Choose the source with the bundle variable `stream_source`
(files | kafka). Use this version to build and test everything without a Kafka cluster.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import stream_path, table
from utilities.topics import ENVELOPE_SCHEMA


@dp.table(
    name=table("bronze", "kafka_multiplex"),
    comment="Raw Kafka-style messages from all freight topics (multiplexed), payload untouched [file source]",
    table_properties={"quality": "bronze"},
)
def kafka_multiplex():
    return (
        spark.readStream.format("cloudFiles")            # Auto Loader remembers which files it has read
        .option("cloudFiles.format", "json")
        .schema(ENVELOPE_SCHEMA)
        .load(stream_path())
        .select(
            "topic", "partition", "offset", "key",
            "value",                                      # raw JSON text, NOT parsed here
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("headers.schema_version").alias("schema_version"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
