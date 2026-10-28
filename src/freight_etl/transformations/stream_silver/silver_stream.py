"""
SILVER (streaming): route the multiplexed bronze table by topic.

For every topic we create:
    silver.<entity>      parsed, validated, DEDUPLICATED messages
    quarantine.<entity>  messages that could not be parsed or broke a rule (original text kept)
and one catch-all:
    quarantine.kafka_unrouted   messages from topics nobody has a definition for yet

Why dedupe? Kafka delivers "at least once", so the same event can arrive twice.
We drop repeats of the same business id that arrive within a 2-day window
(longer than our latest late event, ~40h). State older than that is released.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import table
from utilities.dq import add_dq_flags, invalid_rows, valid_rows
from utilities.topics import STRUCTURAL_RULES, TOPICS

BRONZE = table("bronze", "kafka_multiplex")
KAFKA_COLS = ["topic", "partition", "offset", "kafka_timestamp", "schema_version"]


def register_topic(topic: str, cfg: dict):
    entity, id_col, time_col = cfg["entity"], cfg["id_col"], cfg["time_col"]
    schema = cfg["schema"] + ", _corrupt_json STRING"

    def flagged():
        """Read this topic from bronze, parse the JSON, and tag each message with the rules it failed."""
        raw = spark.readStream.table(BRONZE).where(F.col("topic") == topic)
        parsed = raw.select(
            F.from_json(
                "value", schema,
                # PERMISSIVE keeps unparseable messages and stores their text in _corrupt_json
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_json"},
            ).alias("p"),
            "value", *KAFKA_COLS,
        ).select("p.*", "value", *KAFKA_COLS)
        return add_dq_flags(parsed, {**STRUCTURAL_RULES, **cfg["rules"]})

    # ---- good messages -> silver (deduplicated)
    @dp.table(
        name=table("silver", entity),
        comment=f"Validated, deduplicated messages from {topic}",
        table_properties={"quality": "silver"},
    )
    def silver():
        return (
            valid_rows(flagged())
            .drop("_corrupt_json", "value")
            .withWatermark(time_col, "2 days")                  # how long we remember ids
            .dropDuplicatesWithinWatermark([id_col])            # keep the first copy of each id
            .withColumn("_silver_at", F.current_timestamp())
        )

    # ---- bad messages -> quarantine (keeps the original text + where it came from, so it can be replayed)
    @dp.table(
        name=table("quarantine", entity),
        comment=f"Messages from {topic} that failed parsing or a rule",
        table_properties={"quality": "quarantine"},
    )
    def quarantine():
        return invalid_rows(flagged()).select(
            F.coalesce("_corrupt_json", "value").alias("_raw_record"),
            "_failed_rules",
            "topic", "partition", "offset", "kafka_timestamp",
            F.current_timestamp().alias("_quarantined_at"),
        )


for _topic, _cfg in TOPICS.items():
    register_topic(_topic, _cfg)


# ---- catch-all: a topic that is in the feed but has no definition above
@dp.table(
    name=table("quarantine", "kafka_unrouted"),
    comment="Messages from topics without a silver definition",
    table_properties={"quality": "quarantine"},
)
def kafka_unrouted():
    return (
        spark.readStream.table(BRONZE)
        .where(~F.col("topic").isin(list(TOPICS)))
        .select("topic", "partition", "offset", "kafka_timestamp",
                F.col("value").alias("_raw_record"),
                F.lit("UNROUTED_TOPIC").alias("reason"),
                F.current_timestamp().alias("_quarantined_at"))
    )
