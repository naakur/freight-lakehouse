"""
Tests the topic parsing + rules on the generated Kafka-style files (local Spark, no Kafka needed).
Run with:  FREIGHT_STREAM_DIR=<folder with kafka_batch_*.json>  pytest tests/

Checks per topic:
  * every message that is NOT valid JSON is caught (well_formed_json) - and nothing else is flagged
  * after removing duplicates, the number of distinct ids equals what the generator produced
"""
import glob
import json
import os
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src" / "freight_etl"))

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utilities.dq import add_dq_flags
from utilities.topics import ENVELOPE_SCHEMA, STRUCTURAL_RULES, TOPICS

STREAM_DIR = os.environ.get("FREIGHT_STREAM_DIR", "")
pytestmark = pytest.mark.skipif(not STREAM_DIR, reason="set FREIGHT_STREAM_DIR")


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def python_truth(topic, id_col):
    """Count malformed messages and distinct ids with plain Python (independent of Spark)."""
    malformed, ids = 0, set()
    for f in sorted(glob.glob(os.path.join(STREAM_DIR, "kafka_batch_*.json"))):
        for line in open(f):
            m = json.loads(line)
            if m["topic"] != topic:
                continue
            try:
                ids.add(json.loads(m["value"])[id_col])
            except Exception:
                malformed += 1
    return malformed, len(ids)


@pytest.mark.parametrize("topic", list(TOPICS))
def test_topic_parsing_and_dedupe(spark, topic):
    cfg = TOPICS[topic]
    env = spark.read.json(os.path.join(STREAM_DIR, "kafka_batch_*.json")).where(F.col("topic") == topic)

    schema = cfg["schema"] + ", _corrupt_json STRING"
    parsed = env.select(
        F.from_json("value", schema, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_json"}).alias("p"),
        "value",
    ).select("p.*", "value")
    flagged = add_dq_flags(parsed, {**STRUCTURAL_RULES, **cfg["rules"]})

    bad = flagged.filter(F.size("_failed_rules") > 0)
    good = flagged.filter(F.size("_failed_rules") == 0)
    truth_malformed, truth_distinct = python_truth(topic, cfg["id_col"])

    # every malformed message is caught...
    assert bad.count() == truth_malformed
    # ...and the ONLY reason is the structural one (no false alarms from the business rules)
    other = bad.filter(~F.array_contains("_failed_rules", "well_formed_json")).count()
    assert other == 0
    # valid messages, with repeats removed, match the generator's distinct ids
    assert good.select(cfg["id_col"]).distinct().count() == truth_distinct


def test_file_envelope_reads_like_kafka(spark):
    """The file-source bronze must expose the same columns Kafka would, with typed times and header."""
    df = spark.read.schema(ENVELOPE_SCHEMA).json(os.path.join(STREAM_DIR, "kafka_batch_*.json")).select(
        "topic", "partition", "offset", "key", "value",
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("headers.schema_version").alias("schema_version"),
    )
    assert df.count() == sum(1 for f in glob.glob(os.path.join(STREAM_DIR, "kafka_batch_*.json")) for _ in open(f))
    assert df.filter(F.col("kafka_timestamp").isNull()).count() == 0
    assert {r[0] for r in df.select("schema_version").distinct().collect()} == {"1", "2"}
    # (topic, partition, offset) is the natural key of a Kafka message: it must be unique
    assert df.select("topic", "partition", "offset").distinct().count() == df.count()
