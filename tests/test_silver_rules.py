"""
Runs the REAL silver typing + rules on a local Spark session (no Databricks needed).
Run with:  pytest tests/        (needs: pip install pyspark pytest)

* test_generated_data_has_no_rule_failures : the clean dataset must pass every rule
* test_bad_rows_are_flagged               : each broken row fails the rule we expect
"""
import os
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src" / "freight_etl"))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utilities.dq import add_dq_flags
from utilities.entities import ENTITIES, STRUCTURAL_RULES
from utilities.schemas import SOURCES

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = os.environ.get("FREIGHT_DATA_DIR", "")      # folder that contains the generated *.csv files


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def flag(spark, source, path):
    """Read a CSV as all strings (like bronze), then apply the silver select + rules."""
    # Same reader settings as bronze: malformed rows land in _corrupt_record
    raw = spark.read.option("header", "true").option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(", ".join(f"{c} string" for c in SOURCES[source]) + ", _corrupt_record string").csv(str(path))
    raw = raw.cache()    # Spark requires caching before querying a file that has a corrupt-record column
    # Open-source Spark has no rescued-data feature (Databricks Auto Loader adds it), so we
    # add an empty column here so the same rules can run.
    raw = raw.withColumn("_rescued_data", F.lit(None).cast("string"))
    cfg = ENTITIES[source]
    typed = raw.select(*[F.expr(e) for e in cfg["select"]], F.col("_rescued_data"), F.col("_corrupt_record"))
    return add_dq_flags(typed, {**STRUCTURAL_RULES, **cfg["rules"]})


@pytest.mark.skipif(not DATA_DIR, reason="set FREIGHT_DATA_DIR to the folder with the generated CSVs")
@pytest.mark.parametrize("source", list(SOURCES))
def test_generated_data_has_no_rule_failures(spark, source):
    path = pathlib.Path(DATA_DIR) / f"{source}.csv"
    if not path.exists():
        pytest.skip(f"{path.name} is not part of the generated dataset (carriers_cdc is tested in test_cdc.py)")
    df = flag(spark, source, path)
    bad = df.filter(F.size("_failed_rules") > 0)
    assert bad.count() == 0, bad.limit(3).collect()


def test_bad_rows_are_flagged(spark):
    df = flag(spark, "invoices", ROOT / "sample_inbox_bad" / "invoices_bad_rows.csv")
    got = {r["invoice_number"]: set(r["_failed_rules"]) for r in df.collect()}
    assert got["INV99000001"] == set()                                   # the good row
    assert got["INV99000002"] == {"total_not_negative"}
    assert got["INV99000003"] == {"invoice_date_valid"}
    assert got["INV99000004"] == {"weight_positive"}
    assert got[None] == {"invoice_number_present"}
    assert got["INV99000006"] == {"row_well_formed"}                     # two extra fields -> malformed row
