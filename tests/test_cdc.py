"""
Tests for the carrier change feed (CDC / SCD2).

1. The silver rules flag exactly the 2 broken rows in carriers_cdc_0003.csv   (local Spark)
2. A pure-Python reference ("oracle") of SCD2 applied to the feed gives the numbers the README promises.
   After you run the pipeline, gold.dim_carrier must match this oracle. (AUTO CDC itself only runs on Databricks.)
"""
import csv
import pathlib
import sys
from datetime import datetime

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src" / "freight_etl"))

from utilities.schemas import SOURCES

CDC_DIR = pathlib.Path(__file__).resolve().parents[1] / "sample_cdc"
FIELDS = SOURCES["carriers_cdc"]


def read_feed(files):
    rows = []
    for name in files:
        with open(CDC_DIR / name, newline="") as f:
            rows += list(csv.DictReader(f))
    return rows


def scd2_oracle(rows):
    """
    Reference SCD2: take all VALID changes, sort by change_ts per carrier, and build versions.
    Result: {carrier_id: [(start, end, name, terms), ...]}   end=None means still current.
    """
    valid = [r for r in rows if r["op"] in ("I", "U", "D") and r["change_ts"]]
    valid.sort(key=lambda r: (r["carrier_id"], r["change_ts"]))
    out = {}
    for r in valid:
        ts = datetime.fromisoformat(r["change_ts"])
        versions = out.setdefault(r["carrier_id"], [])
        if r["op"] == "D":
            if versions and versions[-1][1] is None:
                versions[-1][1] = ts                           # close the open version
        else:
            if versions and versions[-1][1] is None:
                versions[-1][1] = ts                           # close the previous version
            versions.append([ts, None, r["carrier_name"], r["payment_terms"]])
    return out


def counts(oracle):
    total = sum(len(v) for v in oracle.values())
    open_rows = sum(1 for v in oracle.values() for x in v if x[1] is None)
    return total, open_rows


def test_oracle_after_wave_a():
    total, open_rows = counts(scd2_oracle(read_feed(["carriers_cdc_0001.csv", "carriers_cdc_0002.csv"])))
    assert (total, open_rows) == (12, 8)       # 12 versions, 8 carriers still active (C008 closed)


def test_oracle_after_wave_b_rewrites_history():
    oracle = scd2_oracle(read_feed(["carriers_cdc_0001.csv", "carriers_cdc_0002.csv", "carriers_cdc_0003.csv"]))
    assert counts(oracle) == (14, 8)
    # C001 has 4 versions in time order: the LATE NET40 record slots in between NET30 and NET45
    assert [v[3] for v in oracle["C001"]] == ["NET30", "NET40", "NET45", "NET60"]
    # no gaps and no overlaps
    for versions in oracle.values():
        for a, b in zip(versions, versions[1:]):
            assert a[1] == b[0]
    # C008 was deleted: its only version is closed
    assert oracle["C008"][0][1] == datetime(2026, 7, 1)


def test_silver_rules_flag_the_two_broken_rows():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from utilities.dq import add_dq_flags
    from utilities.entities import ENTITIES, STRUCTURAL_RULES

    spark = SparkSession.builder.master("local[1]").appName("t").config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1").getOrCreate()
    try:
        raw = spark.read.option("header", "true").option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(", ".join(f"{c} string" for c in FIELDS) + ", _corrupt_record string") \
            .csv(str(CDC_DIR / "carriers_cdc_000*.csv")).cache()
        raw = raw.withColumn("_rescued_data", F.lit(None).cast("string"))
        cfg = ENTITIES["carriers_cdc"]
        typed = raw.select(*[F.expr(e) for e in cfg["select"]], F.col("_rescued_data"), F.col("_corrupt_record"))
        flagged = add_dq_flags(typed, {**STRUCTURAL_RULES, **cfg["rules"]})
        bad = {(r["carrier_id"], tuple(r["_failed_rules"])) for r in flagged.filter(F.size("_failed_rules") > 0).collect()}
        assert bad == {
            ("C006", ("op_is_I_U_or_D",)),
            ("C007", ("change_ts_valid",)),
        }
        assert flagged.filter(F.size("_failed_rules") == 0).count() == 15      # 17 rows - 2 broken
    finally:
        spark.stop()
