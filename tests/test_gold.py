"""
Tests the GOLD business logic on the generated data with local Spark.

The most important test: run the audit rules and compare them with the GROUND TRUTH file, the list of errors
the generator injected. Recall = how many real errors we caught. Precision = how many alerts were real errors.

Run with:  FREIGHT_DATA_DIR=<data/batch> FREIGHT_STREAM_DIR=<data/stream> pytest tests/test_gold.py
(the truth file is expected in <data>/truth/invoice_anomaly_labels.csv, next to the batch folder)
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
from utilities import gold_logic as g
from utilities.entities import ENTITIES
from utilities.schemas import SOURCES
from utilities.topics import TOPICS

DATA_DIR = os.environ.get("FREIGHT_DATA_DIR", "")
STREAM_DIR = os.environ.get("FREIGHT_STREAM_DIR", "")
pytestmark = pytest.mark.skipif(not (DATA_DIR and STREAM_DIR), reason="set FREIGHT_DATA_DIR and FREIGHT_STREAM_DIR")

TRUTH = pathlib.Path(DATA_DIR).parent / "truth" / "invoice_anomaly_labels.csv" if DATA_DIR else None
RULES_WITH_TRUTH = ["OVERCHARGE_RATE", "FUEL_VARIANCE", "WEIGHT_MISMATCH", "UNAUTHORIZED_ACCESSORIAL", "DUPLICATE_INVOICE"]


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("gold").config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def silver(spark, source):
    """Read a generated CSV and type it exactly like the silver layer does."""
    raw = spark.read.option("header", "true").schema(", ".join(f"{c} string" for c in SOURCES[source])) \
        .csv(os.path.join(DATA_DIR, f"{source}.csv"))
    return raw.select(*[F.expr(e) for e in ENTITIES[source]["select"]])


def silver_topic(spark, topic):
    """Parse one topic's messages like silver_stream does: drop malformed, keep the first copy of each id."""
    cfg = TOPICS[topic]
    env = spark.read.json(os.path.join(STREAM_DIR, "kafka_batch_*.json")).where(F.col("topic") == topic)
    p = env.select(F.from_json("value", cfg["schema"] + ", _corrupt_json STRING",
                               {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_json"}).alias("p")).select("p.*")
    return p.where("_corrupt_json IS NULL").drop("_corrupt_json").dropDuplicates([cfg["id_col"]])


@pytest.fixture(scope="module")
def tables(spark):
    t = {s: silver(spark, s).cache() for s in ["shipments", "invoices", "rate_cards", "accessorial_codes", "carriers"]}
    t["findings"] = g.build_findings(t["invoices"], t["shipments"], t["rate_cards"], t["accessorial_codes"]).cache()
    return t


def test_audit_rules_vs_ground_truth(spark, tables):
    truth = spark.read.option("header", "true").csv(str(TRUTH)).select("invoice_number", F.col("anomaly_type").alias("rule_id"))
    found = tables["findings"].where(F.col("rule_id").isin(RULES_WITH_TRUTH)).select("invoice_number", "rule_id")
    print()
    for rule in RULES_WITH_TRUTH:
        t = truth.where(F.col("rule_id") == rule)
        f = found.where(F.col("rule_id") == rule)
        hit = t.join(f, ["invoice_number", "rule_id"]).count()
        recall = hit / t.count()
        precision = hit / f.count()
        print(f"{rule:26s} truth={t.count():4d} found={f.count():4d} recall={recall:.3f} precision={precision:.3f}")
        assert recall >= 0.99, rule
        assert precision >= 0.99, rule
    # the "cannot audit" rules must stay quiet on clean reference data
    assert tables["findings"].where("severity = 'REVIEW'").count() == 0


def test_recoverable_amounts_are_never_negative(tables):
    assert tables["findings"].where("recoverable_amount < 0").count() == 0


def test_fact_invoice_keeps_one_row_per_invoice_and_all_the_money(spark, tables):
    from datetime import datetime
    # carrier history as it stands after wave B (built from the same feed in plain Python)
    hist_rows = [("C001", "Atlas Freight Lines", "NET30", datetime(2026, 1, 1), datetime(2026, 2, 15)),
                 ("C001", "Atlas Freight Lines", "NET40", datetime(2026, 2, 15), datetime(2026, 4, 1)),
                 ("C001", "Atlas Freight Lines", "NET45", datetime(2026, 4, 1), datetime(2026, 8, 1)),
                 ("C001", "Atlas Freight Lines", "NET60", datetime(2026, 8, 1), None)] + \
                [(c, n, "NET30", datetime(2026, 1, 1), None) for c, n in
                 [("C002", "BlueRiver Logistics"), ("C003", "Cardinal Express"), ("C004", "Delta Haul Co"),
                  ("C005", "Evergreen Transport"), ("C006", "Frontier Cargo"), ("C007", "Granite State Carriers")]] + \
                [("C008", "Harbor Line Freight", "NET30", datetime(2026, 1, 1), datetime(2026, 7, 1))]
    hist = spark.createDataFrame(hist_rows, "carrier_id string, carrier_name string, payment_terms string, __START_AT timestamp, __END_AT timestamp")
    fact = g.build_fact_invoice(tables["invoices"], tables["shipments"], hist).cache()

    inv = tables["invoices"]
    assert fact.count() == inv.count()                                           # no row lost or duplicated
    assert fact.select("invoice_number").distinct().count() == inv.count()
    assert fact.agg(F.sum("total_amount")).first()[0] == inv.agg(F.sum("total_amount")).first()[0]
    assert fact.where("payment_terms_at_invoice IS NULL").count() == 0           # every invoice found a carrier version

    # SCD2 lookup: a C001 invoice dated in March uses NET40, one dated in September uses NET60
    mar = fact.where("carrier_id = 'C001' AND invoice_date >= DATE'2026-03-01' AND invoice_date < DATE'2026-04-01'").first()
    sep = fact.where("carrier_id = 'C001' AND invoice_date >= DATE'2026-09-01'").first()
    assert mar["payment_terms_at_invoice"] == "NET40" and sep["payment_terms_at_invoice"] == "NET60"
    assert (mar["due_date"] - mar["invoice_date"]).days == 40


def test_shipment_delivery_matches_plain_python(spark, tables):
    tracking = silver_topic(spark, "freight.tracking.events")
    delivery = g.build_shipment_delivery(tables["shipments"], tracking).cache()
    assert delivery.count() == tables["shipments"].count()

    # independent Python calculation of how many shipments were delivered late
    promised = {r["shipment_id"]: r["promised_delivery_date"] for r in tables["shipments"].collect()}
    delivered = {}
    for f in sorted(glob.glob(os.path.join(STREAM_DIR, "kafka_batch_*.json"))):
        for line in open(f):
            m = json.loads(line)
            if m["topic"] != "freight.tracking.events":
                continue
            try:
                v = json.loads(m["value"])
            except Exception:
                continue
            if v["status"] == "DELIVERED":
                delivered[v["shipment_id"]] = v["event_ts"][:10]
    late = sum(1 for s, d in delivered.items() if d > promised[s].isoformat())

    assert delivery.where("is_delivered").count() == len(delivered)
    assert delivery.where("is_late").count() == late


def test_summaries_build_and_tie_out(spark, tables):
    tracking = silver_topic(spark, "freight.tracking.events")
    delivery = g.build_shipment_delivery(tables["shipments"], tracking)
    claims = g.build_fact_claim(silver_topic(spark, "freight.claims.filed"))
    from datetime import datetime
    hist = spark.createDataFrame([(c, f"name {c}", "NET30", datetime(2026, 1, 1), datetime(2026, 7, 1) if c == "C008" else None)
                                  for c in [f"C00{i}" for i in range(1, 9)]],
                                 "carrier_id string, carrier_name string, payment_terms string, __START_AT timestamp, __END_AT timestamp")
    fact = g.build_fact_invoice(tables["invoices"], tables["shipments"], hist)

    score = g.build_carrier_scorecard(fact, tables["findings"], delivery, claims, hist)
    assert score.count() == 8
    # a carrier that was DELETED in the change feed (C008) keeps its name and is flagged inactive
    assert score.where("carrier_name IS NULL").count() == 0
    assert score.where("carrier_id = 'C008'").first()["is_active"] is False
    # totals in the scorecard equal totals in the facts (nothing lost in aggregation)
    assert score.agg(F.sum("invoices")).first()[0] == fact.count()
    assert score.agg(F.sum("billed_total")).first()[0] == fact.agg(F.sum("total_amount")).first()[0]
    by_month = g.build_recoverable_by_month(tables["findings"])
    assert by_month.agg(F.sum("recoverable_amount")).first()[0] == \
        tables["findings"].where("severity <> 'REVIEW'").agg(F.sum("recoverable_amount")).first()[0]
    lanes = g.build_late_delivery_by_lane(delivery)
    assert lanes.agg(F.sum("delivered_shipments")).first()[0] == delivery.where("is_delivered").count()
