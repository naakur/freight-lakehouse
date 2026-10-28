"""
GOLD: the business-ready layer. Everything here is a MATERIALIZED VIEW (Spark recomputes it from silver;
you never maintain it by hand). All the logic lives in utilities/gold_logic.py so it can be unit tested.

DIMENSIONS (descriptions)                 FACTS (measurements/events)            FLAT SUMMARIES (for dashboards)
  dim_carrier (SCD2)   <- batch pipeline     fact_invoice                           carrier_scorecard
  dim_carrier_current  <- batch pipeline     fact_shipment_delivery                 recoverable_by_month
  dim_rate_card                              fact_audit_finding                     late_delivery_by_lane
  dim_gl_code                                fact_claim                             invoice_status_funnel
  dim_accessorial
  dim_date
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import gold_logic as g
from utilities.config import table


def silver(name):
    return spark.read.table(table("silver", name))


def gold(name):
    return spark.read.table(table("gold", name))


# ------------------------------------------------------------------ dimensions
@dp.materialized_view(name=table("gold", "dim_gl_code"), comment="GL codes for cost allocation")
def dim_gl_code():
    return silver("gl_codes").select("gl_code", "gl_description")


@dp.materialized_view(name=table("gold", "dim_accessorial"), comment="Accessorial charge codes with their flat amounts")
def dim_accessorial():
    return silver("accessorial_codes").select("accessorial_code", "description", "flat_amount")


@dp.materialized_view(name=table("gold", "dim_rate_card"),
                      comment="Contract rates, effective-dated (one row per lane, service and validity period)")
def dim_rate_card():
    return silver("rate_cards").drop("_source_file", "_ingested_at", "_silver_at")


@dp.materialized_view(name=table("gold", "dim_date"), comment="Calendar for 2026")
def dim_date():
    days = spark.sql("SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS date_key")
    return (days
            .withColumn("year", F.year("date_key")).withColumn("quarter", F.quarter("date_key"))
            .withColumn("month", F.month("date_key")).withColumn("month_name", F.date_format("date_key", "MMMM"))
            .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
            .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7)))


# ------------------------------------------------------------------ facts
@dp.materialized_view(name=table("gold", "fact_invoice"),
                      comment="One row per invoice; carrier attributes as they were on the invoice date (SCD2 lookup)")
def fact_invoice():
    return g.build_fact_invoice(silver("invoices"), silver("shipments"), gold("dim_carrier"))


@dp.materialized_view(name=table("gold", "fact_shipment_delivery"),
                      comment="One row per shipment: accumulating snapshot of pickup, delivery and lateness")
def fact_shipment_delivery():
    return g.build_shipment_delivery(silver("shipments"), silver("tracking_events"))


@dp.materialized_view(name=table("gold", "fact_audit_finding"),
                      comment="One row per invoice x audit rule that fired, with the amount to recover")
def fact_audit_finding():
    return g.build_findings(silver("invoices"), silver("shipments"), silver("rate_cards"), silver("accessorial_codes"))


@dp.materialized_view(name=table("gold", "fact_claim"), comment="One row per carrier claim")
def fact_claim():
    return g.build_fact_claim(silver("claims"))


# ------------------------------------------------------------------ flat summary tables
@dp.materialized_view(name=table("gold", "carrier_scorecard"),
                      comment="One row per carrier: spend, invoice errors, recoverable dollars, late deliveries, claims")
def carrier_scorecard():
    return g.build_carrier_scorecard(gold("fact_invoice"), gold("fact_audit_finding"),
                                     gold("fact_shipment_delivery"), gold("fact_claim"), gold("dim_carrier"))


@dp.materialized_view(name=table("gold", "recoverable_by_month"),
                      comment="Recoverable dollars by invoice month, carrier and audit rule")
def recoverable_by_month():
    return g.build_recoverable_by_month(gold("fact_audit_finding"))


@dp.materialized_view(name=table("gold", "late_delivery_by_lane"),
                      comment="On-time performance by origin, destination and service level")
def late_delivery_by_lane():
    return g.build_late_delivery_by_lane(gold("fact_shipment_delivery"))


@dp.materialized_view(name=table("gold", "invoice_status_funnel"),
                      comment="Invoices by their latest lifecycle status, per carrier")
def invoice_status_funnel():
    return g.build_invoice_status_funnel(silver("invoice_status"))
