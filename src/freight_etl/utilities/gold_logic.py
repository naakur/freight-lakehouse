"""
Gold business logic as PLAIN FUNCTIONS: DataFrames in, DataFrame out.

Why not write this directly in the pipeline file?
  * these functions run on a laptop with local Spark, so we can TEST the audit rules against the
    ground-truth file before anything is deployed
  * the pipeline file (transformations/gold/gold_model.py) only wires them to tables

Money rule: amounts are DECIMAL(12,2); comparisons use small tolerances so rounding never causes a false alarm.
"""
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# --- audit rule tolerances -------------------------------------------------------------------------
RATE_TOLERANCE = 0.00005     # rate per lb is stored with 4 decimals
FUEL_TOLERANCE = 0.0005      # fuel surcharge percent is stored with 3 decimals
WEIGHT_TOLERANCE = 1.05      # billed weight may exceed shipped weight by 5% before we flag it

MONEY = "decimal(12,2)"


# =================================================================================================
# AUDIT FINDINGS  (grain: one row per invoice x rule that fired)
# =================================================================================================
def build_findings(invoices: DataFrame, shipments: DataFrame, rate_cards: DataFrame,
                   accessorials: DataFrame) -> DataFrame:
    """Compare every invoice with the shipment and the CONTRACT RATE that was valid on the ship date."""

    # Prefix the columns of the side tables so nothing is ambiguous after the joins
    shp = shipments.select(
        "shipment_id",
        F.col("origin_region").alias("s_origin"), F.col("dest_region").alias("s_dest"),
        F.col("carrier_id").alias("s_carrier_id"),
        F.col("service_level").alias("s_service"), F.col("ship_date").alias("s_ship_date"),
        F.col("weight_lbs").alias("s_weight"), F.col("accessorial_codes").alias("s_acc"),
    )
    rc = rate_cards.select(
        F.col("carrier_id").alias("rc_carrier_id"), F.col("origin_region").alias("rc_origin"),
        F.col("dest_region").alias("rc_dest"), F.col("service_level").alias("rc_service"),
        F.col("rate_per_lb").alias("rc_rate"), F.col("min_charge").cast("double").alias("rc_min"),
        F.col("fuel_surcharge_pct").alias("rc_fuel"),
        F.col("effective_from").alias("rc_from"), F.col("effective_to").alias("rc_to"),
    )

    # invoice + its shipment + the contract rate valid ON THE SHIP DATE (point-in-time join)
    j = (
        invoices.join(shp, "shipment_id", "left")
        .join(
            rc,
            (F.col("s_carrier_id") == F.col("rc_carrier_id"))
            & (F.col("s_origin") == F.col("rc_origin")) & (F.col("s_dest") == F.col("rc_dest"))
            & (F.col("s_service") == F.col("rc_service"))
            & F.col("s_ship_date").between(F.col("rc_from"), F.col("rc_to")),
            "left",
        )
    )

    def finding(df, rule_id, severity, condition, expected, billed):
        return df.where(condition).select(
            "invoice_number", "shipment_id", "carrier_id", "invoice_date",
            F.lit(rule_id).alias("rule_id"), F.lit(severity).alias("severity"),
            expected.cast(MONEY).alias("expected_amount"), billed.cast(MONEY).alias("billed_amount"),
        )

    parts = []

    # 1. Billed rate is higher than the contract rate
    parts.append(finding(
        j, "OVERCHARGE_RATE", "HIGH",
        F.col("billed_rate_per_lb") > F.col("rc_rate") + RATE_TOLERANCE,
        F.greatest(F.col("rc_min"), F.col("billed_weight_lbs") * F.col("rc_rate")),
        F.col("base_charge"),
    ))

    # 2. Fuel surcharge percent is higher than the contract
    parts.append(finding(
        j, "FUEL_VARIANCE", "MEDIUM",
        F.col("fuel_surcharge_pct") > F.col("rc_fuel") + FUEL_TOLERANCE,
        F.col("base_charge") * F.col("rc_fuel"),
        F.col("fuel_amount"),
    ))

    # 3. Billed weight is more than 5% above the weight on the shipment
    parts.append(finding(
        j, "WEIGHT_MISMATCH", "HIGH",
        F.col("billed_weight_lbs") > F.col("s_weight") * WEIGHT_TOLERANCE,
        F.greatest(F.col("rc_min"), F.col("s_weight") * F.col("billed_rate_per_lb")),
        F.col("base_charge"),
    ))

    # 4. Accessorial charges the shipment never had (e.g. liftgate that was not ordered)
    extras = (
        j.select("invoice_number", F.explode(F.array_except("accessorial_codes", "s_acc")).alias("accessorial_code"))
        .join(accessorials.select("accessorial_code", "flat_amount"), "accessorial_code", "left")
        .groupBy("invoice_number").agg(F.coalesce(F.sum("flat_amount"), F.lit(0)).alias("extra_amount"))
    )
    parts.append(finding(
        j.join(extras, "invoice_number"), "UNAUTHORIZED_ACCESSORIAL", "MEDIUM",
        F.lit(True),
        F.col("accessorial_amount") - F.col("extra_amount"),
        F.col("accessorial_amount"),
    ))

    # 5. The same shipment was invoiced twice by the same carrier (every invoice after the first is a duplicate)
    first_first = Window.partitionBy("carrier_id", "shipment_id").orderBy("invoice_date", "invoice_number")
    dup = j.withColumn("_rn", F.row_number().over(first_first))
    parts.append(finding(dup, "DUPLICATE_INVOICE", "HIGH", F.col("_rn") > 1, F.lit(0), F.col("total_amount")))

    # 6. Data problems that stop us from auditing an invoice at all (reported, recoverable amount = 0)
    parts.append(finding(j, "SHIPMENT_NOT_FOUND", "REVIEW", F.col("s_ship_date").isNull(),
                         F.col("total_amount"), F.col("total_amount")))
    parts.append(finding(j, "CONTRACT_RATE_NOT_FOUND", "REVIEW",
                         F.col("s_ship_date").isNotNull() & F.col("rc_rate").isNull(),
                         F.col("total_amount"), F.col("total_amount")))

    findings = reduce(DataFrame.unionByName, parts)
    return (
        findings
        .withColumn("recoverable_amount",
                    F.greatest(F.col("billed_amount") - F.col("expected_amount"), F.lit(0)).cast(MONEY))
        .withColumn("finding_id", F.sha2(F.concat_ws("|", "invoice_number", "rule_id"), 256))
        .select("finding_id", "invoice_number", "shipment_id", "carrier_id", "invoice_date",
                "rule_id", "severity", "expected_amount", "billed_amount", "recoverable_amount")
    )


# =================================================================================================
# FACTS
# =================================================================================================
def build_fact_invoice(invoices: DataFrame, shipments: DataFrame, carrier_history: DataFrame) -> DataFrame:
    """
    Grain: one row per invoice.
    Carrier attributes come from the SCD2 history: the version that was valid ON THE INVOICE DATE
    (the latest version that started on or before it), so payment terms are the ones that applied then.
    """
    hist = carrier_history.select(
        F.col("carrier_id").alias("h_carrier_id"),
        F.col("carrier_name").alias("carrier_name_at_invoice"),
        F.col("payment_terms").alias("payment_terms_at_invoice"),
        F.col("__START_AT").alias("h_start"),
    )
    j = invoices.join(
        hist,
        (F.col("carrier_id") == F.col("h_carrier_id")) & (F.col("h_start") <= F.col("invoice_date").cast("timestamp")),
        "left",
    )
    newest_first = Window.partitionBy("invoice_number").orderBy(F.col("h_start").desc_nulls_last())
    j = j.withColumn("_rn", F.row_number().over(newest_first)).where("_rn = 1").drop("_rn", "h_carrier_id", "h_start")

    shp = shipments.select("shipment_id", "origin_region", "dest_region", "service_level", "ship_date", "gl_code")
    terms_days = F.coalesce(F.regexp_extract("payment_terms_at_invoice", r"NET(\d+)", 1).cast("int"), F.lit(0))
    return (
        j.join(shp, "shipment_id", "left")
        .withColumn("due_date", F.date_add("invoice_date", terms_days))
        .select("invoice_number", "invoice_date", "carrier_id", "carrier_name_at_invoice",
                "payment_terms_at_invoice", "due_date", "shipment_id", "origin_region", "dest_region",
                "service_level", "ship_date", "gl_code", "billed_weight_lbs", "base_charge", "fuel_amount",
                "accessorial_amount", "total_amount", "accessorial_codes")
    )


def build_shipment_delivery(shipments: DataFrame, tracking: DataFrame) -> DataFrame:
    """
    Grain: one row per shipment. An ACCUMULATING SNAPSHOT: the row is filled in as tracking events arrive
    (picked up, delivered ...), and delivery performance is derived from it.
    """
    t = tracking.groupBy("shipment_id").agg(
        F.min(F.when(F.col("status") == "PICKED_UP", F.col("event_ts"))).alias("picked_up_ts"),
        F.max(F.when(F.col("status") == "DELIVERED", F.col("event_ts"))).alias("delivered_ts"),
        F.sum(F.when(F.col("status") == "EXCEPTION", 1).otherwise(0)).alias("exception_count"),
        F.max("event_ts").alias("last_event_ts"),
        F.count("*").alias("event_count"),
    )
    s = shipments.select("shipment_id", "carrier_id", "origin_region", "dest_region", "service_level",
                         "ship_date", "promised_delivery_date", "weight_lbs", "gl_code")
    return (
        s.join(t, "shipment_id", "left")
        .withColumn("is_delivered", F.col("delivered_ts").isNotNull())
        .withColumn("days_late", F.when(F.col("is_delivered"),
                                        F.datediff(F.col("delivered_ts").cast("date"), F.col("promised_delivery_date"))))
        .withColumn("is_late", F.coalesce(F.col("days_late") > 0, F.lit(False)))
        .withColumn("transit_days", F.when(F.col("is_delivered"),
                                           F.datediff(F.col("delivered_ts").cast("date"), F.col("ship_date"))))
    )


def build_fact_claim(claims: DataFrame) -> DataFrame:
    """Grain: one row per claim, with the nested amount flattened."""
    return claims.select(
        "claim_id", "shipment_id", "carrier_id", "claim_type", "filed_ts",
        F.col("amount.value").cast(MONEY).alias("claim_amount"),
        F.col("amount.currency").alias("currency"),
        "reason", F.size("documents").alias("document_count"),
    )


# =================================================================================================
# FLAT SUMMARY TABLES (denormalised, ready for dashboards)
# =================================================================================================
def build_carrier_scorecard(fact_invoice, findings, delivery, claims, carrier_history) -> DataFrame:
    inv = fact_invoice.groupBy("carrier_id").agg(
        F.countDistinct("invoice_number").alias("invoices"), F.sum("total_amount").alias("billed_total"))
    fnd = findings.where("severity <> 'REVIEW'").groupBy("carrier_id").agg(
        F.countDistinct("invoice_number").alias("invoices_with_findings"),
        F.count("*").alias("findings"), F.sum("recoverable_amount").alias("recoverable_total"))
    dlv = delivery.where("is_delivered").groupBy("carrier_id").agg(
        F.count("*").alias("delivered_shipments"), F.sum(F.col("is_late").cast("int")).alias("late_shipments"),
        F.round(F.avg(F.when(F.col("is_late"), F.col("days_late"))), 1).alias("avg_days_late_when_late"))
    clm = claims.groupBy("carrier_id").agg(F.count("*").alias("claims"), F.sum("claim_amount").alias("claims_amount"))

    # Name = the LATEST version in the SCD2 history. (The "current" table drops deleted carriers, but a
    # deleted carrier still has invoices, so its name must not turn into NULL.)
    newest = Window.partitionBy("carrier_id").orderBy(F.col("__START_AT").desc())
    names = (carrier_history.withColumn("_rn", F.row_number().over(newest)).where("_rn = 1")
             .select("carrier_id", "carrier_name", F.col("__END_AT").isNull().alias("is_active")))

    return (
        inv.join(fnd, "carrier_id", "left").join(dlv, "carrier_id", "left").join(clm, "carrier_id", "left")
        .join(names, "carrier_id", "left")
        .fillna(0, ["invoices_with_findings", "findings", "late_shipments", "claims"])
        .fillna(0, ["recoverable_total", "claims_amount"])
        .withColumn("invoice_error_rate_pct", F.round(F.col("invoices_with_findings") / F.col("invoices") * 100, 1))
        .withColumn("late_delivery_pct", F.round(F.col("late_shipments") / F.col("delivered_shipments") * 100, 1))
        .select("carrier_id", "carrier_name", "is_active", "invoices", "billed_total", "invoices_with_findings", "findings",
                "recoverable_total", "invoice_error_rate_pct", "delivered_shipments", "late_shipments",
                "late_delivery_pct", "avg_days_late_when_late", "claims", "claims_amount")
    )


def build_recoverable_by_month(findings: DataFrame) -> DataFrame:
    return (
        findings.where("severity <> 'REVIEW'")
        .groupBy(F.date_trunc("month", "invoice_date").cast("date").alias("invoice_month"), "carrier_id", "rule_id")
        .agg(F.count("*").alias("findings"), F.sum("recoverable_amount").alias("recoverable_amount"))
    )


def build_late_delivery_by_lane(delivery: DataFrame) -> DataFrame:
    return (
        delivery.where("is_delivered")
        .groupBy("origin_region", "dest_region", "service_level")
        .agg(F.count("*").alias("delivered_shipments"), F.sum(F.col("is_late").cast("int")).alias("late_shipments"),
             F.round(F.avg("transit_days"), 1).alias("avg_transit_days"))
        .withColumn("late_pct", F.round(F.col("late_shipments") / F.col("delivered_shipments") * 100, 1))
    )


def build_invoice_status_funnel(invoice_status: DataFrame) -> DataFrame:
    """Where do invoices stand today? Take each invoice's LATEST status event, then count."""
    newest = Window.partitionBy("invoice_number").orderBy(F.col("status_ts").desc())
    latest = invoice_status.withColumn("_rn", F.row_number().over(newest)).where("_rn = 1")
    return (
        latest.groupBy("carrier_id", "status")
        .agg(F.count("*").alias("invoices"), F.sum("total_amount").alias("total_amount"))
    )
