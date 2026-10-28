# Databricks notebook source
# MAGIC %md
# MAGIC ## Audit the gold layer (runs after the gold pipeline)
# MAGIC
# MAGIC | Check | Meaning |
# MAGIC |---|---|
# MAGIC | `fact_invoice_rows_vs_silver` / `_total_vs_silver` | no invoice lost or duplicated, and the money adds up to the cent |
# MAGIC | `fact_delivery_rows_vs_silver` | one delivery row per shipment |
# MAGIC | `fact_claim_rows_vs_silver` | every claim is in the fact |
# MAGIC | `invoices_without_carrier_version` | every invoice found a carrier version in the SCD2 history |
# MAGIC | `scorecard_invoices_vs_fact` / `recoverable_by_month_vs_findings` | the summary tables add up to their facts |
# MAGIC | `recall_<rule>` / `precision_<rule>` | the audit rules vs the GROUND-TRUTH file (needs `truth/invoice_anomaly_labels.csv`) |
# MAGIC
# MAGIC Practice rows (invoice numbers starting `INV99`, from the bad-sample files) are left out of the rule evaluation.

# COMMAND ----------
# MAGIC %run ./ops_common

# COMMAND ----------

from pyspark.sql import functions as F

log_event("audit_gold", "STARTED")
results = []    # (run_id, check_name, entity, expected, actual, status)
RULE_THRESHOLD_PCT = 99
PRACTICE_PREFIX = "INV99"


def q(sql):
    return spark.sql(sql).first()[0]


def add(check, entity, expected, actual):
    results.append((RUN_ID, check, entity, int(expected), int(actual), "PASS" if expected == actual else "FAIL"))


def cents(table, col):
    """Total of a money column in cents (a whole number, so equality is exact)."""
    return q(f"SELECT CAST(ROUND(COALESCE(SUM({col}), 0) * 100) AS BIGINT) FROM {CATALOG}.{table}")


# COMMAND ----------
# MAGIC %md ### 1. Row counts and money totals tie out

# COMMAND ----------

def n(t):
    """Row count of a table in this environment."""
    return q(f"SELECT COUNT(*) FROM {CATALOG}.{t}")

add("fact_invoice_rows_vs_silver", "fact_invoice", n("silver.invoices"), n("gold.fact_invoice"))
add("fact_invoice_total_vs_silver", "fact_invoice", cents("silver.invoices", "total_amount"), cents("gold.fact_invoice", "total_amount"))
add("fact_delivery_rows_vs_silver", "fact_shipment_delivery", n("silver.shipments"), n("gold.fact_shipment_delivery"))
add("fact_claim_rows_vs_silver", "fact_claim", n("silver.claims"), n("gold.fact_claim"))
add("invoices_without_carrier_version", "fact_invoice", 0,
    q(f"SELECT COUNT(*) FROM {CATALOG}.gold.fact_invoice WHERE payment_terms_at_invoice IS NULL"))
add("scorecard_invoices_vs_fact", "carrier_scorecard", n("gold.fact_invoice"),
    q(f"SELECT COALESCE(SUM(invoices), 0) FROM {CATALOG}.gold.carrier_scorecard"))
add("recoverable_by_month_vs_findings", "recoverable_by_month",
    q(f"SELECT CAST(ROUND(COALESCE(SUM(recoverable_amount), 0) * 100) AS BIGINT) FROM {CATALOG}.gold.fact_audit_finding WHERE severity <> 'REVIEW'"),
    cents("gold.recoverable_by_month", "recoverable_amount"))

# COMMAND ----------
# MAGIC %md ### 2. Audit rules vs the ground-truth file (recall and precision per rule)
# MAGIC * **recall** = of the errors that really exist, how many did we find?
# MAGIC * **precision** = of the alerts we raised, how many were real errors?

# COMMAND ----------

truth_path = f"{LANDING_ROOT}/truth/invoice_anomaly_labels.csv"
try:
    truth = (spark.read.option("header", "true").csv(truth_path)
             .select("invoice_number", F.col("anomaly_type").alias("rule_id"))
             .where(~F.col("invoice_number").startswith(PRACTICE_PREFIX)))
    truth.cache().count()
    have_truth = True
except Exception:
    have_truth = False
    results.append((RUN_ID, "ground_truth_file_present", "fact_audit_finding", 1, 0, "WARN"))
    print("no ground-truth file at", truth_path, "- rule evaluation skipped")

if have_truth:
    found = (spark.table(f"{CATALOG}.gold.fact_audit_finding").select("invoice_number", "rule_id")
             .where(~F.col("invoice_number").startswith(PRACTICE_PREFIX)))
    for rule in [r["rule_id"] for r in truth.select("rule_id").distinct().collect()]:
        t, f = truth.where(F.col("rule_id") == rule), found.where(F.col("rule_id") == rule)
        hits = t.join(f, ["invoice_number", "rule_id"]).count()
        recall = int(hits * 100 / max(t.count(), 1))
        precision = int(hits * 100 / max(f.count(), 1))
        for name, value in (("recall", recall), ("precision", precision)):
            results.append((RUN_ID, f"{name}_{rule}", "fact_audit_finding", RULE_THRESHOLD_PCT, value,
                            "PASS" if value >= RULE_THRESHOLD_PCT else "FAIL"))

# COMMAND ----------

cols = "run_id string, check_name string, entity string, expected long, actual long, status string"
res_df = spark.createDataFrame(results, cols).withColumn("checked_at", F.lit(datetime.now(timezone.utc)))
res_df.write.mode("append").saveAsTable(f"{CATALOG}.ops.reconciliation_results")
display(res_df)

failed = [r for r in results if r[5] == "FAIL"]
warned = [r for r in results if r[5] == "WARN"]
if failed:
    log_event("audit_gold", "FAILED", message=f"{len(failed)} check(s) failed")
    raise Exception(f"Gold audit FAILED: {[(r[1], r[3], r[4]) for r in failed]}")
log_event("audit_gold", "WARN" if warned else "SUCCESS", message=f"{len(results)} checks, {len(warned)} warning(s)")
