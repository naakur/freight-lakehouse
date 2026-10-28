"""
SILVER (batch): clean, typed, trusted data  +  a QUARANTINE table for bad rows.

For every entity we create TWO tables from the same bronze table:
    silver.<entity>      rows that passed ALL quality rules (typed correctly)
    quarantine.<entity>  rows that broke at least one rule (raw copy + rule names)

Guarantee we reconcile later:   bronze rows = silver rows + quarantine rows

IMPORTANT: serverless Spark runs in ANSI mode, where CAST('abc' AS DOUBLE) crashes the
whole pipeline. We use try_cast(), which returns NULL instead, and let a rule catch it.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import table
from utilities.dq import add_dq_flags, invalid_rows, valid_rows
from utilities.entities import ENTITIES, STRUCTURAL_RULES

# Columns that bronze adds - not part of the business record
AUDIT_COLS = ["_source_file", "_ingested_at"]


def register_silver(entity: str, select_exprs: list, rules: dict):
    """
    entity       : name of the entity, e.g. "invoices"
    select_exprs : SQL expressions that type/clean the columns (try_cast...)
    rules        : {rule_name: SQL expression that must be TRUE for a good row}
    """

    def flagged():
        """Read bronze, type the columns, and tag each row with the rules it failed."""
        raw = spark.readStream.table(table("bronze", entity))
        business_cols = [c for c in raw.columns
                         if c not in AUDIT_COLS + ["_rescued_data", "_corrupt_record"]]

        typed = raw.select(
            F.to_json(F.struct(*business_cols)).alias("_raw_record"),  # untouched copy for quarantine
            *[F.expr(e) for e in select_exprs],
            F.col("_rescued_data"),
            F.col("_corrupt_record"),
            *AUDIT_COLS,
        )
        # every entity also gets the structural rule(s), e.g. "row_well_formed"
        return add_dq_flags(typed, {**STRUCTURAL_RULES, **rules})

    # ---- good rows -> silver
    @dp.table(
        name=table("silver", entity),
        comment=f"Validated, typed {entity}",
        table_properties={"quality": "silver"},
    )
    def silver():
        return (
            valid_rows(flagged())
            .drop("_raw_record", "_rescued_data", "_corrupt_record")
            .withColumn("_silver_at", F.current_timestamp())
        )

    # ---- bad rows -> quarantine (same shape for every entity, so one query reads them all)
    @dp.table(
        name=table("quarantine", entity),
        comment=f"{entity} rows that failed data-quality rules, with the reasons",
        table_properties={"quality": "quarantine"},
    )
    def quarantine():
        return invalid_rows(flagged()).select(
            # the original line if the row was malformed, otherwise our JSON copy of the values
            F.coalesce(F.col("_corrupt_record"), F.col("_raw_record")).alias("_raw_record"),
            F.col("_failed_rules"),                          # why it failed
            "_source_file",
            "_ingested_at",
            F.current_timestamp().alias("_quarantined_at"),
        )


# =============================================================================
# Entity definitions: how to type each column + which rules a good row must pass
# =============================================================================


# =============================================================================
# Create silver + quarantine tables for every entity defined in utilities/entities.py
# =============================================================================
for _entity, _cfg in ENTITIES.items():
    register_silver(_entity, _cfg["select"], _cfg["rules"])
