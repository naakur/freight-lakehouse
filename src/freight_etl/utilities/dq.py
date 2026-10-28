"""
Tiny data-quality (DQ) helpers.

Idea: every silver table has a dictionary of RULES  {rule_name: SQL expression}.
A row that breaks ANY rule is NOT thrown away - it goes to a quarantine table
together with the names of the rules it broke, so someone can fix and replay it.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_dq_flags(df: DataFrame, rules: dict) -> DataFrame:
    """Add column _failed_rules = array of the rule names this row FAILED ([] = clean)."""
    checks = [
        # when(rule is false OR null) -> emit the rule name; otherwise NULL
        F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name))
        for name, expr in rules.items()
    ]
    # array_compact removes the NULLs, leaving only the names of failed rules
    return df.withColumn("_failed_rules", F.array_compact(F.array(*checks)))


def valid_rows(df: DataFrame) -> DataFrame:
    """Rows that passed every rule."""
    return df.filter(F.size("_failed_rules") == 0).drop("_failed_rules")


def invalid_rows(df: DataFrame) -> DataFrame:
    """Rows that broke at least one rule."""
    return df.filter(F.size("_failed_rules") > 0)
