"""
BRONZE (batch): CSV files -> Delta tables, using Auto Loader.

Rules of bronze (keep them boring!):
  1. Store data exactly as received. Every column is a STRING (no type errors possible).
  2. Add audit columns: which file it came from and when we loaded it.
  3. Never filter or fix anything here. Fixing happens in silver.

Auto Loader = Spark's "cloudFiles" source. It remembers which files it already read
(in the checkpoint), so re-running the pipeline never loads a file twice.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from utilities.config import landing_path, table
from utilities.schemas import SOURCES


def register_bronze(source: str, columns: list):
    """Create one bronze streaming table for one CSV source."""

    # An explicit all-string schema = our "data contract" for this file type.
    # Plus ONE extra column, _corrupt_record: Spark puts the ORIGINAL LINE there when a row
    # is malformed (e.g. too many or too few fields). Without it, such rows are silently
    # accepted with the extra values thrown away.
    schema = StructType(
        [StructField(c, StringType(), True) for c in columns]
        + [StructField("_corrupt_record", StringType(), True)]
    )

    @dp.table(
        name=table("bronze", source),
        comment=f"Raw {source} CSV rows exactly as received (all strings) + audit columns",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")            # Auto Loader
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            # Anything that does not fit the schema (e.g. extra columns) is kept as JSON
            # in this column instead of being lost. Silver will quarantine those rows.
            .option("rescuedDataColumn", "_rescued_data")
            .option("mode", "PERMISSIVE")                                  # keep bad rows, do not fail or drop
            .option("columnNameOfCorruptRecord", "_corrupt_record")        # ...but record them
            .schema(schema)
            .load(landing_path(source))
            # Audit columns
            .select(
                "*",
                F.col("_metadata.file_path").alias("_source_file"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )


# One bronze table per source (carriers, rate_cards, shipments, invoices, ...)
for _source, _columns in SOURCES.items():
    register_bronze(_source, _columns)
