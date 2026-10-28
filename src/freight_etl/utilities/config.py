"""
Environment-aware helpers. Nothing here knows if it runs in dev, test or prod:
the bundle passes the catalog and landing path in as *pipeline configuration*.
"""
from pyspark.sql import SparkSession


def conf(key: str) -> str:
    # Read a value from the pipeline settings (see resources/pipelines.yml)
    return SparkSession.getActiveSession().conf.get(key)


def table(schema: str, name: str) -> str:
    """Full three-part table name, e.g. freight_dev.silver.invoices"""
    return f"{conf('freight.catalog')}.{schema}.{name}"


def landing_path(source: str) -> str:
    """Folder Auto Loader watches for one source (only validated files live here)."""
    return f"{conf('freight.landing_root')}/batch/{source}/"


def stream_path() -> str:
    """Folder Auto Loader watches for the file-based stream (Kafka-style JSON files)."""
    return f"{conf('freight.landing_root')}/stream/"
