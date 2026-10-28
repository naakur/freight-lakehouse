"""
CDC + SCD (slowly changing dimensions) for carriers.

INPUT : silver.carriers_cdc  = validated CHANGE feed (op = I/U/D, change_ts). Bad rows are already in quarantine.
OUTPUT: two tables built from the SAME feed by two "AUTO CDC" flows:

  gold.dim_carrier_current   SCD TYPE 1 - ONE row per carrier, always the latest values.
                             Updates overwrite, deletes remove the row. No history.
                             Use it for "who is the carrier today?".

  gold.dim_carrier           SCD TYPE 2 - ONE row per VERSION of a carrier.
                             Each change closes the old row and opens a new one:
                               __START_AT = when this version became true
                               __END_AT   = when it stopped being true (NULL = still current)
                             Use it for "what were the payment terms when this invoice was issued?".

AUTO CDC reads the feed in change_ts order PER KEY, even when records arrive out of order:
a late record (older change_ts) is slotted into the right place and the history is rewritten.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.config import table

FEED = table("silver", "carriers_cdc")

# Feed columns that describe the CHANGE itself, not the carrier: keep them out of the dimension tables
CHANGE_COLUMNS = ["op", "change_ts", "_source_file", "_ingested_at", "_silver_at"]


@dp.temporary_view(name="carriers_cdc_feed")
def carriers_cdc_feed():
    """The change feed, read as a stream so only NEW changes are processed on each run."""
    return spark.readStream.table(FEED)


# ---- SCD type 1: current state only -------------------------------------------------------------
dp.create_streaming_table(
    name=table("gold", "dim_carrier_current"),
    comment="Carriers, latest values only (SCD type 1)",
)
dp.create_auto_cdc_flow(
    target=table("gold", "dim_carrier_current"),
    source="carriers_cdc_feed",
    keys=["carrier_id"],                              # which column(s) identify one carrier
    sequence_by=F.col("change_ts"),                   # decides which change is newer
    apply_as_deletes=F.expr("op = 'D'"),              # rows with this condition delete the carrier
    except_column_list=CHANGE_COLUMNS,
    stored_as_scd_type=1,
)

# ---- SCD type 2: full history -------------------------------------------------------------------
dp.create_streaming_table(
    name=table("gold", "dim_carrier"),
    comment="Carriers with full history, one row per version (SCD type 2): __START_AT / __END_AT",
)
dp.create_auto_cdc_flow(
    target=table("gold", "dim_carrier"),
    source="carriers_cdc_feed",
    keys=["carrier_id"],
    sequence_by=F.col("change_ts"),
    apply_as_deletes=F.expr("op = 'D'"),              # in SCD2 a delete CLOSES the current version
    except_column_list=CHANGE_COLUMNS,
    stored_as_scd_type=2,
)
