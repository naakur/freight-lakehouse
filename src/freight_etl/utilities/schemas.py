"""
Single source of truth for the batch CSV sources.

Used by:
  * the bronze pipeline    (builds the Auto Loader schema)
  * the preflight notebook (checks a file's header BEFORE ingesting it)
  * the audit notebook     (loops over every source)

Pure Python on purpose (no Spark) so it is easy to unit test.
"""

# source name -> the exact column order we expect in the CSV header
SOURCES = {
    "carriers": ["carrier_id", "carrier_name", "scac"],
    "rate_cards": [
        "rate_id", "carrier_id", "origin_region", "dest_region", "service_level",
        "rate_per_lb", "min_charge", "fuel_surcharge_pct", "effective_from", "effective_to",
    ],
    # CHANGE FEED for carriers (CDC): op = I/U/D, change_ts = when the change happened at the source
    "carriers_cdc": ["carrier_id", "carrier_name", "scac", "payment_terms", "op", "change_ts"],
    "accessorial_codes": ["accessorial_code", "description", "flat_amount"],
    "gl_codes": ["gl_code", "gl_description"],
    "shipments": [
        "shipment_id", "carrier_id", "po_number", "origin_region", "dest_region",
        "service_level", "ship_date", "promised_delivery_date", "weight_lbs",
        "pieces", "accessorials", "gl_code",
    ],
    "invoices": [
        "invoice_number", "carrier_id", "shipment_id", "invoice_date", "billed_weight_lbs",
        "billed_rate_per_lb", "base_charge", "fuel_surcharge_pct", "fuel_amount",
        "accessorials", "accessorial_amount", "total_amount", "currency",
    ],
}
