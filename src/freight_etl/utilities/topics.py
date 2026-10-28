"""
Kafka topic definitions: how to parse each topic's JSON value and which rules a good message must pass.
Pure data (no Spark objects), so tests and notebooks can import it.

Per topic:
  entity   : base name of the silver / quarantine table
  id_col   : business id used to remove duplicates (Kafka is "at least once": the same event can arrive twice)
  time_col : event time column (used for the dedupe watermark)
  schema   : DDL for from_json. Fields missing in older messages simply come back NULL, so ONE schema
             can read schema v1 AND v2 (tracking v2 added `geo`).
  rules    : {rule_name: SQL expression that must be TRUE for a good message}
"""

# Added to every topic. from_json puts the original text in _corrupt_json when it cannot parse a message.
STRUCTURAL_RULES = {"well_formed_json": "_corrupt_json IS NULL"}

TOPICS = {
    "freight.tracking.events": {
        "entity": "tracking_events",
        "id_col": "event_id",
        "time_col": "event_ts",
        "schema": (
            "event_id STRING, shipment_id STRING, carrier_id STRING, status STRING, "
            "event_ts TIMESTAMP, location STRING, geo STRUCT<lat: DOUBLE, lon: DOUBLE>"
        ),
        "rules": {
            "event_id_present": "event_id IS NOT NULL",
            "shipment_id_present": "shipment_id IS NOT NULL",
            "event_ts_present": "event_ts IS NOT NULL",
            "status_known": "status IN ('PICKED_UP','IN_TRANSIT','OUT_FOR_DELIVERY','DELIVERED','EXCEPTION')",
        },
    },
    "freight.invoice.status": {
        "entity": "invoice_status",
        "id_col": "status_event_id",
        "time_col": "status_ts",
        "schema": (
            "status_event_id STRING, invoice_number STRING, shipment_id STRING, carrier_id STRING, "
            "status STRING, status_ts TIMESTAMP, total_amount DOUBLE, reason STRING"
        ),
        "rules": {
            "status_event_id_present": "status_event_id IS NOT NULL",
            "invoice_number_present": "invoice_number IS NOT NULL",
            "status_ts_present": "status_ts IS NOT NULL",
            "status_known": "status IN ('RECEIVED','APPROVED','DISPUTED','PAID')",
            "total_not_negative": "total_amount IS NOT NULL AND total_amount >= 0",
        },
    },
    "freight.rate.changes": {
        "entity": "rate_changes",
        "id_col": "notice_id",
        "time_col": "announced_ts",
        "schema": (
            "notice_id STRING, carrier_id STRING, origin_region STRING, dest_region STRING, "
            "service_level STRING, old_rate_per_lb DOUBLE, new_rate_per_lb DOUBLE, "
            "effective_date DATE, announced_ts TIMESTAMP"
        ),
        "rules": {
            "notice_id_present": "notice_id IS NOT NULL",
            "announced_ts_present": "announced_ts IS NOT NULL",
            "new_rate_positive": "new_rate_per_lb IS NOT NULL AND new_rate_per_lb > 0",
        },
    },
    "freight.claims.filed": {
        "entity": "claims",
        "id_col": "claim_id",
        "time_col": "filed_ts",
        "schema": (
            "claim_id STRING, shipment_id STRING, carrier_id STRING, claim_type STRING, "
            "filed_ts TIMESTAMP, amount STRUCT<currency: STRING, value: DOUBLE>, reason STRING, "
            "documents ARRAY<STRUCT<doc_type: STRING, uri: STRING>>"
        ),
        "rules": {
            "claim_id_present": "claim_id IS NOT NULL",
            "filed_ts_present": "filed_ts IS NOT NULL",
            "claim_type_known": "claim_type IN ('LATE_DELIVERY','DAMAGE')",
            "amount_positive": "amount.value IS NOT NULL AND amount.value > 0",
        },
    },
}


# Shape of one Kafka-style message stored as a line of JSON in the stream/ files (file source only).
ENVELOPE_SCHEMA = (
    "topic STRING, partition INT, offset BIGINT, key STRING, value STRING, "
    "timestamp TIMESTAMP, headers STRUCT<schema_version: STRING>"
)
