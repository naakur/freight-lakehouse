"""
Silver entity definitions: how to type each column and which rules a good row must pass.
Pure data (no Spark objects), so tests can import it without a pipeline running.

For each entity:
  select : SQL expressions that type/clean the bronze (all-string) columns
           NOTE: use try_cast, never CAST (serverless runs ANSI mode: CAST('abc' AS DOUBLE) crashes)
  rules  : {rule_name: SQL expression that must be TRUE for a good row}
"""

ENTITIES = {
    "carriers": {
        "select": [
            "carrier_id",
            "carrier_name",
            "scac",
        ],
        "rules": {
            "carrier_id_present": "carrier_id IS NOT NULL",
            "carrier_name_present": "carrier_name IS NOT NULL",
            "scac_is_4_chars": "length(scac) = 4",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
    "carriers_cdc": {
        "select": [
            "carrier_id",
            "carrier_name",
            "scac",
            "payment_terms",
            "upper(op) AS op",
            "try_cast(change_ts AS TIMESTAMP) AS change_ts",
        ],
        "rules": {
            "carrier_id_present": "carrier_id IS NOT NULL",
            "op_is_I_U_or_D": "op IN ('I','U','D')",
            "change_ts_valid": "change_ts IS NOT NULL",
            # a delete only needs the key; inserts and updates need the descriptive columns
            "attributes_present_unless_delete": "op = 'D' OR (carrier_name IS NOT NULL AND payment_terms RLIKE '^NET[0-9]+$')",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
    "accessorial_codes": {
        "select": [
            "accessorial_code",
            "description",
            "try_cast(flat_amount AS DECIMAL(10,2)) AS flat_amount",
        ],
        "rules": {
            "code_present": "accessorial_code IS NOT NULL",
            "amount_not_negative": "flat_amount IS NOT NULL AND flat_amount >= 0",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
    "gl_codes": {
        "select": [
            "gl_code",
            "gl_description",
        ],
        "rules": {
            "gl_code_present": "gl_code IS NOT NULL",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
    "rate_cards": {
        "select": [
            "rate_id",
            "carrier_id",
            "origin_region",
            "dest_region",
            "service_level",
            "try_cast(rate_per_lb AS DOUBLE) AS rate_per_lb",
            "try_cast(min_charge AS DECIMAL(10,2)) AS min_charge",
            "try_cast(fuel_surcharge_pct AS DOUBLE) AS fuel_surcharge_pct",
            "try_cast(effective_from AS DATE) AS effective_from",
            "try_cast(effective_to AS DATE) AS effective_to",
        ],
        "rules": {
            "rate_id_present": "rate_id IS NOT NULL",
            "rate_positive": "rate_per_lb IS NOT NULL AND rate_per_lb > 0",
            "min_charge_not_negative": "min_charge IS NOT NULL AND min_charge >= 0",
            "fuel_pct_between_0_and_1": "fuel_surcharge_pct BETWEEN 0 AND 1",
            "valid_date_range": "effective_from IS NOT NULL AND effective_from <= effective_to",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
    "shipments": {
        "select": [
            "shipment_id",
            "carrier_id",
            "po_number",
            "origin_region",
            "dest_region",
            "service_level",
            "try_cast(ship_date AS DATE) AS ship_date",
            "try_cast(promised_delivery_date AS DATE) AS promised_delivery_date",
            "try_cast(weight_lbs AS DOUBLE) AS weight_lbs",
            "try_cast(pieces AS INT) AS pieces",
            "filter(split(coalesce(accessorials, ''), '[|]'), x -> x <> '') AS accessorial_codes",
            "gl_code",
        ],
        "rules": {
            "shipment_id_present": "shipment_id IS NOT NULL",
            "carrier_id_present": "carrier_id IS NOT NULL",
            "ship_date_valid": "ship_date IS NOT NULL",
            "promised_not_before_ship": "promised_delivery_date >= ship_date",
            "weight_positive": "weight_lbs IS NOT NULL AND weight_lbs > 0",
            "service_level_known": "service_level IN ('GROUND','EXPRESS','ECONOMY')",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
    "invoices": {
        "select": [
            "invoice_number",
            "carrier_id",
            "shipment_id",
            "try_cast(invoice_date AS DATE) AS invoice_date",
            "try_cast(billed_weight_lbs AS DOUBLE) AS billed_weight_lbs",
            "try_cast(billed_rate_per_lb AS DOUBLE) AS billed_rate_per_lb",
            "try_cast(base_charge AS DECIMAL(12,2)) AS base_charge",
            "try_cast(fuel_surcharge_pct AS DOUBLE) AS fuel_surcharge_pct",
            "try_cast(fuel_amount AS DECIMAL(12,2)) AS fuel_amount",
            "filter(split(coalesce(accessorials, ''), '[|]'), x -> x <> '') AS accessorial_codes",
            "try_cast(accessorial_amount AS DECIMAL(12,2)) AS accessorial_amount",
            "try_cast(total_amount AS DECIMAL(12,2)) AS total_amount",
            "currency",
        ],
        "rules": {
            "invoice_number_present": "invoice_number IS NOT NULL",
            "shipment_id_present": "shipment_id IS NOT NULL",
            "carrier_id_present": "carrier_id IS NOT NULL",
            "invoice_date_valid": "invoice_date IS NOT NULL",
            "weight_positive": "billed_weight_lbs IS NOT NULL AND billed_weight_lbs > 0",
            "total_not_negative": "total_amount IS NOT NULL AND total_amount >= 0",
            "no_unexpected_columns": "_rescued_data IS NULL",
        },
    },
}


# Rules that apply to EVERY entity (added automatically in silver and in the tests).
# A malformed CSV row (wrong number of fields) gets its original line stored in _corrupt_record.
STRUCTURAL_RULES = {
    "row_well_formed": "_corrupt_record IS NULL",
}
