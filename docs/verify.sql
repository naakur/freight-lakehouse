-- Paste into a SQL editor / notebook. Replace freight_dev with your catalog.

-- 1. Row counts per layer (bronze = silver + quarantine)
SELECT 'bronze'     AS layer, 'invoices' AS entity, COUNT(*) AS n FROM freight_dev.bronze.invoices
UNION ALL SELECT 'silver',     'invoices', COUNT(*) FROM freight_dev.silver.invoices
UNION ALL SELECT 'quarantine', 'invoices', COUNT(*) FROM freight_dev.quarantine.invoices;

-- 2. Why rows were quarantined
SELECT _failed_rules, _raw_record, _source_file
FROM freight_dev.quarantine.invoices;

-- 3. Which FILES were rejected before ingestion, and why
SELECT source, file_name, status, reason
FROM freight_dev.ops.file_validation_log
ORDER BY event_ts DESC;

-- 4. Pipeline run log (newest first)
SELECT run_id, task, status, message, rows_in, rows_out, event_ts
FROM freight_dev.ops.pipeline_run_log
ORDER BY event_ts DESC;

-- 5. Reconciliation results of the latest run
SELECT check_name, entity, expected, actual, status
FROM freight_dev.ops.reconciliation_results
WHERE run_id = (SELECT run_id FROM freight_dev.ops.reconciliation_results ORDER BY checked_at DESC LIMIT 1)
ORDER BY status DESC, entity;
