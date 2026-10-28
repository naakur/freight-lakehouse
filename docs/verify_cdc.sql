-- Replace freight_dev with your catalog.

-- 1. The current table: one row per carrier today
SELECT * FROM freight_dev.gold.dim_carrier_current ORDER BY carrier_id;

-- 2. The history table: one row per VERSION
SELECT carrier_id, carrier_name, payment_terms, `__START_AT`, `__END_AT`
FROM freight_dev.gold.dim_carrier
ORDER BY carrier_id, `__START_AT`;

-- 3. Point in time: what were C001's payment terms on 2026-03-01?
--    After wave A: NET30.  After wave B (late record arrives): NET40  <- history was rewritten
SELECT carrier_id, payment_terms
FROM freight_dev.gold.dim_carrier
WHERE carrier_id = 'C001'
  AND TIMESTAMP '2026-03-01 00:00:00' >= `__START_AT`
  AND (`__END_AT` IS NULL OR TIMESTAMP '2026-03-01 00:00:00' < `__END_AT`);

-- 4. The broken feed rows (after wave B)
SELECT _failed_rules, _raw_record FROM freight_dev.quarantine.carriers_cdc;

-- 5. Audit results of the last run
SELECT check_name, entity, expected, actual, status
FROM freight_dev.ops.reconciliation_results
WHERE run_id = (SELECT run_id FROM freight_dev.ops.reconciliation_results ORDER BY checked_at DESC LIMIT 1)
ORDER BY check_name;
