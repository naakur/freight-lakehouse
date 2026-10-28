-- Replace freight_dev with your catalog.

-- 1. Findings: how many per rule and how much money to recover
SELECT rule_id, severity, COUNT(*) AS findings, SUM(recoverable_amount) AS recoverable
FROM freight_dev.gold.fact_audit_finding GROUP BY rule_id, severity ORDER BY recoverable DESC;

-- 2. Carrier scorecard (one row per carrier)
SELECT * FROM freight_dev.gold.carrier_scorecard ORDER BY recoverable_total DESC;

-- 3. SCD2 in action: payment terms as they were ON THE INVOICE DATE (C001 changed terms several times)
SELECT carrier_id, payment_terms_at_invoice, MIN(invoice_date) AS first_invoice, MAX(invoice_date) AS last_invoice, COUNT(*) AS invoices
FROM freight_dev.gold.fact_invoice WHERE carrier_id = 'C001'
GROUP BY carrier_id, payment_terms_at_invoice ORDER BY first_invoice;

-- 4. Late deliveries by lane (worst first)
SELECT * FROM freight_dev.gold.late_delivery_by_lane ORDER BY late_pct DESC LIMIT 10;

-- 5. Recoverable dollars by month
SELECT invoice_month, SUM(recoverable_amount) AS recoverable
FROM freight_dev.gold.recoverable_by_month GROUP BY invoice_month ORDER BY invoice_month;

-- 6. Latest gold audit results
SELECT check_name, entity, expected, actual, status
FROM freight_dev.ops.reconciliation_results
WHERE run_id = (SELECT run_id FROM freight_dev.ops.reconciliation_results ORDER BY checked_at DESC LIMIT 1)
ORDER BY status DESC, check_name;
