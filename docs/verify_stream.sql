-- Replace freight_dev with your catalog.

-- 1. Messages per topic in the multiplexed bronze table
SELECT topic, COUNT(*) AS messages, COUNT(DISTINCT partition) AS partitions
FROM freight_dev.bronze.kafka_multiplex GROUP BY topic ORDER BY topic;

-- 2. Silver and quarantine per entity
SELECT 'tracking_events' AS entity, (SELECT COUNT(*) FROM freight_dev.silver.tracking_events) AS silver, (SELECT COUNT(*) FROM freight_dev.quarantine.tracking_events) AS quarantine
UNION ALL SELECT 'invoice_status', (SELECT COUNT(*) FROM freight_dev.silver.invoice_status), (SELECT COUNT(*) FROM freight_dev.quarantine.invoice_status)
UNION ALL SELECT 'rate_changes',   (SELECT COUNT(*) FROM freight_dev.silver.rate_changes),   (SELECT COUNT(*) FROM freight_dev.quarantine.rate_changes)
UNION ALL SELECT 'claims',         (SELECT COUNT(*) FROM freight_dev.silver.claims),         (SELECT COUNT(*) FROM freight_dev.quarantine.claims);

-- 3. The broken messages (original text kept, with their Kafka coordinates for replay)
SELECT topic, partition, offset, _failed_rules, _raw_record
FROM freight_dev.quarantine.tracking_events LIMIT 10;

-- 4. Schema evolution: v1 and v2 tracking messages parsed by the same schema
SELECT schema_version, COUNT(*) AS n, COUNT(geo) AS with_geo
FROM freight_dev.silver.tracking_events GROUP BY schema_version;

-- 5. Duplicates really gone? (must return 0 rows)
SELECT event_id, COUNT(*) FROM freight_dev.silver.tracking_events GROUP BY event_id HAVING COUNT(*) > 1;

-- 6. Latest reconciliation results
SELECT check_name, entity, expected, actual, status
FROM freight_dev.ops.reconciliation_results
WHERE run_id = (SELECT run_id FROM freight_dev.ops.reconciliation_results ORDER BY checked_at DESC LIMIT 1);
