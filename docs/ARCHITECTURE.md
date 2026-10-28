# Architecture (read this first)

## Goal
A freight-audit lakehouse that ingests **batch files (Auto Loader)** and **Kafka topics**, builds
**silver** (clean) and **gold** (facts, dimensions, summaries), applies **CDC / SCD2**, logs every run,
**quarantines** bad files and bad rows, and **reconciles** counts when a run finishes.
Deployed to dev / test / prod with Databricks Asset Bundles. Portable to Azure.

## Layers and where things live
| Schema | Purpose | Rule of thumb |
|---|---|---|
| `landing` (Volume) | raw files: `inbox/` -> `batch/` (passed) or `quarantine_files/` (failed) | files are never edited |
| `bronze` | rows exactly as received, all STRING, plus `_source_file`, `_ingested_at` | append-only, no fixing |
| `silver` | typed, validated, deduplicated | only rows that pass every rule |
| `quarantine` | rows that failed a rule: raw JSON + failed rule names | nothing is silently dropped |
| `gold` | dimensions, facts, flat summary tables | business-ready, documented |
| `ops` | `pipeline_run_log`, `file_validation_log`, `reconciliation_results` | our audit trail |

One catalog per environment: `freight_dev`, `freight_test`, `freight_prod`.

## Key design decisions (short ADRs)
1. **One catalog per environment in one workspace** - Free Edition has a single workspace. On Azure each environment gets its own workspace; only the bundle target changes.
2. **Bronze is all strings** - ingestion can never fail on a bad value. Typing happens in silver with `try_cast` (serverless runs ANSI mode where `CAST` crashes on bad text).
3. **Quarantine instead of drop** - every silver table has a twin quarantine table. Invariant: `bronze = silver + quarantine`. The audit step proves it after each run.
4. **Two kinds of quarantine** - *files* (wrong header, empty, not a CSV) are stopped by preflight before Auto Loader; *rows* (bad date, negative amount, unexpected columns) are stopped by silver rules.
5. **Rules are data** - rules live in `utilities/entities.py` as `{name: SQL expression}`. Easy to read, test and extend.
6. **Three pipelines, not one** - batch, streaming and gold have different cadences and failure modes. A job orchestrates them.
7. **Logging in jobs, not inside pipelines** - pipelines are declarative; the notebooks around them write `ops.*` tables.
8. **No environment values in code** - catalog and paths come from bundle variables -> pipeline configuration.
9. **Catalogs and schemas are bootstrapped, not deployed by the app** - a pipeline needs its catalog to exist at deploy time. `scripts/bootstrap_environment.sql` is the one-time platform step (Terraform on Azure); the setup job only adds the volume, folders and ops tables.
10. **The bronze source is swappable** - `bronze.kafka_multiplex` can be fed from files (Auto Loader) or from Kafka (Confluent / Event Hubs). Same table, same columns, chosen by the `stream_source` variable. Development and CI use files (fast, deterministic); the real broker is a configuration change.
11. **Business logic is plain functions** - `gold_logic.py` takes DataFrames and returns DataFrames, so the audit rules are unit tested against the ground-truth file before any deploy.
12. **Shared definitions** - `utilities/schemas.py` is the one place that lists sources and columns.

## Run flow (batch)
```
inbox/<source>/  --preflight-->  batch/<source>/  --Auto Loader--> bronze --> silver
                      |                                                   \-> quarantine
                      \--> quarantine_files/<source>/
                                                        audit_and_reconcile -> ops.*
```

## Planned design for the next steps
**Step 4 - Kafka multiplex bronze (BUILT).** One streaming table `bronze.kafka_multiplex` reading all topics
(`subscribe` list) with columns topic, partition, offset, key, value (string), timestamp. Python
`spark.readStream.format("kafka")` with the `kafkashaded` JAAS class; credentials from a secret scope
(one secret per key). A file-based variant with the same columns is used by tests/CI.
Silver routes by topic with `from_json`, dedupes on the business id with a watermark, quarantines unparseable values.

**Step 5 - CDC and SCD2 (BUILT for carriers).** `dim_carrier` from a change feed (op = I/U/D, sequence column) using
`create_auto_cdc_flow` with `stored_as_scd_type=2` (history) and a second target with type 1 (current only).
Rate changes from the `freight.rate.changes` topic feed `dim_rate_card` history.

**Step 6 - Gold model (BUILT).**
- Dimensions: `dim_carrier` (SCD2), `dim_rate_card` (effective-dated), `dim_gl_code`, `dim_accessorial`, `dim_date`, `dim_region`.
- Facts: `fact_shipment`, `fact_invoice`, `fact_tracking_event`, `fact_shipment_delivery` (accumulating snapshot), `fact_claim`, `fact_audit_finding` (grain: invoice x rule).
- Flat summaries (BI-friendly): `carrier_scorecard`, `recoverable_by_month_carrier`, `late_delivery_by_lane`, `invoice_status_funnel`.

**Step 7 - Reconciliation, extended.** Add: Kafka bronze count vs producer manifest, silver vs gold totals
(sum of invoice amounts), fact rows vs silver rows, and finding recall vs the ground-truth file.

**Step 8 - Tests and CI/CD (BUILT, see docs/CICD.md).** pytest (pure functions + local Spark rules), `bundle validate`, deploy to test,
run an integration job, approval gate, deploy to prod. Pin action versions. No credentials in the repo.
