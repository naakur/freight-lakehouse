# Freight Lakehouse - step by step

Databricks Free Edition project: batch (Auto Loader) + Kafka streaming -> bronze / silver / gold,
CDC + SCD2, quarantine, logging and reconciliation, deployed with Databricks Asset Bundles.

**This repo contains Steps 1-8.** Read `docs/ARCHITECTURE.md` first (10 minutes).

## What you have so far
| Step | What | Where |
|---|---|---|
| 1 | Bundle with dev/test/prod targets, environment setup job | `databricks.yml`, `resources/`, `notebooks/00_*` |
| 2 | Batch bronze with Auto Loader + file validation (bad files quarantined) | `bronze_batch.py`, `notebooks/01_*` |
| 3 | Silver typing + rules + row quarantine + run log + reconciliation | `silver_batch.py`, `utilities/entities.py`, `notebooks/90_*` |
| 8 | CI/CD: GitHub Actions (lint, tests, secret scan, validate) -> deploy test -> approval -> deploy prod | `.github/workflows/`, `docs/CICD.md`, `scripts/seed_environment.sh` |
| 6 | Gold: dimensions, facts, flat summaries, audit findings (100% recall/precision vs ground truth), gold audit, end-to-end job | `utilities/gold_logic.py`, `transformations/gold/`, `notebooks/93_*` |
| 5 | CDC + SCD: carrier change feed -> SCD1 current table + SCD2 history table, late-arriving records, CDC audit | `cdc_carriers.py`, `sample_cdc/`, `notebooks/92_*`, `scripts/upload_cdc.ps1` |
| 4 | Multiplexed bronze (files or Kafka), per-topic silver, dedupe, quarantine, stream audit | `transformations/stream_*`, `utilities/topics.py`, `notebooks/91_*`, `scripts/kafka_producer.py` |

## Before you start (10 minutes, do not skip)
1. Install the Databricks CLI and log in: `databricks auth login` (use your Free Edition workspace URL).
2. Copy the CSVs from the earlier dataset zip (`data/batch/*.csv`) to a folder you can reference.
3. Run the tests locally. They prove the typing and rules on the real data (needs `pip install pytest pyspark`):
   ```
   FREIGHT_DATA_DIR=<path to data/batch> pytest tests/
   ```

## Step 1 - bootstrap, deploy and set up dev
```
# 0. ONE TIME: create the catalog + schemas (a pipeline cannot be created without them)
#    Open scripts/bootstrap_environment.sql in the Databricks SQL editor and run the "dev" block.

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run setup_environment -t dev
```
Check: catalog `freight_dev` has the volume `landing.raw` with `inbox/`, `batch/`, `quarantine_files/` folders,
and the three `ops` tables.

## Step 2 - first batch load (good files only)
```
scripts/upload_data.sh freight_dev <folder with the 6 CSVs>      # Windows: .\\scripts\\upload_data.ps1 -Catalog freight_dev -Dir <folder>
databricks bundle run batch_ingest -t dev
```
Expected after the run (all 3 tasks green):
| entity | bronze | silver | quarantine |
|---|---|---|---|
| carriers | 8 | 8 | 0 |
| rate_cards | 1728 | 1728 | 0 |
| accessorial_codes | 6 | 6 | 0 |
| gl_codes | 4 | 4 | 0 |
| shipments | 3000 | 3000 | 0 |
| invoices | 3076 | 3076 | 0 |

Run `docs/verify.sql` (change the catalog name). Run the job a second time: **nothing may change** (idempotency).

## Step 3 - break it on purpose (this is where you learn)
```
scripts/upload_data.sh freight_dev sample_inbox_bad --bad      # Windows: .\\scripts\\upload_data.ps1 -Catalog freight_dev -Dir .\\sample_inbox_bad -Bad
databricks bundle run batch_ingest -t dev
```
Expected:
* `ops.file_validation_log`: 4 files QUARANTINED (wrong header, .txt, empty, header-only), `invoices_bad_rows.csv` PASSED.
* invoices: bronze +6 (3082), silver +1 (3077), quarantine 5 (negative total, bad date, text weight, missing id, and the row with extra fields, which fails `row_well_formed`).
* Job ends with status WARN for preflight (files rejected) and green overall; the quarantine rate is below 5%.
* To see a reconciliation FAILURE: delete a row count by hand (e.g. truncate a silver table) and rerun only the audit task.


## Step 5 - CDC and SCD2 (carriers)

A CDC feed lists CHANGES (`op` = I/U/D and `change_ts`), not the whole table. Two AUTO CDC flows turn the same feed into:
* `gold.dim_carrier_current` - SCD type 1: one row per carrier, latest values, deletes remove the row
* `gold.dim_carrier` - SCD type 2: one row per VERSION, with `__START_AT` / `__END_AT` (NULL end = current)

The feed has a late record on purpose (a change that happened BEFORE one already processed), so you see history being rewritten.

```
databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev          # adds the inbox/carriers_cdc folder
powershell -ExecutionPolicy Bypass -File .\scripts\upload_cdc.ps1 -Catalog freight_dev -Wave A
databricks bundle run batch_ingest -t dev
```
Expected after **wave A** (files 1 and 2):
| table | rows |
|---|---|
| bronze / silver / quarantine `carriers_cdc` | 13 / 13 / 0 |
| `gold.dim_carrier_current` | 8 carriers (C001-C007 and C009; C008 was deleted) |
| `gold.dim_carrier` (history) | 12 versions, 8 still open (C008's version is closed on 2026-07-01) |
| C001 payment terms on 2026-03-01 (`docs/verify_cdc.sql` #3) | **NET30** |

```
powershell -ExecutionPolicy Bypass -File .\scripts\upload_cdc.ps1 -Catalog freight_dev -Wave B
databricks bundle run batch_ingest -t dev
```
Expected after **wave B** (file 3: a late record, a newer update, 2 broken rows):
| table | rows |
|---|---|
| bronze / silver / quarantine `carriers_cdc` | 17 / 15 / 2 |
| `gold.dim_carrier_current` | 8 |
| `gold.dim_carrier` | 14 versions, 8 open; C001 now has NET30, NET40, NET45, NET60 |
| C001 payment terms on 2026-03-01 | **NET40** (the late record rewrote history) |
| quarantine | C006 (`op_is_I_U_or_D`), C007 (`change_ts_valid`) |

The batch audit will show a **WARN** for `quarantine_rate / carriers_cdc` after wave B (2 of 17 rows is about 12%, above the 5% threshold).
That is correct behavior. The `audit_cdc` task (4 independent checks) must PASS.

**Definition of done:** numbers above match, `audit_cdc` passes, re-running with no new files changes nothing,
and you can explain the difference between SCD1 and SCD2 and why `sequence_by` matters for late records.
Local reference: `pytest tests/test_cdc.py` computes the same version counts in plain Python.


## Step 6 - Gold: dimensions, facts, flat summaries, audit findings

Everything is a materialized view (Spark recomputes it from silver). The logic is in `utilities/gold_logic.py`,
so it is tested on a laptop BEFORE you deploy. On the generated data the audit rules find every injected error with no false alarms:

| rule | injected errors | found | recall | precision |
|---|---|---|---|---|
| OVERCHARGE_RATE | 66 | 66 | 100% | 100% |
| FUEL_VARIANCE | 73 | 73 | 100% | 100% |
| WEIGHT_MISMATCH | 68 | 68 | 100% | 100% |
| UNAUTHORIZED_ACCESSORIAL | 78 | 78 | 100% | 100% |
| DUPLICATE_INVOICE | 76 | 76 | 100% | 100% |

| Layer | Tables |
|---|---|
| Dimensions | `dim_carrier` (SCD2) and `dim_carrier_current` from Step 5, plus `dim_rate_card`, `dim_gl_code`, `dim_accessorial`, `dim_date` |
| Facts | `fact_invoice` (carrier attributes as of the invoice date), `fact_shipment_delivery` (accumulating snapshot), `fact_audit_finding` (invoice x rule), `fact_claim` |
| Flat summaries | `carrier_scorecard`, `recoverable_by_month`, `late_delivery_by_lane`, `invoice_status_funnel` |

```
# one-time: upload the ground-truth labels (from the dataset zip, folder truth/)
databricks fs cp "D:\...\freight_audit_dataset\data\truth\invoice_anomaly_labels.csv" dbfs:/Volumes/freight_dev/landing/raw/truth/invoice_anomaly_labels.csv

databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev      # adds the truth folder
databricks bundle run gold_build -t dev             # gold pipeline + gold audit
```
(If the `truth` folder is missing, create it by re-running setup first.) Run `docs/verify_gold.sql` afterwards.

Expected row counts: `fact_invoice` 3,077 (3,076 + the 1 practice invoice that passes silver; the malformed practice row is quarantined), `fact_shipment_delivery` 3,000,
`fact_claim` 187, `dim_date` 365, `dim_rate_card` 1,728, `carrier_scorecard` 8.
Findings per rule on the real invoices (rows starting `INV99` are practice data and are excluded from the audit): 66 / 73 / 68 / 78 / 76.
The `audit_gold` task must PASS every check, including recall and precision >= 99% for each rule.
Without the truth file the audit shows a WARN `ground_truth_file_present` and skips the rule scoring: upload the file and re-run.
One finding on the practice invoice `INV99000001` is expected (e.g. OVERCHARGE_RATE shows 67, not 66).

`freight_end_to_end` runs batch + stream in parallel, then gold: `databricks bundle run freight_end_to_end -t dev`.

**Definition of done:** `gold_build` green, the numbers above match, and you can explain: why a fact has a grain,
why `fact_invoice` uses the carrier version valid on the invoice date, and what recall and precision mean for an audit rule.


## Step 8 - CI/CD and promotion to test and prod

Full instructions: **`docs/CICD.md`** (one-time setup is about 45 minutes). In short:
1. Run the test and prod blocks of `scripts/bootstrap_environment.sql`.
2. `databricks bundle validate/deploy -t test` by hand once, then `-t prod` validate.
3. Push to a private GitHub repo, add the `test` and `production` environments with the two Databricks secrets
   (the production environment requires your approval), protect `main`.
4. Open a pull request: CI runs lint, the tests, a secret scan and `bundle validate` for all three targets.
   Merge: the test environment is deployed, seeded and the whole platform runs with all audits; after your approval prod is deployed.

What is different from the sample project you shared: pinned action versions (exact commits), no prod deploy without tests,
a test environment and an approval gate, workspace host in secrets not in the repo, secret scanning, and a seed script
that is safe to run twice. Lint: `ruff check .` (also runs in pre-commit and CI).

### Lesson from the first real run
`_rescued_data` does NOT catch a row that has more fields than the header, Spark silently drops the extras.
The audit still passed (every row was accounted for), because reconciliation proves *nothing was lost*, not that
*everything accepted is correct*. The fix is the `_corrupt_record` column in bronze plus the `row_well_formed` rule.
After pulling this version run `databricks bundle deploy -t dev`, then re-run the pipeline with a **full refresh**
(`databricks bundle run freight_batch_pipeline -t dev --full-refresh-all`) so existing rows get the new column.


## Step 4 - Streaming: multiplexed bronze, per-topic silver

The streaming flow has two interchangeable sources for the SAME bronze table (`bronze.kafka_multiplex`):

| `stream_source` | Reads from | Use for |
|---|---|---|
| `files` (default) | Kafka-style JSON files in the `stream/` folder, via Auto Loader | building and testing everything now, no Kafka account needed |
| `kafka` | Confluent Cloud topics | the real thing; optional, do it later (Step 4B) |

Silver, gold and the audits are identical for both. Switching is one variable, not a rewrite.

### Step 4A - file source (do this now)
```
databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev       # safe to re-run, adds the stream folders

# 1) send the first 2 files
powershell -ExecutionPolicy Bypass -File .\scripts\upload_stream.ps1 -Catalog freight_dev -Dir "D:\...\freight_audit_dataset\data\stream" -Files 2
databricks bundle run stream_ingest -t dev

# 2) send the other 18 files (arrival in a second wave)
powershell -ExecutionPolicy Bypass -File .\scripts\upload_stream.ps1 -Catalog freight_dev -Dir "D:\...\freight_audit_dataset\data\stream" -Skip 2
databricks bundle run stream_ingest -t dev

# 3) run again with nothing new: counts must not change
databricks bundle run stream_ingest -t dev
```
Expected after step 2 (all 24,909 messages):
| topic | bronze | silver | quarantine |
|---|---|---|---|
| freight.tracking.events | 15,463 | 15,123 | 41 |
| freight.invoice.status | 9,193 | 8,980 | 26 |
| freight.rate.changes | 62 | 60 | 0 |
| freight.claims.filed | 191 | 187 | 0 |

Silver is lower than bronze minus quarantine because duplicate deliveries are removed. Check with `docs/verify_stream.sql`.
After wave 1 the numbers are smaller, and the audit must already pass for that smaller set (it counts the files).

**Definition of done**
- [ ] bronze = lines in the files for every topic (the audit proves it)
- [ ] silver and quarantine match the table; quarantine holds the original broken text
- [ ] wave 2 adds only the new messages, the empty third run changes nothing
- [ ] you can explain: why one multiplexed bronze table, why dedupe needs a window, what "at least once" means

**If the stream pipeline fails with `SQL config "freight.xxx" cannot be found`:** a setting is missing from
`resources/pipelines.yml`. `pytest tests/test_bundle_config.py` finds this kind of mistake before you deploy.

### Step 4B - real Kafka (optional, later)
Needs a Confluent cluster and API key. Nothing downstream changes.
1. `scripts/setup_secrets.ps1` stores the credentials in the secret scope `freight-kafka`.
2. Producer: `python scripts/kafka_producer.py --data-dir <stream folder> --dry-run`, then `--files 2`, then `--skip-files 2`.
3. Smoke test: notebook `04_kafka_smoke_test` (checks secrets, network and the topics).
4. Upload the producer manifests to `stream_manifest/`, set the variable `stream_source` to `kafka`
   (`databricks bundle deploy -t dev --var stream_source=kafka`), and run `stream_ingest`.
Whether Free Edition can reach Confluent is the one thing this repo cannot guarantee: the smoke test tells you in a minute.


## Step 5 - CDC and SCD2 (carriers)

A CDC feed lists CHANGES (`op` = I/U/D and `change_ts`), not the whole table. Two AUTO CDC flows turn the same feed into:
* `gold.dim_carrier_current` - SCD type 1: one row per carrier, latest values, deletes remove the row
* `gold.dim_carrier` - SCD type 2: one row per VERSION, with `__START_AT` / `__END_AT` (NULL end = current)

The feed has a late record on purpose (a change that happened BEFORE one already processed), so you see history being rewritten.

```
databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev          # adds the inbox/carriers_cdc folder
powershell -ExecutionPolicy Bypass -File .\scripts\upload_cdc.ps1 -Catalog freight_dev -Wave A
databricks bundle run batch_ingest -t dev
```
Expected after **wave A** (files 1 and 2):
| table | rows |
|---|---|
| bronze / silver / quarantine `carriers_cdc` | 13 / 13 / 0 |
| `gold.dim_carrier_current` | 8 carriers (C001-C007 and C009; C008 was deleted) |
| `gold.dim_carrier` (history) | 12 versions, 8 still open (C008's version is closed on 2026-07-01) |
| C001 payment terms on 2026-03-01 (`docs/verify_cdc.sql` #3) | **NET30** |

```
powershell -ExecutionPolicy Bypass -File .\scripts\upload_cdc.ps1 -Catalog freight_dev -Wave B
databricks bundle run batch_ingest -t dev
```
Expected after **wave B** (file 3: a late record, a newer update, 2 broken rows):
| table | rows |
|---|---|
| bronze / silver / quarantine `carriers_cdc` | 17 / 15 / 2 |
| `gold.dim_carrier_current` | 8 |
| `gold.dim_carrier` | 14 versions, 8 open; C001 now has NET30, NET40, NET45, NET60 |
| C001 payment terms on 2026-03-01 | **NET40** (the late record rewrote history) |
| quarantine | C006 (`op_is_I_U_or_D`), C007 (`change_ts_valid`) |

The batch audit will show a **WARN** for `quarantine_rate / carriers_cdc` after wave B (2 of 17 rows is about 12%, above the 5% threshold).
That is correct behavior. The `audit_cdc` task (4 independent checks) must PASS.

**Definition of done:** numbers above match, `audit_cdc` passes, re-running with no new files changes nothing,
and you can explain the difference between SCD1 and SCD2 and why `sequence_by` matters for late records.
Local reference: `pytest tests/test_cdc.py` computes the same version counts in plain Python.


## Step 6 - Gold: dimensions, facts, flat summaries, audit findings

Everything is a materialized view (Spark recomputes it from silver). The logic is in `utilities/gold_logic.py`,
so it is tested on a laptop BEFORE you deploy. On the generated data the audit rules find every injected error with no false alarms:

| rule | injected errors | found | recall | precision |
|---|---|---|---|---|
| OVERCHARGE_RATE | 66 | 66 | 100% | 100% |
| FUEL_VARIANCE | 73 | 73 | 100% | 100% |
| WEIGHT_MISMATCH | 68 | 68 | 100% | 100% |
| UNAUTHORIZED_ACCESSORIAL | 78 | 78 | 100% | 100% |
| DUPLICATE_INVOICE | 76 | 76 | 100% | 100% |

| Layer | Tables |
|---|---|
| Dimensions | `dim_carrier` (SCD2) and `dim_carrier_current` from Step 5, plus `dim_rate_card`, `dim_gl_code`, `dim_accessorial`, `dim_date` |
| Facts | `fact_invoice` (carrier attributes as of the invoice date), `fact_shipment_delivery` (accumulating snapshot), `fact_audit_finding` (invoice x rule), `fact_claim` |
| Flat summaries | `carrier_scorecard`, `recoverable_by_month`, `late_delivery_by_lane`, `invoice_status_funnel` |

```
# one-time: upload the ground-truth labels (from the dataset zip, folder truth/)
databricks fs cp "D:\...\freight_audit_dataset\data\truth\invoice_anomaly_labels.csv" dbfs:/Volumes/freight_dev/landing/raw/truth/invoice_anomaly_labels.csv

databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev      # adds the truth folder
databricks bundle run gold_build -t dev             # gold pipeline + gold audit
```
(If the `truth` folder is missing, create it by re-running setup first.) Run `docs/verify_gold.sql` afterwards.

Expected row counts: `fact_invoice` 3,077 (3,076 + the 1 practice invoice that passes silver; the malformed practice row is quarantined), `fact_shipment_delivery` 3,000,
`fact_claim` 187, `dim_date` 365, `dim_rate_card` 1,728, `carrier_scorecard` 8.
Findings per rule on the real invoices (rows starting `INV99` are practice data and are excluded from the audit): 66 / 73 / 68 / 78 / 76.
The `audit_gold` task must PASS every check, including recall and precision >= 99% for each rule.
Without the truth file the audit shows a WARN `ground_truth_file_present` and skips the rule scoring: upload the file and re-run.
One finding on the practice invoice `INV99000001` is expected (e.g. OVERCHARGE_RATE shows 67, not 66).

`freight_end_to_end` runs batch + stream in parallel, then gold: `databricks bundle run freight_end_to_end -t dev`.

**Definition of done:** `gold_build` green, the numbers above match, and you can explain: why a fact has a grain,
why `fact_invoice` uses the carrier version valid on the invoice date, and what recall and precision mean for an audit rule.


## Step 8 - CI/CD and promotion to test and prod

Full instructions: **`docs/CICD.md`** (one-time setup is about 45 minutes). In short:
1. Run the test and prod blocks of `scripts/bootstrap_environment.sql`.
2. `databricks bundle validate/deploy -t test` by hand once, then `-t prod` validate.
3. Push to a private GitHub repo, add the `test` and `production` environments with the two Databricks secrets
   (the production environment requires your approval), protect `main`.
4. Open a pull request: CI runs lint, the tests, a secret scan and `bundle validate` for all three targets.
   Merge: the test environment is deployed, seeded and the whole platform runs with all audits; after your approval prod is deployed.

What is different from the sample project you shared: pinned action versions (exact commits), no prod deploy without tests,
a test environment and an approval gate, workspace host in secrets not in the repo, secret scanning, and a seed script
that is safe to run twice. Lint: `ruff check .` (also runs in pre-commit and CI).

### Lesson from the first real run
`_rescued_data` does NOT catch a row that has more fields than the header, Spark silently drops the extras.
The audit still passed (every row was accounted for), because reconciliation proves *nothing was lost*, not that
*everything accepted is correct*. The fix is the `_corrupt_record` column in bronze plus the `row_well_formed` rule.
After pulling this version run `databricks bundle deploy -t dev`, then re-run the pipeline with a **full refresh**
(`databricks bundle run freight_batch_pipeline -t dev --full-refresh-all`) so existing rows get the new column.


## Step 4 - Kafka streaming (do the sub-steps in order; each one proves one thing)

**4.1 Secrets** - credentials live in a Databricks secret scope, never in the repo.
```
powershell -ExecutionPolicy Bypass -File .\scripts\setup_secrets.ps1
```
Stores `kafka-bootstrap`, `kafka-api-key`, `kafka-api-secret` in scope `freight-kafka`. Use a NEW Confluent API key.

**4.2 Producer, small first** (`pip install -r scripts/requirements-producer.txt`):
```
$env:KAFKA_BOOTSTRAP="..."; $env:KAFKA_API_KEY="..."; $env:KAFKA_API_SECRET="..."
python scripts/kafka_producer.py --data-dir "D:\...\freight_audit_dataset\data\stream" --dry-run     # counts only
python scripts/kafka_producer.py --data-dir "D:\...\freight_audit_dataset\data\stream" --files 2     # 2 files for real
```
Check in the Confluent UI that messages appear in the topics.

**4.3 Smoke test** - re-deploy (`bundle deploy -t dev`), then open the notebook `04_kafka_smoke_test` in the workspace
(`/Workspace/Users/<you>/.bundle/freight_lakehouse/dev/files/notebooks/`). Set the widget `src_path` to
`/Workspace/Users/<you>/.bundle/freight_lakehouse/dev/files/src/freight_etl` and run all cells.
It must print the 3 secret names and a count per topic. If it cannot connect, STOP here and send me the error:
this is the one thing I cannot verify for you (Free Edition -> Confluent network access).

**4.4 Run the pipeline on the 2 files**
```
databricks bundle run setup_environment -t dev          # adds the stream_manifest folder (safe to re-run)
databricks bundle deploy -t dev
databricks bundle run stream_ingest -t dev
```
The audit will WARN (no manifest uploaded yet) - that is expected. Upload the manifest and re-run the audit later.

**4.5 Send the rest, then reconcile**
```
python scripts/kafka_producer.py --data-dir "D:\...\data\stream" --skip-files 2      # the other 18 files
databricks fs cp manifests\<manifest file 1> dbfs:/Volumes/freight_dev/landing/raw/stream_manifest/
databricks fs cp manifests\<manifest file 2> dbfs:/Volumes/freight_dev/landing/raw/stream_manifest/
databricks bundle run stream_ingest -t dev
```
The two manifests together add up to all 24,909 messages. Expected after this run:
| topic | bronze | silver | quarantine |
|---|---|---|---|
| freight.tracking.events | 15,463 | 15,123 | 41 |
| freight.invoice.status | 9,193 | 8,980 | 26 |
| freight.rate.changes | 62 | 60 | 0 |
| freight.claims.filed | 191 | 187 | 0 |

Silver is lower than bronze minus quarantine because duplicate deliveries are removed.
Then run `stream_ingest` once more with no new messages: counts must not change (restart safety).

**Verify** with `docs/verify_stream.sql`. Definition of done for Step 4:
- [ ] bronze = messages delivered (manifest) for every topic
- [ ] silver + dedupe numbers match the table; quarantine holds the broken messages with their original text
- [ ] a second run with no new data changes nothing
- [ ] no credential appears anywhere in the repo (`git grep -i "api"` shows only variable names)

## Definition of done for Steps 1-3
- [ ] `bundle deploy -t dev` and `-t test` both work (use `setup_environment -t test` first)
- [ ] Table above matches; second run changes nothing
- [ ] You can explain, in one sentence each: why bronze is all strings, why we use `try_cast`, what `bronze = silver + quarantine` guarantees
- [ ] Commit with a clear message; tag `v0.1.0`

## If something fails (known uncertainties - I could not run Databricks from here)
| Symptom | Likely cause / fix |
|---|---|
| `bundle validate`: unknown field `glob` or `root_path` | Your CLI is older: update it (`databricks --version`). Fallback: list files under `libraries: - file: path:` |
| `CREATE CATALOG` refused | Use one catalog and prefix schemas (`dev_bronze`...). Record it as ADR-001 |
| Deploy says catalog or schema does not exist | Run the bootstrap SQL first (Step 1, item 0) |
| `from utilities...` import error in a pipeline | Confirm `root_path` is `src/freight_etl`; check the pipeline's source folder in the UI |
| `mode: production` complains about permissions/run_as | Add `permissions:` (CAN_MANAGE for you) under the target; on Free Edition `run_as` stays you |
| `_rescued_data` missing in bronze | Confirm the `rescuedDataColumn` option is on the Auto Loader read |
| notebook task asks for an environment | Keep the `environments` block in `resources/jobs.yml` (copied from a working sample) |
| `materialized_view` not found | Older naming: use `@dp.table` with the same body (a batch read makes it a materialized view). Send me the error |
| `create_auto_cdc_flow` or `temporary_view` not found | Older runtime naming: try `dp.apply_changes` (same arguments) and `@dp.view`. Send me the error |
| `{{job.run_id}}` shows up literally | You ran the notebook by hand: that is fine, run_id defaults to "manual" |

What IS verified locally: the rules and typing run against the full generated dataset on Spark with ANSI mode on,
the bad sample rows fail the rules you expect, and the preflight file checks (13 tests). What is NOT verified:
anything that needs a Databricks workspace (bundle deploy, Auto Loader, pipeline run, the extra-columns rescue).

## Next steps
4 Kafka multiplex bronze + producer -> 5 CDC/SCD2 -> 6 gold facts/dims/summaries -> 7 extended reconciliation -> 8 CI/CD -> 9 observability -> 10 Azure port.
