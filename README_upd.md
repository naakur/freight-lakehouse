# Freight Lakehouse - step by step

Databricks Free Edition project: batch (Auto Loader) + Kafka streaming -> bronze / silver / gold,
CDC + SCD2, quarantine, logging and reconciliation, deployed with Databricks Asset Bundles.

**This repo contains Steps 1-3.** Read `docs/ARCHITECTURE.md` first (10 minutes).

## What you have so far
| Step | What | Where |
|---|---|---|
| 1 | Bundle with dev/test/prod targets, environment setup job | `databricks.yml`, `resources/`, `notebooks/00_*` |
| 2 | Batch bronze with Auto Loader + file validation (bad files quarantined) | `bronze_batch.py`, `notebooks/01_*` |
| 3 | Silver typing + rules + row quarantine + run log + reconciliation | `silver_batch.py`, `utilities/entities.py`, `notebooks/90_*` |

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

# Create catalog and schema in free edition from bootstrap_environment.sql
# In Azure Databricks, the catalog and schema will be created via terraform
-- ---- dev ----
CREATE CATALOG IF NOT EXISTS freight_dev;
CREATE SCHEMA IF NOT EXISTS freight_dev.landing;
CREATE SCHEMA IF NOT EXISTS freight_dev.bronze;
CREATE SCHEMA IF NOT EXISTS freight_dev.silver;
CREATE SCHEMA IF NOT EXISTS freight_dev.gold;
CREATE SCHEMA IF NOT EXISTS freight_dev.quarantine;
CREATE SCHEMA IF NOT EXISTS freight_dev.ops;


databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run setup_environment -t dev
```
Check: catalog `freight_dev` has the volume `landing.raw` with `inbox/`, `batch/`, `quarantine_files/` folders,
and the three `ops` tables.


Copy the csv's to databricks

.\scripts\upload_data.ps1 -Catalog freight_dev -Dir "D:\path\to\your\csv\folder"

.\scripts\upload_data.ps1 -Catalog freight_dev -Dir "D:\Freight Audit Lakehouse Implementation\freight_audit_dataset\data\batch"

## if in case any error on above during authentication, use below
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\upload_data.ps1 -Catalog freight_dev -Dir "D:\Freight Audit Lakehouse Implementation\freight_audit_dataset\data\batch"


databricks bundle run batch_ingest -t dev

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
* invoices: bronze +6 (3082), silver +1 (3077), quarantine 5 (negative total, bad date, text weight, missing id, extra columns).
* Job ends with status WARN for preflight (files rejected) and green overall; the quarantine rate is below 5%.
* To see a reconciliation FAILURE: delete a row count by hand (e.g. truncate a silver table) and rerun only the audit task.

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
| `{{job.run_id}}` shows up literally | You ran the notebook by hand: that is fine, run_id defaults to "manual" |

What IS verified locally: the rules and typing run against the full generated dataset on Spark with ANSI mode on,
the bad sample rows fail the rules you expect, and the preflight file checks (13 tests). What is NOT verified:
anything that needs a Databricks workspace (bundle deploy, Auto Loader, pipeline run, the extra-columns rescue).

## Next steps
4 Kafka multiplex bronze + producer -> 5 CDC/SCD2 -> 6 gold facts/dims/summaries -> 7 extended reconciliation -> 8 CI/CD -> 9 observability -> 10 Azure port.
