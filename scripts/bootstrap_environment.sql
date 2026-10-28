-- =============================================================================
-- ONE-TIME BOOTSTRAP per environment. Run BEFORE the first `bundle deploy`.
-- Why: a pipeline can only be created if its catalog (and default schema) already
-- exist. In companies this is done by a platform team or Terraform, not by the app.
--
-- Run in the Databricks SQL editor (or a notebook). Repeat for test / prod when you get there.
-- =============================================================================

-- ---- dev ----
CREATE CATALOG IF NOT EXISTS freight_dev;
CREATE SCHEMA IF NOT EXISTS freight_dev.landing;
CREATE SCHEMA IF NOT EXISTS freight_dev.bronze;
CREATE SCHEMA IF NOT EXISTS freight_dev.silver;
CREATE SCHEMA IF NOT EXISTS freight_dev.gold;
CREATE SCHEMA IF NOT EXISTS freight_dev.quarantine;
CREATE SCHEMA IF NOT EXISTS freight_dev.ops;

-- ---- test (uncomment when you reach Step 8) ----
CREATE CATALOG IF NOT EXISTS freight_test;
CREATE SCHEMA IF NOT EXISTS freight_test.landing;
CREATE SCHEMA IF NOT EXISTS freight_test.bronze;
CREATE SCHEMA IF NOT EXISTS freight_test.silver;
CREATE SCHEMA IF NOT EXISTS freight_test.gold;
CREATE SCHEMA IF NOT EXISTS freight_test.quarantine;
CREATE SCHEMA IF NOT EXISTS freight_test.ops;

-- ---- prod (uncomment when you reach Step 8) ----
CREATE CATALOG IF NOT EXISTS freight_prod;
CREATE SCHEMA IF NOT EXISTS freight_prod.landing;
CREATE SCHEMA IF NOT EXISTS freight_prod.bronze;
CREATE SCHEMA IF NOT EXISTS freight_prod.silver;
CREATE SCHEMA IF NOT EXISTS freight_prod.gold;
CREATE SCHEMA IF NOT EXISTS freight_prod.quarantine;
CREATE SCHEMA IF NOT EXISTS freight_prod.ops;
