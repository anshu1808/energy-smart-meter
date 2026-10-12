-- Energy Smart Meter: Unity Catalog setup and permissions reference
-- IMPORTANT:
-- 1) This is a reference/runbook, NOT an automatically executed DAB file.
-- 2) Run only the block for the intended environment.
-- 3) Durable DDL and grants should be moved into Liquibase changelogs under sql/ddl/changelogs/.
-- 4) Run as a Unity Catalog admin/owner authorized to grant privileges.
-- 5) Production catalog in this project is `energy` (not `energy_prod`).

-- ============================================================
-- A. ONE-TIME CATALOG/SCHEMA/VOLUME PREREQUISITES
-- ============================================================
-- Catalog creation may require a metastore admin. Do not run these
-- from the pipeline service principal unless it has been deliberately
-- granted catalog-creation privileges.

-- DEV
CREATE SCHEMA IF NOT EXISTS energy_dev.liquibase;
CREATE SCHEMA IF NOT EXISTS energy_dev.bronze;
CREATE SCHEMA IF NOT EXISTS energy_dev.silver;
CREATE SCHEMA IF NOT EXISTS energy_dev.gold;
CREATE SCHEMA IF NOT EXISTS energy_dev.audit;
CREATE SCHEMA IF NOT EXISTS energy_dev.security;
CREATE VOLUME IF NOT EXISTS energy_dev.bronze.raw;

-- QA
CREATE SCHEMA IF NOT EXISTS energy_qa.liquibase;
CREATE SCHEMA IF NOT EXISTS energy_qa.bronze;
CREATE SCHEMA IF NOT EXISTS energy_qa.silver;
CREATE SCHEMA IF NOT EXISTS energy_qa.gold;
CREATE SCHEMA IF NOT EXISTS energy_qa.audit;
CREATE SCHEMA IF NOT EXISTS energy_qa.security;
CREATE VOLUME IF NOT EXISTS energy_qa.bronze.raw;

-- PROD (project target maps to catalog `energy`)
CREATE SCHEMA IF NOT EXISTS energy.liquibase;
CREATE SCHEMA IF NOT EXISTS energy.bronze;
CREATE SCHEMA IF NOT EXISTS energy.silver;
CREATE SCHEMA IF NOT EXISTS energy.gold;
CREATE SCHEMA IF NOT EXISTS energy.audit;
CREATE SCHEMA IF NOT EXISTS energy.security;
CREATE VOLUME IF NOT EXISTS energy.bronze.raw;


-- ============================================================
-- B. DAB RUNTIME SERVICE PRINCIPAL PERMISSIONS
-- ============================================================
-- These grants are for job runtime. They are not admin permissions.
-- Run after the listed schemas and raw volume exist.

-- ---------------- DEV: Energy Smart Meter DEV ----------------
GRANT USE CATALOG ON CATALOG energy_dev
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;

GRANT USE SCHEMA ON SCHEMA energy_dev.bronze
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.bronze
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;

GRANT USE SCHEMA ON SCHEMA energy_dev.silver
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.silver
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;

GRANT USE SCHEMA ON SCHEMA energy_dev.gold
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.gold
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;

GRANT USE SCHEMA ON SCHEMA energy_dev.audit
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.audit
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME energy_dev.bronze.raw
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;

-- Explicit table grant helps diagnose the currently reported audit-table error.
-- Run only after energy_dev.audit.pipeline_lineage exists.
GRANT SELECT, MODIFY ON TABLE energy_dev.audit.pipeline_lineage
TO `5344eedb-7784-40a2-be58-21e9c59b260e`;


-- ---------------- QA: Energy Smart Meter QA ------------------
GRANT USE CATALOG ON CATALOG energy_qa
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;

GRANT USE SCHEMA ON SCHEMA energy_qa.bronze
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_qa.bronze
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;

GRANT USE SCHEMA ON SCHEMA energy_qa.silver
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_qa.silver
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;

GRANT USE SCHEMA ON SCHEMA energy_qa.gold
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_qa.gold
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;

GRANT USE SCHEMA ON SCHEMA energy_qa.audit
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_qa.audit
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME energy_qa.bronze.raw
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;

-- Run after energy_qa.audit.pipeline_lineage exists.
GRANT SELECT, MODIFY ON TABLE energy_qa.audit.pipeline_lineage
TO `cc98e027-7ed0-4567-99f4-2d0b7eb3a2e3`;


-- ---------------- PROD: Energy Smart Meter PROD ----------------
GRANT USE CATALOG ON CATALOG energy
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;

GRANT USE SCHEMA ON SCHEMA energy.bronze
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy.bronze
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;

GRANT USE SCHEMA ON SCHEMA energy.silver
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy.silver
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;

GRANT USE SCHEMA ON SCHEMA energy.gold
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy.gold
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;

GRANT USE SCHEMA ON SCHEMA energy.audit
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy.audit
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME energy.bronze.raw
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;

-- Run after energy.audit.pipeline_lineage exists.
GRANT SELECT, MODIFY ON TABLE energy.audit.pipeline_lineage
TO `4ee2288b-500d-45c2-99a1-846da9a8312a`;


-- ============================================================
-- C. HUMAN GROUP PERMISSIONS (DEV EXAMPLE)
-- ============================================================
-- Repeat for QA/PROD only if these groups should have access there.
GRANT USE CATALOG ON CATALOG energy_dev TO `energy_data_engineer`;
GRANT USE SCHEMA ON SCHEMA energy_dev.bronze TO `energy_data_engineer`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.bronze TO `energy_data_engineer`;
GRANT USE SCHEMA ON SCHEMA energy_dev.silver TO `energy_data_engineer`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.silver TO `energy_data_engineer`;
GRANT USE SCHEMA ON SCHEMA energy_dev.gold TO `energy_data_engineer`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.gold TO `energy_data_engineer`;

GRANT USE CATALOG ON CATALOG energy_dev TO `energy_data_analyst`;
GRANT USE SCHEMA ON SCHEMA energy_dev.gold TO `energy_data_analyst`;
GRANT SELECT ON SCHEMA energy_dev.gold TO `energy_data_analyst`;

GRANT USE CATALOG ON CATALOG energy_dev TO `energy_business_user`;
GRANT USE SCHEMA ON SCHEMA energy_dev.gold TO `energy_business_user`;
GRANT SELECT ON TABLE energy_dev.gold.v_revenue_summary_secure TO `energy_business_user`;
GRANT SELECT ON TABLE energy_dev.gold.gold_peak_load TO `energy_business_user`;

GRANT USE CATALOG ON CATALOG energy_dev TO `energy_hr`;
GRANT USE SCHEMA ON SCHEMA energy_dev.silver TO `energy_hr`;
GRANT SELECT ON TABLE energy_dev.silver.dim_customer TO `energy_hr`;


-- ============================================================
-- D. SECURITY FUNCTIONS AND SECURE VIEW (DEV EXAMPLE)
-- ============================================================
-- Confirm the referenced tables/columns exist before applying masks/filters.
-- These are schema migrations and should be versioned in Liquibase.

CREATE OR REPLACE FUNCTION energy_dev.security.mask_customer_id(customer_id STRING)
RETURN CASE
  WHEN is_account_group_member('energy_data_engineer') THEN customer_id
  ELSE '********'
END;

CREATE OR REPLACE FUNCTION energy_dev.security.mask_lclid(lclid STRING)
RETURN CASE
  WHEN is_account_group_member('energy_data_engineer') THEN lclid
  ELSE '********'
END;

CREATE OR REPLACE FUNCTION energy_dev.security.mask_financial_value(val DOUBLE)
RETURN CASE
  WHEN is_account_group_member('energy_data_engineer')
    OR is_account_group_member('energy_data_analyst') THEN val
  ELSE NULL
END;

CREATE OR REPLACE FUNCTION energy_dev.security.region_filter(region STRING)
RETURN CASE
  WHEN is_account_group_member('north_team') THEN region = 'NORTH'
  WHEN is_account_group_member('south_team') THEN region = 'SOUTH'
  ELSE TRUE
END;

-- Add region only if not already present. If your Databricks SQL runtime does
-- not support IF NOT EXISTS for ADD COLUMN, keep this as a one-time migration.
ALTER TABLE energy_dev.silver.fact_consumption ADD COLUMN region STRING;
ALTER TABLE energy_dev.silver.fact_billing ADD COLUMN region STRING;

MERGE INTO energy_dev.silver.fact_consumption AS f
USING energy_dev.silver.dim_customer AS dc
ON f.customer_key = dc.customer_key
WHEN MATCHED AND f.region IS NULL THEN UPDATE SET f.region = dc.region;

MERGE INTO energy_dev.silver.fact_billing AS f
USING energy_dev.silver.dim_customer AS dc
ON f.customer_key = dc.customer_key
WHEN MATCHED AND f.region IS NULL THEN UPDATE SET f.region = dc.region;

-- Apply only where the named columns exist and the executing migration identity
-- owns/manages the tables. The dim_meter/dim_tariff region columns must be checked.
ALTER TABLE energy_dev.silver.dim_customer
  ALTER COLUMN customer_id SET MASK energy_dev.security.mask_customer_id;
ALTER TABLE energy_dev.silver.dim_customer
  ALTER COLUMN LCLid SET MASK energy_dev.security.mask_lclid;
ALTER TABLE energy_dev.silver.dim_meter
  ALTER COLUMN LCLid SET MASK energy_dev.security.mask_lclid;
ALTER TABLE energy_dev.silver.dim_tariff
  ALTER COLUMN rate_per_kwh SET MASK energy_dev.security.mask_financial_value;
ALTER TABLE energy_dev.silver.fact_billing
  ALTER COLUMN customer_id SET MASK energy_dev.security.mask_customer_id;
ALTER TABLE energy_dev.silver.fact_billing
  ALTER COLUMN bill_amount SET MASK energy_dev.security.mask_financial_value;
ALTER TABLE energy_dev.silver.fact_billing
  ALTER COLUMN rate_per_kwh SET MASK energy_dev.security.mask_financial_value;
ALTER TABLE energy_dev.gold.gold_theft_detection
  ALTER COLUMN LCLid SET MASK energy_dev.security.mask_lclid;

-- Only apply a row filter if every target table has the `region` column.
ALTER TABLE energy_dev.silver.dim_customer
  SET ROW FILTER energy_dev.security.region_filter ON (region);
ALTER TABLE energy_dev.silver.fact_consumption
  SET ROW FILTER energy_dev.security.region_filter ON (region);
ALTER TABLE energy_dev.silver.fact_billing
  SET ROW FILTER energy_dev.security.region_filter ON (region);
-- Add filters to dim_meter/dim_tariff only after confirming those tables have region.

CREATE OR REPLACE VIEW energy_dev.gold.v_revenue_summary_secure AS
SELECT
  billing_period,
  customer_count,
  meter_count,
  total_consumption_kwh,
  CASE
    WHEN is_account_group_member('energy_data_engineer')
      OR is_account_group_member('energy_data_analyst')
    THEN total_revenue
    ELSE NULL
  END AS total_revenue,
  avg_consumption_kwh,
  CASE
    WHEN is_account_group_member('energy_data_engineer')
      OR is_account_group_member('energy_data_analyst')
    THEN avg_bill_amount
    ELSE NULL
  END AS avg_bill_amount,
  load_ts
FROM energy_dev.gold.gold_revenue_summary;

-- Iceberg compatibility example. Run only if this feature is required and supported
-- by the workspace/runtime. REORG can be expensive and should be planned separately.
ALTER TABLE energy_dev.silver.dim_tariff
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
ALTER TABLE energy_dev.silver.dim_tariff
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
REORG TABLE energy_dev.silver.dim_tariff APPLY (PURGE);
ALTER TABLE energy_dev.silver.dim_tariff
  SET TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true');


-- ============================================================
-- E. DIAGNOSTIC QUERIES (READ-ONLY; NOT MIGRATIONS)
-- ============================================================
SHOW SCHEMAS IN energy_dev;
SHOW VOLUMES IN energy_dev.bronze;
SHOW GRANTS ON CATALOG energy_dev;
SHOW GRANTS ON SCHEMA energy_dev.audit;
SHOW GRANTS ON TABLE energy_dev.audit.pipeline_lineage;
DESCRIBE HISTORY energy_dev.silver.fact_consumption;

-- Do NOT put CLI commands such as `databricks secrets create-scope` in this SQL file.
-- Secret scopes/ACLs are managed with the Databricks CLI or workspace UI.
-- Do NOT create pipeline_lineage as an empty table: its columns must match the
-- audit task and the get_last_pipeline_start_ts() code.
