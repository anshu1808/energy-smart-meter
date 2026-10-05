--  *********************************************************************
--  Update Database Script
--  *********************************************************************
--  Change Log: changelog-master.yml
--  Ran at: 10/2/26, 5:14 AM
--  Against: null@offline:mysql?changeLogFile=/tmp/st.csv
--  Liquibase version: 4.32.0
--  *********************************************************************
--  Changeset changelogs/2026.09.001-create-audit-dbcr.yml::2026.09.001-create-audit-dbcr::anshu
--  Create DBCR audit table
CREATE TABLE energy_dev.dbcr (dbcr_id STRING NOT NULL, work_item_id STRING NULL, changeset_id STRING NULL, author STRING NULL, approvers STRING NULL, environment STRING NULL, operation STRING NULL, objects STRING NULL, is_destructive TINYINT NULL, is_rollback_available TINYINT NULL, checksum STRING NULL, git_commit STRING NULL, applied_at timestamp NULL, status STRING NULL);
--  Changeset changelogs/2026.10.100-create-bronze-raw-volume.yml::2026.10.100-create-bronze-raw-volume::anshu
--  WI-5 Create raw landing volume
CREATE VOLUME IF NOT EXISTS energy_dev.bronze.raw;
--  Changeset changelogs/2026.10.110-grants-data-engineer.yml::2026.10.110-grants-data-engineer::anshu
--  WI-5 Data engineer access to bronze/silver/gold
GRANT USE CATALOG ON CATALOG energy_dev TO `energy_data_engineer`;
GRANT USE SCHEMA ON SCHEMA energy_dev.bronze TO `energy_data_engineer`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.bronze TO `energy_data_engineer`;
GRANT USE SCHEMA ON SCHEMA energy_dev.silver TO `energy_data_engineer`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.silver TO `energy_data_engineer`;
GRANT USE SCHEMA ON SCHEMA energy_dev.gold TO `energy_data_engineer`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA energy_dev.gold TO `energy_data_engineer`;
--  Changeset changelogs/2026.10.111-grants-data-analyst.yml::2026.10.111-grants-data-analyst::anshu
--  WI-5 Data analyst read access to gold
GRANT USE CATALOG ON CATALOG energy_dev TO `energy_data_analyst`;
GRANT USE SCHEMA ON SCHEMA energy_dev.gold TO `energy_data_analyst`;
GRANT SELECT ON SCHEMA energy_dev.gold TO `energy_data_analyst`;
--  Changeset changelogs/2026.10.112-grants-hr.yml::2026.10.112-grants-hr::anshu
--  WI-5 HR read access to dim_customer
GRANT USE CATALOG ON CATALOG energy_dev TO `energy_hr`;
GRANT USE SCHEMA ON SCHEMA energy_dev.silver TO `energy_hr`;
GRANT SELECT ON TABLE energy_dev.silver.dim_customer TO `energy_hr`;
--  Changeset changelogs/2026.10.120-create-security-mask-functions.yml::2026.10.120-create-security-mask-functions::anshu
--  WI-5 Column mask functions
CREATE OR REPLACE FUNCTION energy_dev.security.mask_customer_id(customer_id STRING)
RETURN CASE WHEN is_account_group_member('energy_data_engineer') THEN customer_id ELSE '********' END;
CREATE OR REPLACE FUNCTION energy_dev.security.mask_lclid(lclid STRING)
RETURN CASE WHEN is_account_group_member('energy_data_engineer') THEN lclid ELSE '********' END;
CREATE OR REPLACE FUNCTION energy_dev.security.mask_financial_value(val DOUBLE)
RETURN CASE WHEN is_account_group_member('energy_data_engineer') OR is_account_group_member('energy_data_analyst') THEN val ELSE NULL END;
--  Changeset changelogs/2026.10.121-create-security-region-filter.yml::2026.10.121-create-security-region-filter::anshu
--  WI-5 Row filter function by region
CREATE OR REPLACE FUNCTION energy_dev.security.region_filter(region STRING)
RETURN CASE WHEN is_account_group_member('north_team') THEN region = 'NORTH'
            WHEN is_account_group_member('south_team') THEN region = 'SOUTH'
            ELSE TRUE END;
--  Changeset changelogs/2026.10.130-add-region-to-fact-consumption.yml::2026.10.130-add-region-to-fact-consumption::anshu
--  WI-5 Add region column to silver.fact_consumption
ALTER TABLE energy_dev.silver.fact_consumption ADD COLUMN region STRING;
--  Changeset changelogs/2026.10.131-add-region-to-fact-billing.yml::2026.10.131-add-region-to-fact-billing::anshu
--  WI-5 Add region column to silver.fact_billing
ALTER TABLE energy_dev.silver.fact_billing ADD COLUMN region STRING;
--  Changeset changelogs/2026.10.132-backfill-region-fact-consumption.yml::2026.10.132-backfill-region-fact-consumption::anshu
--  WI-5 One-time region backfill for silver.fact_consumption
MERGE INTO energy_dev.silver.fact_consumption AS f
USING energy_dev.silver.dim_customer AS dc
ON f.customer_key = dc.customer_key
WHEN MATCHED AND f.region IS NULL THEN UPDATE SET f.region = dc.region;
--  Changeset changelogs/2026.10.133-backfill-region-fact-billing.yml::2026.10.133-backfill-region-fact-billing::anshu
--  WI-5 One-time region backfill for silver.fact_billing
MERGE INTO energy_dev.silver.fact_billing AS f
USING energy_dev.silver.dim_customer AS dc
ON f.customer_key = dc.customer_key
WHEN MATCHED AND f.region IS NULL THEN UPDATE SET f.region = dc.region;
--  Changeset changelogs/2026.10.140-apply-column-masks.yml::2026.10.140-apply-column-masks::anshu
--  WI-5 Apply column masks to PII and financial columns
ALTER TABLE energy_dev.silver.dim_customer ALTER COLUMN customer_id SET MASK energy_dev.security.mask_customer_id;
ALTER TABLE energy_dev.silver.dim_customer ALTER COLUMN LCLid SET MASK energy_dev.security.mask_lclid;
ALTER TABLE energy_dev.silver.dim_meter ALTER COLUMN LCLid SET MASK energy_dev.security.mask_lclid;
ALTER TABLE energy_dev.silver.dim_tariff ALTER COLUMN rate_per_kwh SET MASK energy_dev.security.mask_financial_value;
ALTER TABLE energy_dev.silver.fact_billing ALTER COLUMN customer_id SET MASK energy_dev.security.mask_customer_id;
ALTER TABLE energy_dev.silver.fact_billing ALTER COLUMN bill_amount SET MASK energy_dev.security.mask_financial_value;
ALTER TABLE energy_dev.silver.fact_billing ALTER COLUMN rate_per_kwh SET MASK energy_dev.security.mask_financial_value;
ALTER TABLE energy_dev.gold.gold_theft_detection ALTER COLUMN LCLid SET MASK energy_dev.security.mask_lclid;
--  Changeset changelogs/2026.10.150-apply-region-row-filters.yml::2026.10.150-apply-region-row-filters::anshu
--  WI-5 Apply region row filters
ALTER TABLE energy_dev.silver.dim_customer SET ROW FILTER energy_dev.security.region_filter ON (region);
ALTER TABLE energy_dev.silver.dim_meter SET ROW FILTER energy_dev.security.region_filter ON (region);
ALTER TABLE energy_dev.silver.dim_tariff SET ROW FILTER energy_dev.security.region_filter ON (region);
ALTER TABLE energy_dev.silver.fact_consumption SET ROW FILTER energy_dev.security.region_filter ON (region);
ALTER TABLE energy_dev.silver.fact_billing SET ROW FILTER energy_dev.security.region_filter ON (region);
--  Changeset changelogs/2026.10.160-create-secure-revenue-view.yml::2026.10.160-create-secure-revenue-view::anshu
--  WI-5 Secure revenue summary view
CREATE OR REPLACE VIEW energy_dev.gold.v_revenue_summary_secure AS
SELECT
  billing_period, customer_count, meter_count, total_consumption_kwh,
  CASE WHEN is_account_group_member('energy_data_engineer') OR is_account_group_member('energy_data_analyst')
       THEN total_revenue ELSE NULL END AS total_revenue,
  avg_consumption_kwh,
  CASE WHEN is_account_group_member('energy_data_engineer') OR is_account_group_member('energy_data_analyst')
       THEN avg_bill_amount ELSE NULL END AS avg_bill_amount,
  load_ts
FROM energy_dev.gold.gold_revenue_summary;
--  Changeset changelogs/2026.10.161-grants-business-user.yml::2026.10.161-grants-business-user::anshu
--  WI-5 Business user access via secure view
GRANT USE CATALOG ON CATALOG energy_dev TO `energy_business_user`;
GRANT USE SCHEMA ON SCHEMA energy_dev.gold TO `energy_business_user`;
GRANT SELECT ON TABLE energy_dev.gold.v_revenue_summary_secure TO `energy_business_user`;
GRANT SELECT ON TABLE energy_dev.gold.gold_peak_load TO `energy_business_user`;
--  Changeset changelogs/2026.10.162-revoke-business-user-revenue-base-table.yml::2026.10.162-revoke-business-user-revenue-base-table::anshu
--  WI-5 Remove direct base-table revenue access from business users
REVOKE SELECT ON TABLE energy_dev.gold.gold_revenue_summary FROM `energy_business_user`;
--  Changeset changelogs/2026.10.170-enable-iceberg-compat-dim-tariff.yml::2026.10.170-enable-iceberg-compat-dim-tariff::anshu
--  WI-5 Iceberg compatibility on silver.dim_tariff
ALTER TABLE energy_dev.silver.dim_tariff SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
ALTER TABLE energy_dev.silver.dim_tariff SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
REORG TABLE energy_dev.silver.dim_tariff APPLY (PURGE);
ALTER TABLE energy_dev.silver.dim_tariff SET TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true');
