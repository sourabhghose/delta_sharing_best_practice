# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Recipient Best Practices (Recipient Workspace)
# MAGIC
# MAGIC **Reference notebook — read and run individual cells as needed. Most cells are commented out to prevent accidental execution.**
# MAGIC
# MAGIC This notebook is a production readiness guide for teams operating a Delta Sharing
# MAGIC recipient environment. It covers every aspect of governance, security, performance,
# MAGIC and operations for the `delta_sharing_demo` medallion pipeline.
# MAGIC
# MAGIC Topics covered:
# MAGIC 1. **Access control** — GRANT patterns, row-level security filters, column masks
# MAGIC 2. **Performance** — compute sizing, caching shared tables, partition pruning, predicate pushdown
# MAGIC 3. **Unity Catalog governance** — data lineage queries, audit logs, classification tags
# MAGIC 4. **Monitoring dashboard** — recommended SQL tiles for pipeline observability
# MAGIC 5. **Operational runbook** — step-by-step responses to common production incidents

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Access Control
# MAGIC
# MAGIC Unity Catalog provides fine-grained access control on shared data.
# MAGIC Apply the **principle of least privilege** — grant only what each team needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema-Level Grants

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant read access to the analytics team on gold layer
# MAGIC -- GRANT USAGE ON CATALOG delta_sharing_demo TO `analytics-team`;
# MAGIC -- GRANT USAGE ON SCHEMA delta_sharing_demo.gold TO `analytics-team`;
# MAGIC -- GRANT SELECT ON SCHEMA delta_sharing_demo.gold TO `analytics-team`;
# MAGIC
# MAGIC -- Grant broader access to the data engineering team
# MAGIC -- GRANT USAGE ON CATALOG delta_sharing_demo TO `data-engineering`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA delta_sharing_demo.bronze TO `data-engineering`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA delta_sharing_demo.silver TO `data-engineering`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA delta_sharing_demo.gold TO `data-engineering`;
# MAGIC
# MAGIC SELECT 'Uncomment GRANT statements above to apply' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table-Level Grants

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restrict specific tables
# MAGIC -- GRANT SELECT ON TABLE delta_sharing_demo.gold.gold_daily_price_summary TO `analysts`;
# MAGIC -- DENY SELECT ON TABLE delta_sharing_demo.bronze.bronze_positions TO `analysts`;
# MAGIC
# MAGIC SELECT 'Uncomment GRANT/DENY statements above to apply' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Filter Functions
# MAGIC
# MAGIC Restrict which rows a user can see based on their identity.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a row filter function that restricts by region
# MAGIC -- CREATE OR REPLACE FUNCTION delta_sharing_demo.control.region_filter(region_val STRING)
# MAGIC -- RETURNS BOOLEAN
# MAGIC -- RETURN IF(
# MAGIC --   is_member('unrestricted-data-access'),
# MAGIC --   true,
# MAGIC --   region_val IN ('PJM-WEST', 'PJM-EAST')
# MAGIC -- );
# MAGIC --
# MAGIC -- -- Apply the filter to the gold table
# MAGIC -- ALTER TABLE delta_sharing_demo.gold.gold_daily_price_summary
# MAGIC -- SET ROW FILTER delta_sharing_demo.control.region_filter ON (region);
# MAGIC
# MAGIC SELECT 'Row filter example — uncomment to apply' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Mask Functions
# MAGIC
# MAGIC Mask sensitive column values for users without elevated access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a column mask that hides exact prices for restricted users
# MAGIC -- CREATE OR REPLACE FUNCTION delta_sharing_demo.control.price_mask(price_val DOUBLE)
# MAGIC -- RETURNS DOUBLE
# MAGIC -- RETURN IF(
# MAGIC --   is_member('price-data-access'),
# MAGIC --   price_val,
# MAGIC --   ROUND(price_val / 10) * 10  -- Round to nearest 10
# MAGIC -- );
# MAGIC --
# MAGIC -- ALTER TABLE delta_sharing_demo.gold.gold_daily_price_summary
# MAGIC -- ALTER COLUMN avg_price SET MASK delta_sharing_demo.control.price_mask;
# MAGIC
# MAGIC SELECT 'Column mask example — uncomment to apply' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Grants

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show current grants on the gold schema
# MAGIC SHOW GRANTS ON SCHEMA delta_sharing_demo.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show grants on a specific table
# MAGIC SHOW GRANTS ON TABLE delta_sharing_demo.gold.gold_daily_price_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Performance
# MAGIC
# MAGIC ### Compute Sizing Guide
# MAGIC
# MAGIC | Workload | Cluster Type | Workers | Node Type | Use Case |
# MAGIC |----------|-------------|---------|-----------|----------|
# MAGIC | **Batch ingestion** (03-05) | Job cluster | 2-4 | `i3.xlarge` | Daily pipeline |
# MAGIC | **Interactive analytics** | All-purpose | 2-8 | `i3.xlarge` | Analysts, ad-hoc |
# MAGIC | **Streaming** | Job cluster | 2-4 | `i3.xlarge` | Near-real-time |
# MAGIC | **DLT pipeline** | Managed by DLT | Auto | Auto | Production |
# MAGIC
# MAGIC > Use **serverless** compute where available to eliminate cluster management overhead.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Caching Shared Tables
# MAGIC
# MAGIC Shared tables are read over the network. Cache frequently accessed data locally.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cache a shared table for the duration of the session
# MAGIC -- CACHE TABLE shared_energy_market.energy_trading.market_prices_pjm;
# MAGIC
# MAGIC -- Cache a specific query result
# MAGIC -- CACHE SELECT * FROM shared_energy_market.energy_trading.market_prices_pjm
# MAGIC -- WHERE datetime_beginning_ept >= '2024-01-01';
# MAGIC
# MAGIC SELECT 'Caching examples — uncomment to apply' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition Pruning
# MAGIC
# MAGIC Bronze tables are partitioned by `_ingested_date`. Always include this in WHERE clauses.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: partition pruning kicks in, reads only relevant files
# MAGIC EXPLAIN COST
# MAGIC SELECT * FROM delta_sharing_demo.bronze.bronze_market_prices
# MAGIC WHERE _ingested_date = current_date();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: no partition pruning, full table scan
# MAGIC EXPLAIN COST
# MAGIC SELECT * FROM delta_sharing_demo.bronze.bronze_market_prices
# MAGIC WHERE _ingested_at > '2024-01-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predicate Pushdown on Shared Tables
# MAGIC
# MAGIC Delta Sharing supports predicate pushdown — filters are pushed to the provider,
# MAGIC reducing network transfer. Use column filters in your queries.
# MAGIC
# MAGIC ```sql
# MAGIC -- Predicate pushdown: only matching files are transferred
# MAGIC SELECT * FROM shared_energy_market.energy_trading.market_prices_pjm
# MAGIC WHERE datetime_beginning_ept >= '2024-06-01';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unity Catalog Governance
# MAGIC
# MAGIC ### Data Lineage
# MAGIC
# MAGIC Query `system.access.table_lineage` to trace data flow from shared tables through the pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trace lineage: what reads from the shared catalog?
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   target_table_full_name,
# MAGIC   event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name LIKE 'shared_energy_market%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trace lineage: what feeds gold tables?
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   target_table_full_name,
# MAGIC   event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE 'delta_sharing_demo.gold%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Logs
# MAGIC
# MAGIC Query `system.access.audit` to see who accessed shared data and when.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent access to shared tables
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email AS user_email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg AS table_name
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('getTable', 'commandSubmit')
# MAGIC   AND request_params.full_name_arg LIKE 'delta_sharing_demo%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tags for Classification
# MAGIC
# MAGIC Apply tags to catalogs, schemas, and tables for data classification and discovery.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the shared catalog
# MAGIC -- ALTER CATALOG delta_sharing_demo SET TAGS ('data_source' = 'delta_sharing', 'environment' = 'demo');
# MAGIC
# MAGIC -- Tag schemas by layer
# MAGIC -- ALTER SCHEMA delta_sharing_demo.bronze SET TAGS ('layer' = 'bronze', 'pii' = 'false');
# MAGIC -- ALTER SCHEMA delta_sharing_demo.silver SET TAGS ('layer' = 'silver', 'pii' = 'false');
# MAGIC -- ALTER SCHEMA delta_sharing_demo.gold SET TAGS ('layer' = 'gold', 'pii' = 'false');
# MAGIC
# MAGIC -- Tag sensitive tables
# MAGIC -- ALTER TABLE delta_sharing_demo.bronze.bronze_positions SET TAGS ('sensitivity' = 'high');
# MAGIC
# MAGIC SELECT 'Tag examples — uncomment to apply' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monitoring Dashboard
# MAGIC
# MAGIC Build a SQL dashboard with these recommended tiles over the control tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1: Pipeline Freshness

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows when each pipeline stage last ran
# MAGIC SELECT
# MAGIC   pipeline_stage,
# MAGIC   last_processed_version,
# MAGIC   last_processed_ts,
# MAGIC   run_id,
# MAGIC   TIMESTAMPDIFF(MINUTE, last_processed_ts, current_timestamp()) AS minutes_since_last_run
# MAGIC FROM delta_sharing_demo.control.control_cdf_checkpoint
# MAGIC ORDER BY pipeline_stage;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 2: Data Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DQ check results (from notebook 06)
# MAGIC SELECT
# MAGIC   check_name,
# MAGIC   table_name,
# MAGIC   status,
# MAGIC   checked_at,
# MAGIC   details
# MAGIC FROM delta_sharing_demo.control.control_data_quality
# MAGIC ORDER BY checked_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 3: CDF Lag KPI

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare checkpoint version vs current table version to detect lag
# MAGIC -- This query shows how far behind each stage is from its source table
# MAGIC SELECT
# MAGIC   c.pipeline_stage,
# MAGIC   c.last_processed_version AS checkpoint_version,
# MAGIC   c.last_processed_ts,
# MAGIC   TIMESTAMPDIFF(MINUTE, c.last_processed_ts, current_timestamp()) AS lag_minutes
# MAGIC FROM delta_sharing_demo.control.control_cdf_checkpoint c
# MAGIC ORDER BY lag_minutes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Operational Runbook
# MAGIC
# MAGIC ### Scenario 1: Share Access Revoked
# MAGIC
# MAGIC **Symptom:** Queries against `shared_energy_market` fail with permission errors.
# MAGIC
# MAGIC **Response:**
# MAGIC 1. Verify: `SHOW SHARES IN PROVIDER e2_demo_west_provider`
# MAGIC 2. Fall back to bronze tables (last good copy)
# MAGIC 3. Contact the provider to restore access
# MAGIC 4. Once restored, run `03_bronze_ingestion` with `force_full_reload=true`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2: CDF Disabled on Shared Table
# MAGIC
# MAGIC **Symptom:** CDF reads fail with "Change data feed is not enabled".
# MAGIC
# MAGIC **Response:**
# MAGIC 1. Contact provider to re-enable CDF
# MAGIC 2. Reset checkpoint for affected stage:
# MAGIC ```sql
# MAGIC DELETE FROM delta_sharing_demo.control.control_cdf_checkpoint
# MAGIC WHERE pipeline_stage = 'bronze_market_prices';
# MAGIC ```
# MAGIC 3. Run notebook with `force_full_reload=true`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3: Schema Drift Detected
# MAGIC
# MAGIC **Symptom:** Notebook 06 reports schema drift alert.
# MAGIC
# MAGIC **Response:**
# MAGIC 1. Inspect the drift:
# MAGIC ```sql
# MAGIC DESCRIBE TABLE shared_energy_market.energy_trading.market_prices_pjm;
# MAGIC DESCRIBE TABLE delta_sharing_demo.bronze.bronze_market_prices;
# MAGIC ```
# MAGIC 2. Bronze handles new columns via `mergeSchema=true` (automatic)
# MAGIC 3. Update silver DDL if new columns need to flow through:
# MAGIC ```sql
# MAGIC ALTER TABLE delta_sharing_demo.silver.silver_prices_bitemporal
# MAGIC ADD COLUMNS (new_column_name TYPE COMMENT 'description');
# MAGIC ```
# MAGIC 4. Update MERGE logic in notebook 04 to map the new column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 4: SLA Breached
# MAGIC
# MAGIC **Symptom:** Dashboard shows `minutes_since_last_run` > SLA threshold.
# MAGIC
# MAGIC **Response:**
# MAGIC 1. Check job history:
# MAGIC ```bash
# MAGIC databricks jobs list-runs --job-id <id> --limit 5
# MAGIC ```
# MAGIC 2. Check provider freshness:
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY shared_energy_market.energy_trading.market_prices_pjm LIMIT 5;
# MAGIC ```
# MAGIC 3. If provider is stale — nothing to do, document in audit
# MAGIC 4. If pipeline failed — fix and manually trigger:
# MAGIC ```bash
# MAGIC databricks jobs run-now --job-id <id>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 5: CDF Version Gap (VACUUM)
# MAGIC
# MAGIC **Symptom:** CDF read fails with "version X has been vacuumed" error.
# MAGIC
# MAGIC **Response:**
# MAGIC 1. Find the earliest available version:
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY delta_sharing_demo.bronze.bronze_market_prices;
# MAGIC -- Look for the earliest version still available
# MAGIC ```
# MAGIC 2. Reset checkpoint to earliest available:
# MAGIC ```sql
# MAGIC UPDATE delta_sharing_demo.control.control_cdf_checkpoint
# MAGIC SET last_processed_version = <earliest_available_version>,
# MAGIC     last_processed_ts = current_timestamp(),
# MAGIC     run_id = 'vacuum_recovery'
# MAGIC WHERE pipeline_stage = 'bronze_market_prices';
# MAGIC ```
# MAGIC 3. Run pipeline — it will reprocess from the earliest available version
# MAGIC 4. **Prevention:** Set `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration`
# MAGIC    longer than your max pipeline gap:
# MAGIC ```sql
# MAGIC ALTER TABLE delta_sharing_demo.bronze.bronze_market_prices
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = 'interval 30 days',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 30 days'
# MAGIC );
# MAGIC ```
