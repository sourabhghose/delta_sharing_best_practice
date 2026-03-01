# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Data Quality Monitoring (Recipient Workspace)
# MAGIC
# MAGIC **Run this notebook on the Recipient Workspace.**
# MAGIC
# MAGIC Data quality monitoring runs **after every ingestion cycle** (notebooks 03-05).
# MAGIC It writes structured results to `control.control_data_quality` and
# MAGIC `control.control_freshness`, which can be queried by a monitoring dashboard.
# MAGIC
# MAGIC Checks implemented:
# MAGIC 1. **Row count delta** — alert if the number of rows changed by more than N% between batches
# MAGIC    (sudden drops may indicate source data loss; sudden spikes may indicate duplicates)
# MAGIC 2. **Null rate** — alert if required columns have unexpected nulls (ingestion failures)
# MAGIC 3. **Schema drift** — detect if the provider added or removed columns from a shared table
# MAGIC 4. **Freshness SLA** — alert if data hasn't been updated within the agreed SLA window
# MAGIC
# MAGIC All results are written as Delta rows with PASS/FAIL status, making them
# MAGIC queryable via SQL and suitable for Databricks AI/BI dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

DEMO_CATALOG   = "delta_sharing_demo"
SHARED_CATALOG = "shared_energy_market"
BRONZE_SCHEMA  = f"{DEMO_CATALOG}.bronze"
CONTROL_SCHEMA = f"{DEMO_CATALOG}.control"

# --- DQ Thresholds ---
# These are configurable — adjust to match business SLAs
ROW_COUNT_DELTA_THRESHOLD = 0.50   # Alert if batch-over-batch row count changes by more than 50%
NULL_RATE_THRESHOLD       = 0.05   # Alert if null rate on required columns exceeds 5%
FRESHNESS_SLA_HOURS       = 24     # Alert if bronze data is older than 24 hours

CURRENT_TS = datetime.utcnow().isoformat()

print(f"Row count delta threshold : {ROW_COUNT_DELTA_THRESHOLD:.0%}")
print(f"Null rate threshold       : {NULL_RATE_THRESHOLD:.0%}")
print(f"Freshness SLA             : {FRESHNESS_SLA_HOURS} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Control Tables
# MAGIC
# MAGIC These tables persist DQ results across runs, enabling trend analysis and alerting.
# MAGIC Both tables have CDF enabled so they can be subscribed to by downstream monitors.

# COMMAND ----------

# DQ check results — one row per check per run
# Structured as a fact table: check_type + table_name + status + metric_value
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.control_data_quality (
        check_id        STRING    COMMENT 'UUID for this specific check execution',
        check_time      TIMESTAMP COMMENT 'When the check ran',
        table_name      STRING    COMMENT 'Fully-qualified table name being checked',
        check_type      STRING    COMMENT 'Category: row_count, null_rate, schema_drift',
        check_detail    STRING    COMMENT 'Human-readable description of this specific check',
        metric_value    DOUBLE    COMMENT 'The measured value (e.g. null rate = 0.02 = 2%)',
        threshold_value DOUBLE    COMMENT 'The configured threshold for PASS/FAIL',
        status          STRING    COMMENT 'PASS = within threshold, FAIL = threshold exceeded, ERROR = check could not run',
        message         STRING    COMMENT 'Human-readable result with context'
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    COMMENT 'Data quality check results. Query with status=FAIL to surface issues.'
""")
print("✓ control_data_quality table created/verified.")

# Freshness tracking — one row per source table, updated on every run (overwrite mode)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.control_freshness (
        table_name          STRING    COMMENT 'Bronze table being monitored',
        last_check_time     TIMESTAMP COMMENT 'When freshness was last evaluated',
        last_ingested_at    TIMESTAMP COMMENT 'Most recent _ingested_at value in the bronze table',
        staleness_hours     DOUBLE    COMMENT 'Hours since the most recent row was ingested',
        sla_hours           DOUBLE    COMMENT 'Freshness SLA threshold in hours',
        sla_status          STRING    COMMENT 'WITHIN_SLA = OK, BREACHED = alert needed, UNKNOWN = no data',
        last_source_version LONG      COMMENT 'Provider table version at last ingestion',
        row_count           LONG      COMMENT 'Total rows currently in the bronze table'
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    COMMENT 'Freshness and SLA status per bronze table. Updated on each pipeline run.'
""")
print("✓ control_freshness table created/verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Row Count Delta Checks
# MAGIC
# MAGIC Compares the row count of the latest ingestion batch against the previous batch.
# MAGIC A large delta (positive or negative) often indicates a data issue:
# MAGIC - **Sudden drop**: provider may have deleted records or the share was partially read
# MAGIC - **Sudden spike**: possible duplicate ingestion or source data anomaly

# COMMAND ----------

import uuid

bronze_tables = [
    f"{BRONZE_SCHEMA}.bronze_market_prices",
    f"{BRONZE_SCHEMA}.bronze_daily_trading_summary",
    f"{BRONZE_SCHEMA}.bronze_positions",
    f"{BRONZE_SCHEMA}.bronze_iso_market",
    f"{BRONZE_SCHEMA}.bronze_turbine_locations",
]

dq_results = []  # Collect all check results here; written to Delta at the end

print("Row Count Delta Checks:")
print("-" * 70)

for table_fqn in bronze_tables:
    table_name = table_fqn.split(".")[-1]

    try:
        current_count = spark.table(table_fqn).count()

        # Group by batch and sort by latest ingestion time to get the two most recent batches
        # We compare batch N vs batch N-1 to detect unusual changes
        batch_counts = (
            spark.table(table_fqn)
            .groupBy("_batch_id")
            .agg(
                F.count("*").alias("batch_row_count"),
                F.max("_ingested_at").alias("latest_ingested_at")   # For sorting
            )
            .orderBy(F.col("latest_ingested_at").desc())
            .collect()
        )

        if len(batch_counts) >= 2:
            # Two or more batches: compare latest vs previous
            latest_count = batch_counts[0]["batch_row_count"]
            prev_count   = batch_counts[1]["batch_row_count"]
            delta_pct    = abs(latest_count - prev_count) / max(prev_count, 1)
            status  = "PASS" if delta_pct <= ROW_COUNT_DELTA_THRESHOLD else "FAIL"
            msg     = f"Row count delta: {prev_count:,} → {latest_count:,} ({delta_pct:.1%} change)"
        else:
            # First batch — no baseline to compare against; always PASS
            delta_pct = 0.0
            status    = "PASS"
            msg       = f"First batch: {current_count:,} rows (no previous batch to compare)"

        dq_results.append((
            str(uuid.uuid4())[:8], CURRENT_TS, table_fqn, "row_count",
            "Row count delta between consecutive batches",
            delta_pct, ROW_COUNT_DELTA_THRESHOLD, status, msg
        ))
        print(f"  {table_name}: {status} — {msg}")

    except Exception as e:
        dq_results.append((
            str(uuid.uuid4())[:8], CURRENT_TS, table_fqn, "row_count",
            "Row count check", 0.0, ROW_COUNT_DELTA_THRESHOLD, "ERROR", str(e)
        ))
        print(f"  {table_name}: ERROR — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Null Rate Checks
# MAGIC
# MAGIC Required metadata columns should never be null — if they are, the ingestion code
# MAGIC failed to set them, indicating a bug in notebook 03.
# MAGIC
# MAGIC We check `_ingested_at` and `_batch_id` for all tables because these are always
# MAGIC set by the ingestion loop regardless of the source schema.

# COMMAND ----------

# Required columns per table — add business columns here as needed
# (e.g. 'region' for market_prices once the source schema is confirmed)
required_columns = {
    f"{BRONZE_SCHEMA}.bronze_market_prices":          ["_ingested_at", "_batch_id"],
    f"{BRONZE_SCHEMA}.bronze_positions":              ["_ingested_at", "_batch_id"],
    f"{BRONZE_SCHEMA}.bronze_daily_trading_summary":  ["_ingested_at", "_batch_id"],
    f"{BRONZE_SCHEMA}.bronze_iso_market":             ["_ingested_at", "_batch_id"],
    f"{BRONZE_SCHEMA}.bronze_turbine_locations":      ["_ingested_at", "_batch_id"],
}

print("\nNull Rate Checks:")
print("-" * 70)

for table_fqn, columns in required_columns.items():
    table_name = table_fqn.split(".")[-1]
    try:
        df    = spark.table(table_fqn)
        total = df.count()

        for col_name in columns:
            if col_name not in df.columns:
                # Column doesn't exist in this table — skip rather than error
                continue

            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rate  = null_count / max(total, 1)
            status     = "PASS" if null_rate <= NULL_RATE_THRESHOLD else "FAIL"
            msg        = f"Null rate for '{col_name}': {null_count}/{total} ({null_rate:.2%})"

            dq_results.append((
                str(uuid.uuid4())[:8], CURRENT_TS, table_fqn, "null_rate",
                f"Null check on required column: {col_name}",
                null_rate, NULL_RATE_THRESHOLD, status, msg
            ))
            print(f"  {table_name}.{col_name}: {status} — {msg}")

    except Exception as e:
        print(f"  {table_name}: ERROR — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Schema Drift Detection
# MAGIC
# MAGIC Compares the current bronze column set (minus our metadata columns) against
# MAGIC the expected schema established on the first run.
# MAGIC
# MAGIC **Why this matters:** If the provider adds or renames a column, the bronze table
# MAGIC handles it via `mergeSchema=true` — but the silver MERGE logic in notebook 04
# MAGIC may need to be updated to handle the new column correctly.
# MAGIC
# MAGIC Schema drift is expected occasionally — this check surfaces it so the team
# MAGIC can decide whether action is needed.

# COMMAND ----------

# Baseline schema is captured on the first run of each session
# In production, persist this to a control table so it's stable across runs
expected_schemas = {}

print("\nSchema Drift Detection:")
print("-" * 70)

# Our metadata columns — excluded from the drift comparison because they're always added by us
meta_cols = {"_ingested_at", "_source_share", "_source_version", "_batch_id", "_ingested_date"}

for table_fqn in bronze_tables:
    table_name = table_fqn.split(".")[-1]

    try:
        current_cols = set(spark.table(table_fqn).columns)
        source_cols  = current_cols - meta_cols  # Strip our added columns to get the provider's schema

        if table_name in expected_schemas:
            expected = expected_schemas[table_name]
            added    = source_cols - expected
            removed  = expected - source_cols

            if added or removed:
                status = "FAIL"
                msg = f"Schema drift! Added: {added or 'none'}, Removed: {removed or 'none'}"
            else:
                status = "PASS"
                msg = f"Schema matches baseline ({len(source_cols)} provider columns)"
        else:
            # First run: establish baseline for this session
            expected_schemas[table_name] = source_cols
            status = "PASS"
            msg = f"Baseline schema recorded: {len(source_cols)} provider columns"

        dq_results.append((
            str(uuid.uuid4())[:8], CURRENT_TS, table_fqn, "schema_drift",
            "Schema drift vs baseline", 0.0, 0.0, status, msg
        ))
        print(f"  {table_name}: {status} — {msg}")

    except Exception as e:
        print(f"  {table_name}: ERROR — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write DQ Results to Control Table
# MAGIC
# MAGIC All check results collected above are written in a single append operation.
# MAGIC This keeps the control table as a complete audit log across all pipeline runs.

# COMMAND ----------

dq_schema = StructType([
    StructField("check_id",        StringType()),
    StructField("check_time",      StringType()),
    StructField("table_name",      StringType()),
    StructField("check_type",      StringType()),
    StructField("check_detail",    StringType()),
    StructField("metric_value",    DoubleType()),
    StructField("threshold_value", DoubleType()),
    StructField("status",          StringType()),
    StructField("message",         StringType()),
])

dq_df = (
    spark.createDataFrame(dq_results, dq_schema)
    .withColumn("check_time", F.col("check_time").cast("timestamp"))
)

dq_df.write.format("delta").mode("append").saveAsTable(f"{CONTROL_SCHEMA}.control_data_quality")
print(f"✓ {len(dq_results)} DQ check results written to control_data_quality")

# COMMAND ----------

# Summary view of all check results from this run
print("\nData Quality Results (this run):\n")
(
    spark.table(f"{CONTROL_SCHEMA}.control_data_quality")
    .orderBy("check_time", "table_name")
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Freshness SLA Tracking
# MAGIC
# MAGIC For each bronze table, we measure how many hours have elapsed since the most recent
# MAGIC `_ingested_at` value. If this exceeds `FRESHNESS_SLA_HOURS`, the SLA is considered
# MAGIC breached and the monitoring dashboard should alert.
# MAGIC
# MAGIC This catches scenarios where:
# MAGIC - The scheduled ingestion job failed silently
# MAGIC - The provider stopped updating their shared table
# MAGIC - A permissions issue prevented the last run from completing

# COMMAND ----------

freshness_results = []

print("Freshness SLA Tracking:")
print("-" * 70)

for table_fqn in bronze_tables:
    table_name = table_fqn.split(".")[-1]

    try:
        df        = spark.table(table_fqn)
        row_count = df.count()

        # Find the most recently ingested row — this tells us when the last run completed
        latest_row = df.agg(
            F.max("_ingested_at").alias("last_ingested_at"),
            F.max("_source_version").alias("last_source_version"),
        ).collect()[0]

        last_ingested = latest_row["last_ingested_at"]
        last_version  = latest_row["last_source_version"]

        if last_ingested:
            # Compare wall-clock time now vs when the most recent row arrived
            staleness = (datetime.utcnow() - last_ingested).total_seconds() / 3600
            sla_status = "WITHIN_SLA" if staleness <= FRESHNESS_SLA_HOURS else "BREACHED"
            print(f"  {table_name}: {sla_status} (staleness: {staleness:.1f}h, SLA: {FRESHNESS_SLA_HOURS}h)")
        else:
            staleness  = None
            sla_status = "UNKNOWN"  # Table exists but has no data
            print(f"  {table_name}: {sla_status} (no data in table)")

        freshness_results.append((
            table_fqn,
            CURRENT_TS,
            str(last_ingested) if last_ingested else None,
            staleness,
            float(FRESHNESS_SLA_HOURS),
            sla_status,
            int(last_version) if last_version else None,
            row_count
        ))

    except Exception as e:
        print(f"  {table_name}: ERROR — {e}")

# COMMAND ----------

freshness_schema = StructType([
    StructField("table_name",          StringType()),
    StructField("last_check_time",     StringType()),
    StructField("last_ingested_at",    StringType()),
    StructField("staleness_hours",     DoubleType()),
    StructField("sla_hours",           DoubleType()),
    StructField("sla_status",          StringType()),
    StructField("last_source_version", LongType()),
    StructField("row_count",           LongType()),
])

freshness_df = (
    spark.createDataFrame(freshness_results, freshness_schema)
    .withColumn("last_check_time",  F.col("last_check_time").cast("timestamp"))
    .withColumn("last_ingested_at", F.col("last_ingested_at").cast("timestamp"))
)

# Overwrite: we only need the current freshness state, not a history
# (DQ history is in control_data_quality; freshness is a current-state view)
freshness_df.write.format("delta").mode("overwrite").saveAsTable(f"{CONTROL_SCHEMA}.control_freshness")
print(f"\n✓ Freshness data written for {len(freshness_results)} tables")

# COMMAND ----------

print("Freshness SLA Status:\n")
spark.table(f"{CONTROL_SCHEMA}.control_freshness").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Summary
# MAGIC
# MAGIC In a production setup, this section would trigger notifications via
# MAGIC Databricks SQL Alerts, PagerDuty webhooks, or email when failures are found.
# MAGIC For this demo, we print the failures to the notebook output.

# COMMAND ----------

# Summarize DQ failures from this run
failures      = spark.table(f"{CONTROL_SCHEMA}.control_data_quality").filter("status = 'FAIL'")
fail_count    = failures.count()
sla_breaches  = spark.table(f"{CONTROL_SCHEMA}.control_freshness").filter("sla_status = 'BREACHED'")
breach_count  = sla_breaches.count()

print(f"DQ Check Failures  : {fail_count}")
print(f"SLA Breaches       : {breach_count}")

if fail_count > 0:
    print("\n⚠ FAILED DQ Checks — investigate before proceeding:")
    failures.display()

if breach_count > 0:
    print("\n⚠ SLA Breaches — data freshness is outside the agreed window:")
    sla_breaches.display()

if fail_count == 0 and breach_count == 0:
    print("\n✓ All checks passed. Pipeline data quality is within acceptable thresholds.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** Run `07_audit_history` for permanent CDF archive and reconciliation.
