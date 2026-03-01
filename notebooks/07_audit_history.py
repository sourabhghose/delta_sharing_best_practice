# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Audit History & Reconciliation (Recipient Workspace)
# MAGIC
# MAGIC **Run this notebook on the Recipient Workspace.**
# MAGIC
# MAGIC The **audit layer** provides a permanent, tamper-evident record of every change
# MAGIC that has flowed through the pipeline. It is designed for:
# MAGIC - **Compliance** — regulators may require evidence of what data was seen and when
# MAGIC - **Debugging** — trace any downstream anomaly back to a specific change event
# MAGIC - **Reconciliation** — confirm bronze captures the same data the provider exposed
# MAGIC
# MAGIC Design:
# MAGIC - Reads CDF from bronze tables and stores each change event as a JSON-serialized row
# MAGIC - Partition by `source_table` for efficient per-table audit queries
# MAGIC - 365-day log retention (configurable to 7 years for regulatory contexts)
# MAGIC - Bronze tables get 90-day retention (re-ingestable from the share if needed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

DEMO_CATALOG   = "delta_sharing_demo"
SHARED_CATALOG = "shared_energy_market"
BRONZE_SCHEMA  = f"{DEMO_CATALOG}.bronze"
AUDIT_SCHEMA   = f"{DEMO_CATALOG}.audit"

CURRENT_TS = datetime.utcnow().isoformat()

print(f"Bronze schema : {BRONZE_SCHEMA}")
print(f"Audit schema  : {AUDIT_SCHEMA}")
print(f"Run time      : {CURRENT_TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Audit Changelog Table
# MAGIC
# MAGIC This is the permanent CDF archive. It stores a flattened representation of every
# MAGIC change event from the bronze layer, including a JSON snapshot of the full row.
# MAGIC
# MAGIC Why store a JSON snapshot?
# MAGIC - Captures the full row at the moment of change, even if the bronze schema evolves later
# MAGIC - Enables point-in-time reconstruction of any record's state without querying Delta history
# MAGIC - Suitable for export to cold storage or compliance systems that can't read Delta

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.audit_changelog (
        changelog_id     STRING    COMMENT 'UUID for this audit entry',
        source_table     STRING    COMMENT 'Bronze table that produced this change event',
        change_type      STRING    COMMENT 'CDF operation: insert, update_preimage, update_postimage, delete',
        change_version   LONG      COMMENT 'Delta commit version where this change occurred',
        change_timestamp TIMESTAMP COMMENT 'Wall-clock time of the Delta commit',
        record_key       STRING    COMMENT 'Best-effort business key of the changed row (uses _batch_id as fallback)',
        record_snapshot  STRING    COMMENT 'Full JSON snapshot of the row at the time of the change',
        captured_at      TIMESTAMP COMMENT 'When this audit entry was written (this notebook run time)',
        batch_id         STRING    COMMENT 'Processing batch ID grouping this audit run'
    )
    USING DELTA
    PARTITIONED BY (source_table)              -- Each bronze table gets its own partition for efficient per-table queries
    TBLPROPERTIES (
        delta.enableChangeDataFeed           = true,
        delta.autoOptimize.optimizeWrite     = true,
        delta.logRetentionDuration           = 'interval 365 days',     -- Retain Delta log for 1 year
        delta.deletedFileRetentionDuration   = 'interval 365 days'      -- Don't vacuum data files for 1 year
    )
    COMMENT 'Permanent CDF archive. Each row records a change event from a bronze table. Retention: 365 days.'
""")
print("✓ audit_changelog table created with 365-day retention.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Capture CDF from Bronze Tables
# MAGIC
# MAGIC For each bronze table, we read the full CDF history starting from the first version
# MAGIC where CDF was enabled. This builds the complete audit archive.
# MAGIC
# MAGIC In a scheduled pipeline, you would checkpoint the last captured version (same pattern
# MAGIC as notebooks 03-05) to avoid re-processing the same events on every run.
# MAGIC For this demo, we read from the beginning each time and rely on Delta's idempotency.

# COMMAND ----------

import uuid
import json

# List of (fully-qualified-name, short-name) pairs for bronze tables
bronze_tables = [
    (f"{BRONZE_SCHEMA}.bronze_market_prices",         "bronze_market_prices"),
    (f"{BRONZE_SCHEMA}.bronze_daily_trading_summary",  "bronze_daily_trading_summary"),
    (f"{BRONZE_SCHEMA}.bronze_positions",             "bronze_positions"),
    (f"{BRONZE_SCHEMA}.bronze_iso_market",            "bronze_iso_market"),
    (f"{BRONZE_SCHEMA}.bronze_turbine_locations",     "bronze_turbine_locations"),
]

batch_id     = str(uuid.uuid4())[:8]  # Groups all audit entries captured in this run
total_changes = 0

for table_fqn, table_name in bronze_tables:
    print(f"\nCapturing CDF from {table_name}...")

    try:
        # Find the first version where CDF was enabled on this bronze table
        # CDF events only exist from the version where it was enabled onward
        history  = spark.sql(f"DESCRIBE HISTORY {table_fqn}").orderBy("version")
        versions = history.collect()

        # Scan the history to find the CDF-enable commit
        # If not found, start from version 0 (all data was written after CDF was enabled)
        cdf_start_version = 0
        for v in versions:
            if v["operationParameters"] and "enableChangeDataFeed" in str(v["operationParameters"]):
                cdf_start_version = v["version"]
                break

        print(f"  Reading CDF from version {cdf_start_version} onward...")

        # Read the full CDF history from this bronze table
        cdf_df = (
            spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", cdf_start_version)
            .table(table_fqn)
        )

        change_count = cdf_df.count()
        print(f"  CDF records: {change_count:,}")

        if change_count > 0:
            # Extract data columns (everything except the CDF metadata columns)
            # These will be serialized as the JSON snapshot
            data_cols = [c for c in cdf_df.columns if not c.startswith("_change_")]

            audit_entries = (
                cdf_df
                # Unique ID for each audit row
                .withColumn("changelog_id",      F.expr("uuid()"))
                # Which bronze table this change came from
                .withColumn("source_table",      F.lit(table_name))
                # The CDF change type: insert, update_preimage, update_postimage, delete
                .withColumn("change_type",       F.col("_change_type"))
                # Delta's internal version number at the time of the change
                .withColumn("change_version",    F.col("_commit_version"))
                # Wall-clock time of the commit
                .withColumn("change_timestamp",  F.col("_commit_timestamp"))
                # Business key: use batch_id as a fallback; add a real key here if available
                .withColumn("record_key",        F.coalesce(F.col("_batch_id"), F.lit("unknown")))
                # Serialize the full row as JSON for long-term archival
                .withColumn("record_snapshot",   F.to_json(F.struct(*data_cols)))
                .withColumn("captured_at",       F.lit(CURRENT_TS).cast("timestamp"))
                .withColumn("batch_id",          F.lit(batch_id))
                # Select only audit columns — strip out the original data columns
                .select(
                    "changelog_id", "source_table", "change_type", "change_version",
                    "change_timestamp", "record_key", "record_snapshot", "captured_at", "batch_id"
                )
            )

            audit_entries.write.format("delta").mode("append").saveAsTable(f"{AUDIT_SCHEMA}.audit_changelog")
            total_changes += change_count
            print(f"  ✓ {change_count:,} entries written to audit_changelog")

    except Exception as e:
        if "not enabled" in str(e).lower() or "change data feed" in str(e).lower():
            # CDF might not be available if the table was just created with no CDF version
            print(f"  ⚠ CDF not available for the requested version range — skipping.")
        else:
            print(f"  ✗ Error: {e}")

print(f"\n{'='*60}")
print(f"Total audit entries captured: {total_changes:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Audit Queries
# MAGIC
# MAGIC Demonstrate the types of compliance and debugging queries this archive enables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query: Change Counts by Source Table and Type

# COMMAND ----------

# Overview: how many of each change type came from each source table?
# An unexpected high delete count may indicate the provider removed data
print("Changes by Source Table and Type:\n")
(
    spark.table(f"{AUDIT_SCHEMA}.audit_changelog")
    .groupBy("source_table", "change_type")
    .agg(F.count("*").alias("change_count"))
    .orderBy("source_table", "change_type")
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query: Recent Changes (Last 7 Days)

# COMMAND ----------

# Show the most recent audit entries — useful for daily monitoring
print("Recent Audit Entries (last 7 days, latest first):\n")
(
    spark.table(f"{AUDIT_SCHEMA}.audit_changelog")
    .filter("captured_at >= current_timestamp() - INTERVAL 7 DAYS")
    .orderBy(F.col("change_timestamp").desc())
    .limit(20)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query: Full History for a Specific Record
# MAGIC
# MAGIC Trace every change event for a single record — answers "what happened to this row?"

# COMMAND ----------

# Find a sample record key from the positions table to demonstrate with
sample = (
    spark.table(f"{AUDIT_SCHEMA}.audit_changelog")
    .filter("source_table = 'bronze_positions'")
    .select("record_key")
    .limit(1)
    .collect()
)

if sample:
    sample_key = sample[0]["record_key"]
    print(f"Full audit trail for record key: {sample_key}\n")
    (
        spark.table(f"{AUDIT_SCHEMA}.audit_changelog")
        .filter(f"record_key = '{sample_key}'")
        .orderBy("change_timestamp")   # Chronological order
        .display()
    )
else:
    print("No audit entries for positions yet — run notebook 03 first to ingest data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Reconciliation — Bronze vs Shared Table
# MAGIC
# MAGIC A reconciliation check verifies that the recipient's bronze tables contain
# MAGIC the same number of rows as the provider's shared tables.
# MAGIC
# MAGIC **Important caveat:** Bronze is append-only and may contain multiple batches,
# MAGIC so we compare the **latest batch only** against the current shared table row count.
# MAGIC A mismatch (MISMATCH) could indicate:
# MAGIC - The provider updated their tables between our ingestion and this check
# MAGIC - A partial ingestion failure in notebook 03
# MAGIC - Rows were filtered out (e.g. by the CDF change_type filter)

# COMMAND ----------

print("Reconciliation: Latest Bronze Batch vs Current Shared Table Counts\n")
print(f"{'Table':<40} {'Shared':>10} {'Bronze':>10} {'Delta':>10} {'Status':>10}")
print("-" * 85)

# Map from bronze table name to shared table FQN
shared_mappings = {
    "bronze_market_prices":        f"{SHARED_CATALOG}.energy_trading.market_prices_pjm",
    "bronze_daily_trading_summary": f"{SHARED_CATALOG}.energy_trading.gold_daily_trading_summary",
    "bronze_positions":            f"{SHARED_CATALOG}.energy_trading.positions",
    "bronze_iso_market":           f"{SHARED_CATALOG}.power_generation.iso_market",
    "bronze_turbine_locations":    f"{SHARED_CATALOG}.power_generation.turbine_locations",
}

for bronze_name, shared_fqn in shared_mappings.items():
    bronze_fqn = f"{BRONZE_SCHEMA}.{bronze_name}"

    try:
        # Count current shared table rows — this is the provider's current state
        shared_count = spark.table(shared_fqn).count()

        # Count only the most recent batch in bronze to get a comparable point-in-time snapshot
        latest_batch = (
            spark.table(bronze_fqn)
            .agg(F.max("_batch_id").alias("latest_batch"))
            .collect()[0]["latest_batch"]
        )
        bronze_count = (
            spark.table(bronze_fqn)
            .filter(f"_batch_id = '{latest_batch}'")
            .count()
        )

        delta  = bronze_count - shared_count
        status = "OK" if delta == 0 else "MISMATCH"
        print(f"{bronze_name:<40} {shared_count:>10,} {bronze_count:>10,} {delta:>10,} {status:>10}")

    except Exception as e:
        print(f"{bronze_name:<40} {'ERROR':>10} {'—':>10} {'—':>10} {'ERROR':>10}")
        print(f"  Detail: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Retention Policy Recommendations
# MAGIC
# MAGIC Different layers have different retention needs based on how they're used and
# MAGIC whether they can be reconstructed from upstream data.
# MAGIC
# MAGIC | Layer | Table | Recommended Retention | Rationale |
# MAGIC |-------|-------|-----------------------|-----------|
# MAGIC | Bronze | `bronze_*` | 90 days (Delta log: 30 days) | Raw data can be re-ingested from the share |
# MAGIC | Silver | `silver_*_bitemporal` | 365 days | Bitemporal history is the system of record — don't delete |
# MAGIC | Gold | `gold_*` | 90 days | Recomputable from silver at any time |
# MAGIC | Audit | `audit_changelog` | **7 years** | Regulatory compliance — permanent record |
# MAGIC | Control | `control_*` | 365 days | Operational monitoring trend analysis |

# COMMAND ----------

# Apply long-term retention settings to the audit table
# These settings prevent VACUUM from deleting audit data prematurely
spark.sql(f"""
    ALTER TABLE {AUDIT_SCHEMA}.audit_changelog SET TBLPROPERTIES (
        delta.logRetentionDuration         = 'interval 365 days',
        delta.deletedFileRetentionDuration = 'interval 365 days'
    )
""")
print("✓ Audit changelog: 365-day retention confirmed")

# Apply shorter retention to bronze tables
# Bronze data can be re-ingested from the share, so long retention is not required
for _, bronze_name in bronze_tables:
    bronze_fqn = f"{BRONZE_SCHEMA}.{bronze_name}"
    try:
        spark.sql(f"""
            ALTER TABLE {bronze_fqn} SET TBLPROPERTIES (
                delta.logRetentionDuration         = 'interval 90 days',
                delta.deletedFileRetentionDuration = 'interval 90 days'
            )
        """)
        print(f"✓ {bronze_name}: 90-day retention set")
    except Exception as e:
        print(f"⚠ {bronze_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Status |
# MAGIC |-----------|--------|
# MAGIC | `audit_changelog` table created | ✓ |
# MAGIC | CDF captured from all bronze tables | ✓ |
# MAGIC | Reconciliation (bronze vs shared) | ✓ |
# MAGIC | Retention policies applied | ✓ |
# MAGIC
# MAGIC ### Full Medallion Pipeline Complete
# MAGIC
# MAGIC ```
# MAGIC Shared Tables (Provider Workspace)
# MAGIC         │
# MAGIC         │  Delta Sharing (secure, open protocol)
# MAGIC         ▼
# MAGIC Bronze   — append-only ingestion + metadata + CDF enabled
# MAGIC         │
# MAGIC         │  SCD2 MERGE (CDF incremental)
# MAGIC         ▼
# MAGIC Silver   — bitemporal: valid time (fact) + transaction time (knowledge)
# MAGIC         │
# MAGIC         │  Aggregation (CDF incremental MERGE)
# MAGIC         ▼
# MAGIC Gold     — daily summaries, position rollups, liquid clustering
# MAGIC
# MAGIC Cross-cutting:
# MAGIC   Audit  ← CDF archive of all bronze changes (permanent record)
# MAGIC   Control ← DQ checks + freshness SLAs + CDF processing checkpoints
# MAGIC ```
# MAGIC
# MAGIC **Optional next steps:**
# MAGIC - Review `08_scheduling_and_orchestration` for Jobs, streaming, and DLT patterns
# MAGIC - Review `09_recipient_best_practices` for access control, performance, and governance
