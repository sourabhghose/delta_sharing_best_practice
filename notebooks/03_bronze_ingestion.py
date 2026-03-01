# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Bronze Ingestion (Recipient Workspace)
# MAGIC
# MAGIC **Run this notebook on the Recipient Workspace.**
# MAGIC
# MAGIC The **bronze layer** is the first stage in the medallion architecture.
# MAGIC It stores a faithful, append-only copy of everything received from the share,
# MAGIC enriched with ingestion metadata for lineage and debugging.
# MAGIC
# MAGIC Design principles:
# MAGIC - **Append-only** — we never update or delete bronze records; every ingestion adds new rows
# MAGIC - **Metadata enrichment** — every row is tagged with when it arrived, from which share, and at which version
# MAGIC - **CDF enabled** — downstream layers (silver) can read only what changed since the last run
# MAGIC - **Schema evolution** — `mergeSchema` allows the provider to add columns without breaking ingestion
# MAGIC - **Incremental by default** — uses CDF on the shared tables when a checkpoint exists
# MAGIC
# MAGIC This notebook supports two run modes controlled by widgets:
# MAGIC - **Full load** (`force_full_reload=true`): reads the entire shared table — use on first run
# MAGIC - **Incremental** (`force_full_reload=false`, default): reads only CDF changes since the last checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# The foreign catalog that mirrors the provider's share (read-only)
SHARED_CATALOG = "shared_energy_market"

# Our writable demo catalog where bronze tables are stored
DEMO_CATALOG   = "delta_sharing_demo"
BRONZE_SCHEMA  = f"{DEMO_CATALOG}.bronze"

# Batch metadata — stamped onto every row for lineage and debugging
# BATCH_ID: a short random ID to group all rows written in this run
# INGESTION_TS: the wall-clock time this notebook started (used for _ingested_at)
BATCH_ID      = str(uuid.uuid4())[:8]
INGESTION_TS  = datetime.utcnow().isoformat()
SOURCE_SHARE  = "energy_market_share"  # Informational — the share name on the provider side

# Mapping of (shared_table_fqn, bronze_table_name)
# Each entry defines one ingestion pipeline: read from share, write to bronze
TABLE_MAPPINGS = [
    (f"{SHARED_CATALOG}.energy_trading.market_prices_pjm",         "bronze_market_prices"),
    (f"{SHARED_CATALOG}.energy_trading.gold_daily_trading_summary", "bronze_daily_trading_summary"),
    (f"{SHARED_CATALOG}.energy_trading.positions",                  "bronze_positions"),
    (f"{SHARED_CATALOG}.power_generation.iso_market",               "bronze_iso_market"),
    (f"{SHARED_CATALOG}.power_generation.turbine_locations",        "bronze_turbine_locations"),
]

print(f"Batch ID       : {BATCH_ID}")
print(f"Ingestion time : {INGESTION_TS}")
print(f"Source share   : {SOURCE_SHARE}")
print(f"Tables to load : {len(TABLE_MAPPINGS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters
# MAGIC
# MAGIC These widgets allow the notebook to be parameterized when scheduled via Databricks Jobs.
# MAGIC In the Databricks UI they appear as interactive dropdowns/text boxes at the top of the notebook.
# MAGIC
# MAGIC | Widget | Purpose | First Run | Subsequent Runs |
# MAGIC |--------|---------|-----------|----------------|
# MAGIC | `force_full_reload` | Override CDF and read full table | `true` | `false` |
# MAGIC | `starting_version` | Manually override the CDF starting version | leave blank | leave blank |

# COMMAND ----------

dbutils.widgets.text("starting_version", "",      "CDF Starting Version (blank = auto)")
dbutils.widgets.text("force_full_reload", "false", "Force Full Reload (true/false)")

# Parse widget values — Jobs passes them as strings
FORCE_FULL = dbutils.widgets.get("force_full_reload").strip().lower() == "true"
STARTING_VERSION_OVERRIDE = dbutils.widgets.get("starting_version").strip()

# The checkpoint table tracks the last Delta version processed per stage
CONTROL_TABLE = f"{DEMO_CATALOG}.control.control_cdf_checkpoint"

print(f"Force full reload : {FORCE_FULL}")
print(f"Version override  : {STARTING_VERSION_OVERRIDE or '(auto from checkpoint)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Helpers
# MAGIC
# MAGIC These two functions implement the **incremental processing contract**:
# MAGIC - `get_checkpoint(stage)` — returns the last successfully processed Delta version for a stage
# MAGIC - `write_checkpoint(stage, version, batch_id)` — updates the checkpoint after a successful run
# MAGIC
# MAGIC The checkpoint is stored in `control.control_cdf_checkpoint` and survives across runs.
# MAGIC On the next run, `get_checkpoint` returns the saved version, allowing us to start
# MAGIC CDF reads exactly where we left off.

# COMMAND ----------

def get_checkpoint(stage_name):
    """
    Read the last successfully processed Delta version for a pipeline stage.
    Returns None if no checkpoint exists (first run).
    """
    try:
        rows = spark.sql(f"""
            SELECT last_processed_version
            FROM {CONTROL_TABLE}
            WHERE pipeline_stage = '{stage_name}'
        """).collect()
        if rows:
            return rows[0]["last_processed_version"]
    except Exception:
        pass
    return None

def write_checkpoint(stage_name, version, batch_id):
    """
    Upsert the checkpoint for a pipeline stage using MERGE.
    MERGE ensures this is idempotent — safe to call even if the row already exists.
    """
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS tgt
        USING (SELECT
            '{stage_name}'       AS pipeline_stage,
            {version}            AS last_processed_version,
            current_timestamp()  AS last_processed_ts,
            '{batch_id}'         AS run_id
        ) AS src
        ON tgt.pipeline_stage = src.pipeline_stage
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest Shared Tables into Bronze
# MAGIC
# MAGIC For each table in `TABLE_MAPPINGS`:
# MAGIC 1. Check if a CDF checkpoint exists for this stage
# MAGIC 2. If yes → read only CDF changes since `last_processed_version + 1`
# MAGIC 3. If no (or `force_full_reload=true`) → read the full shared table
# MAGIC 4. Add ingestion metadata columns
# MAGIC 5. Append to the bronze table (partitioned by `_ingested_date`)
# MAGIC 6. Write an updated checkpoint

# COMMAND ----------

ingestion_results = []  # Collect results for the summary table below

for source_fqn, bronze_name in TABLE_MAPPINGS:
    bronze_fqn = f"{BRONZE_SCHEMA}.{bronze_name}"
    stage_name = bronze_name  # Stage name matches the bronze table name for clarity

    print(f"\n{'='*70}")
    print(f"Ingesting: {source_fqn}")
    print(f"       →  {bronze_fqn}")
    print(f"{'='*70}")

    try:
        # --- Decide read mode ---
        # Priority: widget override > checkpoint > full load
        checkpoint_version = None if FORCE_FULL else get_checkpoint(stage_name)
        if STARTING_VERSION_OVERRIDE:
            # Manual version override via widget — useful for debugging or recovery
            checkpoint_version = int(STARTING_VERSION_OVERRIDE)

        # Get the current version of the source table for lineage tracking
        # _source_version tells downstream teams "this bronze row came from share version N"
        try:
            version_info = spark.sql(f"DESCRIBE HISTORY {source_fqn} LIMIT 1").collect()
            source_version = version_info[0]["version"] if version_info else -1
        except Exception:
            # Shared tables may not support DESCRIBE HISTORY — fall back to -1
            source_version = -1

        if checkpoint_version is not None and not FORCE_FULL:
            # --- CDF Incremental Read ---
            # Read only the changes since the last checkpoint.
            # This is more efficient than a full table scan for large tables.
            print(f"  Mode: CDF incremental (startingVersion={checkpoint_version + 1})")
            df = (
                spark.read
                .option("readChangeFeed", "true")
                .option("startingVersion", checkpoint_version + 1)
                .table(source_fqn)
            )
            # Filter to only the "net new" data:
            # - insert: new row that didn't exist before
            # - update_postimage: the new value of a row that changed
            # We drop update_preimage and delete to keep bronze append-only
            df = df.filter(F.col("_change_type").isin("insert", "update_postimage"))
            # Drop CDF metadata columns — they belong to the CDF protocol, not the business data
            df = df.drop("_change_type", "_commit_version", "_commit_timestamp")
        else:
            # --- Full Table Read ---
            # Used on first run or when forced. Reads the entire shared table snapshot.
            print(f"  Mode: Full table read")
            df = spark.table(source_fqn)

        source_count = df.count()
        print(f"  Source rows: {source_count:,}")

        if source_count == 0:
            # No new data — skip the write to avoid creating empty Delta commits
            print(f"  ⚠ No new data — skipping write")
            existing_count = spark.table(bronze_fqn).count() if spark.catalog.tableExists(bronze_fqn) else 0
            ingestion_results.append((bronze_name, 0, existing_count, "NO_NEW_DATA"))
            continue

        # --- Add ingestion metadata ---
        # These columns are added by the recipient, not present in the shared table.
        # They enable lineage tracking, partitioning, and debugging.
        df_bronze = (
            df
            .withColumn("_ingested_at",     F.lit(INGESTION_TS).cast("timestamp"))   # When we read the data
            .withColumn("_source_share",    F.lit(SOURCE_SHARE))                      # Which share it came from
            .withColumn("_source_version",  F.lit(source_version).cast("long"))       # Provider table version at read time
            .withColumn("_batch_id",        F.lit(BATCH_ID))                          # Groups all rows from this run
            .withColumn("_ingested_date",   F.lit(INGESTION_TS[:10]).cast("date"))    # Partition key (YYYY-MM-DD)
        )

        # --- Write to bronze (append mode) ---
        # Append means we accumulate all ingestion runs — bronze is a historical log.
        # partitionBy(_ingested_date) enables efficient pruning when querying recent data.
        # mergeSchema allows new columns from the provider to flow through automatically.
        (
            df_bronze.write
            .format("delta")
            .mode("append")
            .partitionBy("_ingested_date")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_fqn)
        )

        bronze_count = spark.table(bronze_fqn).count()
        print(f"  Bronze rows: {bronze_count:,}")
        print(f"  ✓ Ingestion complete")

        # --- Save checkpoint ---
        # Record the source version we just processed.
        # On the next run, CDF will start from source_version + 1.
        write_checkpoint(stage_name, source_version, BATCH_ID)
        print(f"  ✓ Checkpoint saved (version={source_version})")

        ingestion_results.append((bronze_name, source_count, bronze_count, "SUCCESS"))

    except Exception as e:
        print(f"  ✗ Error: {e}")
        ingestion_results.append((bronze_name, 0, 0, f"ERROR: {e}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

# Build a summary DataFrame from the results collected in the loop above
summary_schema = StructType([
    StructField("table",       StringType()),
    StructField("source_rows", LongType()),    # Rows read from the share (or CDF)
    StructField("bronze_rows", LongType()),    # Total rows now in the bronze table (all batches)
    StructField("status",      StringType()),  # SUCCESS / NO_NEW_DATA / ERROR
])

spark.createDataFrame(ingestion_results, summary_schema).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Enable CDF and Auto-Optimize on Bronze Tables
# MAGIC
# MAGIC These table properties are set **after** the initial write so the table exists first.
# MAGIC
# MAGIC | Property | Purpose |
# MAGIC |----------|---------|
# MAGIC | `delta.enableChangeDataFeed` | Allows the silver notebook to read only CDF changes |
# MAGIC | `delta.autoOptimize.optimizeWrite` | Produces right-sized Parquet files during writes |
# MAGIC | `delta.autoOptimize.autoCompact` | Automatically merges small files in the background |

# COMMAND ----------

for _, bronze_name in TABLE_MAPPINGS:
    bronze_fqn = f"{BRONZE_SCHEMA}.{bronze_name}"
    print(f"Configuring {bronze_fqn}...")

    spark.sql(f"""
        ALTER TABLE {bronze_fqn} SET TBLPROPERTIES (
            delta.enableChangeDataFeed        = true,
            delta.autoOptimize.optimizeWrite  = true,
            delta.autoOptimize.autoCompact    = true
        )
    """)
    print(f"  ✓ CDF + auto-optimize enabled")

print("\nAll bronze tables configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Bronze Tables
# MAGIC
# MAGIC Confirm row counts and partition counts match expectations.
# MAGIC Each run adds a new partition (`_ingested_date`), so partition count grows over time.

# COMMAND ----------

print("Bronze Table Verification:\n")
print(f"{'Table':<50} {'Rows':>10} {'Partitions':>12}")
print("-" * 75)

for _, bronze_name in TABLE_MAPPINGS:
    bronze_fqn = f"{BRONZE_SCHEMA}.{bronze_name}"
    count      = spark.table(bronze_fqn).count()
    partitions = spark.sql(f"SHOW PARTITIONS {bronze_fqn}").count()
    print(f"{bronze_fqn:<50} {count:>10,} {partitions:>12}")

# COMMAND ----------

# Show a sample with all metadata columns visible
# This confirms _ingested_at, _source_version, _batch_id, etc. were correctly written
print("Sample: bronze_market_prices (showing metadata columns)\n")
(
    spark.table(f"{BRONZE_SCHEMA}.bronze_market_prices")
    .select("*")
    .limit(5)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage
# MAGIC
# MAGIC ```
# MAGIC Shared Table (Provider Workspace)    Bronze Table (Recipient Workspace)
# MAGIC ─────────────────────────────────    ──────────────────────────────────
# MAGIC market_prices_pjm            ───►   bronze_market_prices
# MAGIC gold_daily_trading_summary   ───►   bronze_daily_trading_summary
# MAGIC positions                    ───►   bronze_positions
# MAGIC iso_market                   ───►   bronze_iso_market
# MAGIC turbine_locations            ───►   bronze_turbine_locations
# MAGIC
# MAGIC Metadata added per row:
# MAGIC   _ingested_at      — UTC timestamp this row was written to bronze
# MAGIC   _source_share     — name of the Delta Share it came from
# MAGIC   _source_version   — provider table version at time of read
# MAGIC   _batch_id         — groups all rows from the same notebook run
# MAGIC   _ingested_date    — partition key (date portion of _ingested_at)
# MAGIC ```
# MAGIC
# MAGIC **Next:** Run `04_silver_bitemporal` to build SCD2 bitemporal silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: SQL-First Consumption of Shared Tables
# MAGIC
# MAGIC The PySpark ingestion above is the recommended pattern for the medallion pipeline.
# MAGIC However, shared tables can also be consumed directly via SQL for ad-hoc analysis.
# MAGIC The patterns below demonstrate the full range of SQL consumption options.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Live VIEW over a shared table
# MAGIC
# MAGIC A view that queries the shared table directly — zero copy, zero storage cost.
# MAGIC The view always returns the latest version of the data.
# MAGIC Useful for analysts who need current data without waiting for the pipeline to run.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view that reads through to the provider in real-time
# MAGIC -- No data is stored locally — every query hits the foreign catalog
# MAGIC CREATE OR REPLACE VIEW delta_sharing_demo.bronze.v_shared_market_prices AS
# MAGIC SELECT * FROM shared_energy_market.energy_trading.market_prices_pjm;
# MAGIC
# MAGIC -- Query the view — always current, reads from provider on each execution
# MAGIC SELECT count(*) AS row_count FROM delta_sharing_demo.bronze.v_shared_market_prices;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Materialized snapshot (CREATE TABLE AS SELECT)
# MAGIC
# MAGIC A full copy of the shared table at a point in time.
# MAGIC Useful for reproducible analysis where you need a stable dataset that won't change
# MAGIC as the provider updates their data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Copy the current state of the shared table into a local Delta table
# MAGIC -- _snapshot_at records exactly when this copy was taken for reproducibility
# MAGIC CREATE OR REPLACE TABLE delta_sharing_demo.bronze.snapshot_market_prices AS
# MAGIC SELECT *, current_timestamp() AS _snapshot_at
# MAGIC FROM shared_energy_market.energy_trading.market_prices_pjm;
# MAGIC
# MAGIC SELECT count(*) AS row_count FROM delta_sharing_demo.bronze.snapshot_market_prices;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Time-travel on shared tables
# MAGIC
# MAGIC If the provider has enabled history sharing (`WITH HISTORY` in notebook 01),
# MAGIC you can read the shared table as it existed at a specific version or timestamp.
# MAGIC This is useful for debugging, auditing, or recreating historical analyses.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VERSION AS OF: read the shared table at a specific Delta commit version
# MAGIC -- Version 0 is the initial write; higher versions are subsequent updates
# MAGIC SELECT count(*) AS rows_at_v0
# MAGIC FROM shared_energy_market.energy_trading.market_prices_pjm VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TIMESTAMP AS OF: read the shared table as it existed at a specific point in time
# MAGIC -- Useful for regulatory queries ("what did we see on date X?")
# MAGIC -- Uncomment and adjust the timestamp to match your data:
# MAGIC -- SELECT count(*) AS rows_at_time
# MAGIC -- FROM shared_energy_market.energy_trading.market_prices_pjm TIMESTAMP AS OF '2024-01-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 4: Partition pruning on bronze tables
# MAGIC
# MAGIC Bronze tables are partitioned by `_ingested_date`. Spark can skip entire partitions
# MAGIC when the WHERE clause filters on the partition key — this is called **partition pruning**.
# MAGIC Always include `_ingested_date` in filters for efficient queries against large bronze tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: Spark reads only the files in the matching partition directory
# MAGIC SELECT count(*) FROM delta_sharing_demo.bronze.bronze_market_prices
# MAGIC WHERE _ingested_date = current_date();
# MAGIC
# MAGIC -- BAD: Spark reads all partitions, then filters in memory (full scan)
# MAGIC -- SELECT count(*) FROM delta_sharing_demo.bronze.bronze_market_prices
# MAGIC -- WHERE _ingested_at > '2024-01-01';
# MAGIC -- Use EXPLAIN COST to verify pruning is happening before running at scale

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Consumption Pattern Summary
# MAGIC
# MAGIC | Pattern | Use Case | Storage Cost | Data Freshness |
# MAGIC |---------|----------|-------------|----------------|
# MAGIC | `VIEW` over shared table | Live dashboards, ad-hoc queries | None (read-through) | Always current |
# MAGIC | `CTAS` snapshot | Reproducible historical analysis | Full copy stored locally | Point-in-time |
# MAGIC | `VERSION AS OF` | Debugging, auditing specific commits | None | Historical snapshot |
# MAGIC | `TIMESTAMP AS OF` | Regulatory queries ("what did we know on date X?") | None | Historical snapshot |
# MAGIC | Partition pruning | Efficient queries on large bronze tables | N/A | N/A |
