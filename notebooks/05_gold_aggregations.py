# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Aggregations (Recipient Workspace)
# MAGIC
# MAGIC **Run this notebook on the Recipient Workspace.**
# MAGIC
# MAGIC The **gold layer** is the final stage in the medallion pipeline.
# MAGIC It aggregates the curated silver data into analytics-ready summaries
# MAGIC that are optimized for BI tools and dashboards.
# MAGIC
# MAGIC Design principles:
# MAGIC - **Pre-aggregated** — expensive groupBy/agg operations run once at pipeline time, not at query time
# MAGIC - **Liquid clustering** — Delta's adaptive clustering auto-organizes files for the most common query patterns
# MAGIC - **Z-ORDER** — co-locates related values on disk to minimize files read per query
# MAGIC - **CDF enabled** — allows the downstream audit layer to track changes to gold tables
# MAGIC - **Incremental refresh** — on subsequent runs, only recomputes groups affected by new silver data
# MAGIC
# MAGIC Tables produced:
# MAGIC - `gold_daily_price_summary` — avg/min/max/stddev price by region × date
# MAGIC - `gold_trading_position_summary` — net long/short position by instrument

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F

DEMO_CATALOG  = "delta_sharing_demo"
SILVER_SCHEMA = f"{DEMO_CATALOG}.silver"
GOLD_SCHEMA   = f"{DEMO_CATALOG}.gold"

# The checkpoint table tracks the last silver version processed by each gold stage
CONTROL_TABLE = f"{DEMO_CATALOG}.control.control_cdf_checkpoint"

print(f"Silver schema : {SILVER_SCHEMA}")
print(f"Gold schema   : {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Helpers
# MAGIC
# MAGIC Same pattern as notebooks 03 and 04 — each gold stage tracks the last
# MAGIC silver version it processed. On the next run, CDF lets us recompute
# MAGIC only the groups affected by new silver data instead of the entire table.

# COMMAND ----------

def get_checkpoint(stage_name):
    """Return last processed silver version for this gold stage, or None (first run)."""
    try:
        rows = spark.sql(f"""
            SELECT last_processed_version FROM {CONTROL_TABLE}
            WHERE pipeline_stage = '{stage_name}'
        """).collect()
        if rows:
            return rows[0]["last_processed_version"]
    except Exception:
        pass
    return None

def write_checkpoint(stage_name, version):
    """Upsert the checkpoint after a successful gold refresh."""
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS tgt
        USING (SELECT
            '{stage_name}'      AS pipeline_stage,
            {version}           AS last_processed_version,
            current_timestamp() AS last_processed_ts,
            'gold_refresh'      AS run_id
        ) AS src
        ON tgt.pipeline_stage = src.pipeline_stage
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Gold Daily Price Summary
# MAGIC
# MAGIC Aggregates current silver prices into daily statistics per (region, price_type).
# MAGIC `CLUSTER BY (price_date, region)` uses liquid clustering — Delta automatically
# MAGIC co-locates files along these dimensions as data grows, without requiring manual ZORDER runs.

# COMMAND ----------

# CREATE OR REPLACE to handle schema changes cleanly during development
# In production, use ALTER TABLE to add columns without dropping history
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.gold_daily_price_summary (
        price_date     DATE      COMMENT 'Trading date derived from interval_start',
        region         STRING    COMMENT 'Market region or pricing node',
        price_type     STRING    COMMENT 'Price type (LMP, MCP, etc.)',
        avg_price      DOUBLE    COMMENT 'Volume-weighted average price for the day ($/MWh)',
        min_price      DOUBLE    COMMENT 'Minimum intra-day price ($/MWh)',
        max_price      DOUBLE    COMMENT 'Maximum intra-day price ($/MWh)',
        stddev_price   DOUBLE    COMMENT 'Price volatility (standard deviation)',
        interval_count LONG      COMMENT 'Number of pricing intervals in the day',
        price_spread   DOUBLE    COMMENT 'Intra-day price range: max - min ($/MWh)',
        _updated_at    TIMESTAMP COMMENT 'When this summary row was last recomputed'
    )
    USING DELTA
    CLUSTER BY (price_date, region)   -- Liquid clustering: auto-organizes files for common query pattern
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true   -- Allow audit layer to track gold changes
    )
    COMMENT 'Gold: daily market price statistics per region. Cluster keys: price_date, region.'
""")
print("✓ gold_daily_price_summary table created.")

# COMMAND ----------

# --- Full table population (Step 1, first run) ---
# Filter to current silver records only — historical SCD2 rows are excluded from aggregations
# because they represent superseded prices, not the currently known state
silver_prices = spark.table(f"{SILVER_SCHEMA}.silver_prices_bitemporal").filter("is_current = true")

daily_summary = (
    silver_prices
    .withColumn("price_date", F.to_date("interval_start"))   # Extract date from interval timestamp
    .groupBy("price_date", "region", "price_type")
    .agg(
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.stddev("price").alias("stddev_price"),
        F.count("*").alias("interval_count"),
    )
    .withColumn("price_spread", F.col("max_price") - F.col("min_price"))
    .withColumn("_updated_at", F.current_timestamp())
)

(
    daily_summary.write
    .format("delta")
    .mode("overwrite")         # Full overwrite on initial load — incremental MERGE used on subsequent runs
    .saveAsTable(f"{GOLD_SCHEMA}.gold_daily_price_summary")
)

row_count = spark.table(f"{GOLD_SCHEMA}.gold_daily_price_summary").count()
print(f"✓ gold_daily_price_summary populated: {row_count:,} rows")

# COMMAND ----------

# Preview the result — sorted by date and region for readability
print("Daily Price Summary (top 20 rows):\n")
(
    spark.table(f"{GOLD_SCHEMA}.gold_daily_price_summary")
    .orderBy("price_date", "region")
    .limit(20)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Gold Trading Position Summary
# MAGIC
# MAGIC Rolls up current trading positions into net exposure per instrument.
# MAGIC Used by risk managers and traders for portfolio-level views.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.gold_trading_position_summary (
        instrument      STRING    COMMENT 'Financial instrument or ticker symbol',
        total_long_qty  DOUBLE    COMMENT 'Sum of all Long position quantities',
        total_short_qty DOUBLE    COMMENT 'Sum of all Short position quantities',
        net_position    DOUBLE    COMMENT 'Net exposure: total_long - total_short',
        position_count  LONG      COMMENT 'Number of individual positions in this instrument',
        avg_quantity    DOUBLE    COMMENT 'Average position size',
        _updated_at     TIMESTAMP COMMENT 'When this summary was last computed'
    )
    USING DELTA
    CLUSTER BY (instrument)
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
    COMMENT 'Gold: net trading position by instrument. A positive net_position = net long exposure.'
""")
print("✓ gold_trading_position_summary table created.")

# COMMAND ----------

# Aggregate current positions only — closed/historical SCD2 rows are excluded
silver_positions = spark.table(f"{SILVER_SCHEMA}.silver_positions_bitemporal").filter("is_current = true")

position_summary = (
    silver_positions
    .groupBy("instrument")
    .agg(
        # Long positions: count when direction is LONG, else 0
        F.sum(F.when(F.upper("direction") == "LONG",  F.col("quantity")).otherwise(0)).alias("total_long_qty"),
        # Short positions: count when direction is SHORT, else 0
        F.sum(F.when(F.upper("direction") == "SHORT", F.col("quantity")).otherwise(0)).alias("total_short_qty"),
        F.count("*").alias("position_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .withColumn("net_position", F.col("total_long_qty") - F.col("total_short_qty"))
    .withColumn("_updated_at", F.current_timestamp())
)

(
    position_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_SCHEMA}.gold_trading_position_summary")
)

row_count = spark.table(f"{GOLD_SCHEMA}.gold_trading_position_summary").count()
print(f"✓ gold_trading_position_summary populated: {row_count:,} rows")

# COMMAND ----------

print("Trading Position Summary:\n")
spark.table(f"{GOLD_SCHEMA}.gold_trading_position_summary").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Optimize Gold Tables
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY` rearranges data files so rows with the same key values
# MAGIC are physically co-located. This minimizes the number of files Spark reads when
# MAGIC filtering on those columns — critical for BI query performance.
# MAGIC
# MAGIC Run OPTIMIZE periodically (e.g. after each pipeline run or daily).

# COMMAND ----------

print("Optimizing gold_daily_price_summary (ZORDER BY price_date, region)...")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.gold_daily_price_summary ZORDER BY (price_date, region)")
print("✓ Optimized\n")

print("Optimizing gold_trading_position_summary (ZORDER BY instrument)...")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.gold_trading_position_summary ZORDER BY (instrument)")
print("✓ Optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary
# MAGIC
# MAGIC | Table | Key Dimensions | Use Case |
# MAGIC |-------|---------------|---------|
# MAGIC | `gold_daily_price_summary` | price_date, region | Price volatility analysis, regional comparisons |
# MAGIC | `gold_trading_position_summary` | instrument | Risk management, net exposure monitoring |
# MAGIC
# MAGIC **Features:**
# MAGIC - Liquid clustering on query columns for adaptive file organization
# MAGIC - Z-ORDER optimization applied for file skipping
# MAGIC - CDF enabled so the audit layer can track gold-level changes
# MAGIC
# MAGIC **Next:** Run `06_data_quality_monitoring` for DQ checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Incremental Gold Refresh via CDF
# MAGIC
# MAGIC **Why incremental?**
# MAGIC For large datasets, recomputing `gold_daily_price_summary` in full every run is expensive.
# MAGIC Instead, we read CDF from silver to find which `(price_date, region)` groups changed,
# MAGIC then recompute and MERGE only those groups. Unchanged groups are untouched.
# MAGIC
# MAGIC **Pattern:**
# MAGIC 1. Read CDF from silver since `gold_checkpoint + 1`
# MAGIC 2. Extract the distinct `(price_date, region)` keys that changed
# MAGIC 3. Re-aggregate only those keys from the full silver table (we need all intervals, not just changed ones)
# MAGIC 4. MERGE results into gold — UPDATE existing rows, INSERT new ones
# MAGIC 5. Write checkpoint
# MAGIC
# MAGIC > **Note:** The full OVERWRITE in Steps 1-2 is fine for small/medium datasets.
# MAGIC > This section demonstrates the production CDF-incremental pattern for at-scale use.

# COMMAND ----------

gold_stage = "gold_daily_price_summary"
gold_ckpt  = get_checkpoint(gold_stage)

if gold_ckpt is not None:
    # --- Incremental refresh path ---
    print(f"Incremental gold refresh: reading CDF from silver (startingVersion={gold_ckpt + 1})")

    # Step 1: Find what changed in silver since the last gold run
    cdf_changes = (
        spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", gold_ckpt + 1)
        .table(f"{SILVER_SCHEMA}.silver_prices_bitemporal")
    )

    # Step 2: Extract the distinct (price_date, region) groups that were touched
    # Any change to a group (insert, update, delete) requires recomputing that group's aggregates
    affected_groups = (
        cdf_changes
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .withColumn("price_date", F.to_date("interval_start"))
        .select("price_date", "region")
        .distinct()
    )

    affected_count = affected_groups.count()
    print(f"  Affected (price_date, region) groups: {affected_count}")

    if affected_count > 0:
        # Step 3: Re-aggregate the affected groups from the full silver table
        # We must re-read from silver (not just from CDF) because we need ALL intervals
        # in a (price_date, region) group to compute correct avg/min/max, not just the changed ones
        affected_groups.createOrReplaceTempView("affected_groups")

        recomputed = spark.sql(f"""
            SELECT
                CAST(s.interval_start AS DATE)   AS price_date,
                s.region,
                s.price_type,
                AVG(s.price)                     AS avg_price,
                MIN(s.price)                     AS min_price,
                MAX(s.price)                     AS max_price,
                STDDEV(s.price)                  AS stddev_price,
                COUNT(*)                         AS interval_count,
                MAX(s.price) - MIN(s.price)      AS price_spread,
                current_timestamp()              AS _updated_at
            FROM {SILVER_SCHEMA}.silver_prices_bitemporal s
            -- INNER JOIN limits the re-aggregation to affected groups only
            INNER JOIN affected_groups ag
                ON CAST(s.interval_start AS DATE) = ag.price_date
               AND s.region = ag.region
            WHERE s.is_current = true   -- Exclude historical SCD2 versions
            GROUP BY CAST(s.interval_start AS DATE), s.region, s.price_type
        """)
        recomputed.createOrReplaceTempView("recomputed_gold")

        # Step 4: MERGE into gold — more precise than a full OVERWRITE
        # Existing rows for affected groups are updated; new (price_date, region) combinations are inserted
        spark.sql(f"""
            MERGE INTO {GOLD_SCHEMA}.gold_daily_price_summary AS tgt
            USING recomputed_gold AS src
            ON  tgt.price_date = src.price_date
            AND tgt.region     = src.region
            AND tgt.price_type = src.price_type
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  ✓ Incremental MERGE complete ({affected_count} groups refreshed)")
    else:
        print("  No changes to process — gold table is already up to date.")

    # Step 5: Save checkpoint to current silver version
    silver_version = spark.sql(
        f"DESCRIBE HISTORY {SILVER_SCHEMA}.silver_prices_bitemporal LIMIT 1"
    ).collect()[0]["version"]
    write_checkpoint(gold_stage, silver_version)
    print(f"  ✓ Checkpoint updated (silver version={silver_version})")

else:
    # --- First run path ---
    # Full overwrite already ran in Step 1 above.
    # Just record the starting checkpoint so the next run can use CDF.
    silver_version = spark.sql(
        f"DESCRIBE HISTORY {SILVER_SCHEMA}.silver_prices_bitemporal LIMIT 1"
    ).collect()[0]["version"]
    write_checkpoint(gold_stage, silver_version)
    print(f"First run: checkpoint initialized at silver version {silver_version}.")
    print("Next run will use CDF incremental refresh instead of a full recompute.")
