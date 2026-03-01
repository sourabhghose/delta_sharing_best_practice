# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver Bitemporal Modeling (Recipient Workspace)
# MAGIC
# MAGIC **Run this notebook on the Recipient Workspace.**
# MAGIC
# MAGIC The **silver layer** curates the raw bronze data into a business-ready, historically consistent
# MAGIC model using **SCD Type 2 bitemporal** tables.
# MAGIC
# MAGIC ## What is Bitemporal Modeling?
# MAGIC
# MAGIC Bitemporal modeling tracks **two independent time dimensions** for every record:
# MAGIC
# MAGIC | Dimension | Columns | Question it answers |
# MAGIC |-----------|---------|---------------------|
# MAGIC | **Valid Time** | `valid_from`, `valid_to` | "When was this fact true in the real world?" |
# MAGIC | **Transaction Time** | `recorded_at`, `superseded_at` | "When did our system learn about it?" |
# MAGIC
# MAGIC This allows queries like:
# MAGIC - *"What price did we record for region X on June 1st?"* (transaction time query)
# MAGIC - *"What was the actual market price on June 1st, per our latest data?"* (valid time query)
# MAGIC - *"Show me all versions of position Y we've ever held"* (full bitemporal history)
# MAGIC
# MAGIC ## How SCD2 MERGE Works
# MAGIC
# MAGIC On each run, we MERGE incoming bronze data against silver:
# MAGIC - **New entity** → insert a new row with `is_current = true`
# MAGIC - **Changed entity** → close the old row (`is_current = false`, `superseded_at = now`)
# MAGIC   and insert a new current row (note: two separate operations, but Delta's MERGE handles this)
# MAGIC - **Unchanged entity** → no action (MERGE skips it)
# MAGIC
# MAGIC Tables produced:
# MAGIC - `silver_prices_bitemporal` — market prices by region + interval
# MAGIC - `silver_positions_bitemporal` — trading positions by position ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

DEMO_CATALOG  = "delta_sharing_demo"
BRONZE_SCHEMA = f"{DEMO_CATALOG}.bronze"
SILVER_SCHEMA = f"{DEMO_CATALOG}.silver"

# CURRENT_TS: the transaction time — when *this run* processed the data
# MAX_TS: sentinel value for "currently open" records (not yet superseded)
CURRENT_TS = datetime.utcnow().isoformat()
MAX_TS     = "9999-12-31T23:59:59"

# Checkpoint control table — tracks last bronze version processed by each silver stage
CONTROL_TABLE = f"{DEMO_CATALOG}.control.control_cdf_checkpoint"

print(f"Bronze schema : {BRONZE_SCHEMA}")
print(f"Silver schema : {SILVER_SCHEMA}")
print(f"Current time  : {CURRENT_TS}  (used as transaction time for this run)")
print(f"Max timestamp : {MAX_TS}  (sentinel for open records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Helpers
# MAGIC
# MAGIC These functions read and write incremental processing state for each silver stage.
# MAGIC When a checkpoint exists, we read CDF from bronze (only changed rows) instead of
# MAGIC the full table. This dramatically reduces processing time on subsequent runs.

# COMMAND ----------

def get_checkpoint(stage_name):
    """
    Return the last Delta version of the source (bronze) table that was successfully
    processed into silver. Returns None if this is the first run for this stage.
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

def write_checkpoint(stage_name, version, run_id="manual"):
    """
    Upsert the checkpoint record for a silver stage.
    Called after a successful MERGE to record how far we've processed.
    """
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS tgt
        USING (SELECT
            '{stage_name}'      AS pipeline_stage,
            {version}           AS last_processed_version,
            current_timestamp() AS last_processed_ts,
            '{run_id}'          AS run_id
        ) AS src
        ON tgt.pipeline_stage = src.pipeline_stage
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Silver Prices Bitemporal Table
# MAGIC
# MAGIC The schema encodes both time dimensions for every price record.
# MAGIC `is_current = true` marks the most recently known state of each entity.
# MAGIC Historical versions have `is_current = false` and a non-MAX `superseded_at`.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.silver_prices_bitemporal (
        -- Business identity
        entity_key       STRING    COMMENT 'MD5 hash of (region, interval_start) — stable business key for MERGE',
        region           STRING    COMMENT 'Market region or pricing node (e.g. PJM-WEST)',
        interval_start   TIMESTAMP COMMENT 'Start of the pricing interval',
        interval_end     TIMESTAMP COMMENT 'End of the pricing interval (NULL if unknown)',
        price_type       STRING    COMMENT 'Price type (LMP, MCP, DAM, etc.)',
        price            DOUBLE    COMMENT 'Price value in $/MWh',

        -- Valid time (when the fact is true in the real world)
        valid_from       TIMESTAMP COMMENT 'When this price became effective in the market',
        valid_to         TIMESTAMP COMMENT 'When this price was superseded (9999-12-31 = currently effective)',

        -- Transaction time (when our system learned about it)
        recorded_at      TIMESTAMP COMMENT 'When we first ingested this version of the record',
        superseded_at    TIMESTAMP COMMENT 'When a newer version replaced this one (9999-12-31 = current)',

        -- SCD2 flag — shortcut for current-state queries
        is_current       BOOLEAN   COMMENT 'True = this is the most recent version we know about',

        -- Lineage
        _source_batch_id STRING    COMMENT 'Bronze batch ID for tracing back to the ingestion run'
    )
    USING DELTA
    TBLPROPERTIES (
        delta.enableChangeDataFeed       = true,   -- Enables downstream CDF reads by gold layer
        delta.autoOptimize.optimizeWrite = true    -- Right-size Parquet files on write
    )
    COMMENT 'SCD2 bitemporal market prices. Use is_current=true for current state, or query by valid_from/recorded_at for historical views.'
""")
print("✓ silver_prices_bitemporal table created/verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Silver Positions Bitemporal Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.silver_positions_bitemporal (
        -- Business identity
        entity_key       STRING    COMMENT 'MD5 hash of position_id — stable business key for MERGE',
        position_id      STRING    COMMENT 'Trading position identifier from the source system',
        instrument       STRING    COMMENT 'Financial instrument / ticker symbol',
        direction        STRING    COMMENT 'Trade direction: Long or Short',
        quantity         DOUBLE    COMMENT 'Position size in units',
        entry_price      DOUBLE    COMMENT 'Price at which the position was entered (NULL if unknown)',
        current_price    DOUBLE    COMMENT 'Current mark-to-market price (NULL if not available)',
        pnl              DOUBLE    COMMENT 'Profit and loss (NULL if not available)',

        -- Valid time
        valid_from       TIMESTAMP COMMENT 'When this position state became effective',
        valid_to         TIMESTAMP COMMENT 'When this state was superseded (9999-12-31 = active)',

        -- Transaction time
        recorded_at      TIMESTAMP COMMENT 'When we first recorded this version',
        superseded_at    TIMESTAMP COMMENT 'When a newer version replaced this one',

        is_current       BOOLEAN   COMMENT 'True = current state of this position',
        _source_batch_id STRING    COMMENT 'Bronze batch ID for lineage'
    )
    USING DELTA
    TBLPROPERTIES (
        delta.enableChangeDataFeed       = true,
        delta.autoOptimize.optimizeWrite = true
    )
    COMMENT 'SCD2 bitemporal trading positions. Use is_current=true for active positions.'
""")
print("✓ silver_positions_bitemporal table created/verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: SCD2 MERGE — Market Prices
# MAGIC
# MAGIC **Algorithm:**
# MAGIC 1. Read incoming data from bronze (CDF incremental if checkpoint exists, else full table)
# MAGIC 2. Derive `entity_key = MD5(region || interval_start)` as the stable business key
# MAGIC 3. Deduplicate to the latest row per entity_key (in case the same entity was ingested multiple times)
# MAGIC 4. MERGE into silver:
# MAGIC    - Matched + price changed → close the old record (`is_current = false`, `superseded_at = now`)
# MAGIC    - Not matched → insert a new current record (`is_current = true`)
# MAGIC 5. Write checkpoint so the next run reads only CDF changes

# COMMAND ----------

# --- Read source data (CDF incremental or full table) ---
silver_prices_stage = "silver_prices"
silver_prices_ckpt  = get_checkpoint(silver_prices_stage)

if silver_prices_ckpt is not None:
    # Incremental: only read bronze rows added or changed since the last checkpoint
    print(f"CDF incremental read: bronze_market_prices (startingVersion={silver_prices_ckpt + 1})")
    bronze_prices = (
        spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", silver_prices_ckpt + 1)
        .table(f"{BRONZE_SCHEMA}.bronze_market_prices")
    )
    # Keep only rows that represent net new or updated business data
    bronze_prices = bronze_prices.filter(F.col("_change_type").isin("insert", "update_postimage"))
    # Remove CDF protocol columns — they're not business data
    bronze_prices = bronze_prices.drop("_change_type", "_commit_version", "_commit_timestamp")
else:
    # First run: process the entire bronze table to build the initial silver state
    print("Full table read: bronze_market_prices (no checkpoint found — first run)")
    bronze_prices = spark.table(f"{BRONZE_SCHEMA}.bronze_market_prices")

# Inspect available columns — the shared table schema may vary between environments
price_columns = bronze_prices.columns
print(f"Bronze price columns: {price_columns}")

# COMMAND ----------

# --- Dynamic column detection ---
# We detect column names from the actual schema rather than hardcoding them.
# This makes the notebook resilient to minor schema variations in different environments.
region_col    = next((c for c in price_columns if c.lower() in ("region", "pricing_node", "node", "zone")), None)
interval_col  = next((c for c in price_columns if c.lower() in ("interval_start", "datetime_beginning_ept", "timestamp", "interval")), None)
price_col     = next((c for c in price_columns if c.lower() in ("price", "total_lmp_da", "lmp", "mcp")), None)
price_type_col= next((c for c in price_columns if c.lower() in ("price_type", "type", "market_type")), None)

print(f"Detected — region: {region_col}, interval: {interval_col}, price: {price_col}, price_type: {price_type_col}")

if region_col and interval_col:
    # --- Build the incoming dataset with SCD2 fields ---
    incoming_prices = (
        bronze_prices
        # entity_key: deterministic hash of the natural key so MERGE can join on it
        .withColumn("entity_key",  F.md5(F.concat_ws("||", F.col(region_col), F.col(interval_col).cast("string"))))
        .withColumn("region",      F.col(region_col))
        .withColumn("interval_start", F.col(interval_col).cast("timestamp"))
        .withColumn("price",       F.col(price_col).cast("double") if price_col else F.lit(None).cast("double"))
        .withColumn("price_type",  F.col(price_type_col) if price_type_col else F.lit("LMP"))
        # valid_from: the real-world effective time (here we use interval_start as a proxy)
        .withColumn("valid_from",  F.col(interval_col).cast("timestamp"))
        .withColumn("valid_to",    F.lit(MAX_TS).cast("timestamp"))   # Open — not yet superseded
        # recorded_at: transaction time — when THIS notebook run processed the row
        .withColumn("recorded_at", F.lit(CURRENT_TS).cast("timestamp"))
        .withColumn("superseded_at", F.lit(MAX_TS).cast("timestamp")) # Open — not yet replaced
        .withColumn("is_current",  F.lit(True))
        .withColumn("_source_batch_id", F.col("_batch_id"))
    )

    # Deduplicate: if the same entity_key appears multiple times (e.g. multiple batches in bronze),
    # keep only the most recently ingested row. This prevents duplicate MERGE conflicts.
    window = Window.partitionBy("entity_key").orderBy(F.col("_ingested_at").desc())
    incoming_prices = (
        incoming_prices
        .withColumn("_rn", F.row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn")
    )

    incoming_prices.createOrReplaceTempView("incoming_prices")
    print(f"\nIncoming price records (deduped): {incoming_prices.count():,}")
else:
    print("⚠ Could not detect region/interval columns. Inspect the bronze schema and adapt the column detection above.")

# COMMAND ----------

# Execute the SCD2 MERGE
if region_col and interval_col:
    # Handle optional interval_end column — not all source schemas include it
    interval_end_expr = "src.interval_end" if "interval_end" in price_columns else "NULL"

    spark.sql(f"""
        MERGE INTO {SILVER_SCHEMA}.silver_prices_bitemporal AS tgt
        USING incoming_prices AS src
        ON tgt.entity_key = src.entity_key AND tgt.is_current = true

        -- WHEN MATCHED: the entity exists and the price has changed
        -- → Close the current record by setting is_current=false and recording when it was superseded
        -- → The new record will be inserted by the NOT MATCHED clause on the next run
        --   (or by a separate INSERT if using MERGE with INSERT for updated records)
        WHEN MATCHED AND tgt.price != src.price THEN UPDATE SET
            tgt.is_current    = false,
            tgt.superseded_at = src.recorded_at,
            tgt.valid_to      = src.recorded_at

        -- WHEN NOT MATCHED: new entity we haven't seen before
        -- → Insert as a fresh current record
        WHEN NOT MATCHED THEN INSERT (
            entity_key, region, interval_start, interval_end, price_type, price,
            valid_from, valid_to, recorded_at, superseded_at, is_current, _source_batch_id
        ) VALUES (
            src.entity_key, src.region, src.interval_start, {interval_end_expr}, src.price_type, src.price,
            src.valid_from, src.valid_to, src.recorded_at, src.superseded_at, src.is_current, src._source_batch_id
        )
    """)

    # Save checkpoint: record the current bronze table version so the next run reads CDF from here+1
    bronze_version = spark.sql(f"DESCRIBE HISTORY {BRONZE_SCHEMA}.bronze_market_prices LIMIT 1").collect()[0]["version"]
    write_checkpoint(silver_prices_stage, bronze_version)
    print(f"✓ SCD2 MERGE complete for silver_prices_bitemporal")
    print(f"  Total rows (all versions): {spark.table(f'{SILVER_SCHEMA}.silver_prices_bitemporal').count():,}")
    print(f"  ✓ Checkpoint saved (bronze version={bronze_version})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: SCD2 MERGE — Trading Positions
# MAGIC
# MAGIC Same pattern as prices, applied to the positions table.
# MAGIC Change detection triggers on: quantity changed OR direction changed.

# COMMAND ----------

# --- Read source data ---
silver_positions_stage = "silver_positions"
silver_positions_ckpt  = get_checkpoint(silver_positions_stage)

if silver_positions_ckpt is not None:
    print(f"CDF incremental read: bronze_positions (startingVersion={silver_positions_ckpt + 1})")
    bronze_positions = (
        spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", silver_positions_ckpt + 1)
        .table(f"{BRONZE_SCHEMA}.bronze_positions")
    )
    bronze_positions = bronze_positions.filter(F.col("_change_type").isin("insert", "update_postimage"))
    bronze_positions = bronze_positions.drop("_change_type", "_commit_version", "_commit_timestamp")
else:
    print("Full table read: bronze_positions (no checkpoint — first run)")
    bronze_positions = spark.table(f"{BRONZE_SCHEMA}.bronze_positions")

pos_columns = bronze_positions.columns
print(f"Bronze position columns: {pos_columns}")

# COMMAND ----------

# Detect the natural key and business columns dynamically
pos_id_col    = next((c for c in pos_columns if c.lower() in ("position_id", "id", "trade_id")), None)
instrument_col= next((c for c in pos_columns if c.lower() in ("instrument", "symbol", "ticker")), None)
direction_col = next((c for c in pos_columns if c.lower() in ("direction", "side", "buy_sell")), None)
quantity_col  = next((c for c in pos_columns if c.lower() in ("quantity", "qty", "size", "volume")), None)

print(f"Detected — pos_id: {pos_id_col}, instrument: {instrument_col}, direction: {direction_col}, quantity: {quantity_col}")

if pos_id_col:
    incoming_positions = (
        bronze_positions
        .withColumn("entity_key",   F.md5(F.col(pos_id_col).cast("string")))
        .withColumn("position_id",  F.col(pos_id_col).cast("string"))
        .withColumn("instrument",   F.col(instrument_col) if instrument_col else F.lit(None))
        .withColumn("direction",    F.col(direction_col) if direction_col else F.lit(None))
        .withColumn("quantity",     F.col(quantity_col).cast("double") if quantity_col else F.lit(None).cast("double"))
        # entry_price, current_price, pnl — not available in this source; placeholder NULLs
        .withColumn("entry_price",  F.lit(None).cast("double"))
        .withColumn("current_price",F.lit(None).cast("double"))
        .withColumn("pnl",          F.lit(None).cast("double"))
        # valid_from: use _ingested_at as a proxy for when this position state became known
        .withColumn("valid_from",   F.col("_ingested_at"))
        .withColumn("valid_to",     F.lit(MAX_TS).cast("timestamp"))
        .withColumn("recorded_at",  F.lit(CURRENT_TS).cast("timestamp"))
        .withColumn("superseded_at",F.lit(MAX_TS).cast("timestamp"))
        .withColumn("is_current",   F.lit(True))
        .withColumn("_source_batch_id", F.col("_batch_id"))
    )

    # Deduplicate to latest per position entity_key
    window = Window.partitionBy("entity_key").orderBy(F.col("_ingested_at").desc())
    incoming_positions = (
        incoming_positions
        .withColumn("_rn", F.row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn")
    )
    incoming_positions.createOrReplaceTempView("incoming_positions")
    print(f"Incoming position records (deduped): {incoming_positions.count():,}")

    # Execute MERGE — change detected if quantity OR direction changed
    spark.sql(f"""
        MERGE INTO {SILVER_SCHEMA}.silver_positions_bitemporal AS tgt
        USING incoming_positions AS src
        ON tgt.entity_key = src.entity_key AND tgt.is_current = true

        -- Position changed: size increased/decreased or direction flipped
        WHEN MATCHED AND (
            tgt.quantity  != src.quantity OR
            tgt.direction != src.direction
        ) THEN UPDATE SET
            tgt.is_current    = false,
            tgt.superseded_at = src.recorded_at,
            tgt.valid_to      = src.recorded_at

        -- New position we haven't seen before
        WHEN NOT MATCHED THEN INSERT (
            entity_key, position_id, instrument, direction, quantity,
            entry_price, current_price, pnl,
            valid_from, valid_to, recorded_at, superseded_at, is_current, _source_batch_id
        ) VALUES (
            src.entity_key, src.position_id, src.instrument, src.direction, src.quantity,
            src.entry_price, src.current_price, src.pnl,
            src.valid_from, src.valid_to, src.recorded_at, src.superseded_at, src.is_current, src._source_batch_id
        )
    """)

    bronze_pos_version = spark.sql(f"DESCRIBE HISTORY {BRONZE_SCHEMA}.bronze_positions LIMIT 1").collect()[0]["version"]
    write_checkpoint(silver_positions_stage, bronze_pos_version)
    print(f"✓ SCD2 MERGE complete for silver_positions_bitemporal")
    print(f"  Total rows (all versions): {spark.table(f'{SILVER_SCHEMA}.silver_positions_bitemporal').count():,}")
    print(f"  ✓ Checkpoint saved (bronze version={bronze_pos_version})")
else:
    print("⚠ Could not detect position ID column. Inspect the bronze schema above and adapt column detection.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Bitemporal Demo Queries
# MAGIC
# MAGIC These queries demonstrate the three canonical query patterns against a bitemporal table.
# MAGIC Each answers a fundamentally different analytical question.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Transaction Time — "What did we know as of a specific time?"
# MAGIC
# MAGIC Filters on `recorded_at` and `superseded_at` to reconstruct the state of our system's
# MAGIC knowledge at a point in time. Useful for auditing: "what would a report have shown on date X?"

# COMMAND ----------

# Show prices as our system knew them at the time of this ingestion run
as_of_time = CURRENT_TS

query1 = f"""
    SELECT entity_key, region, interval_start, price, price_type,
           valid_from, valid_to, recorded_at, superseded_at, is_current
    FROM {SILVER_SCHEMA}.silver_prices_bitemporal
    WHERE recorded_at   <= '{as_of_time}'   -- We had recorded this row by this time
      AND superseded_at  > '{as_of_time}'   -- And it hadn't been replaced yet
    ORDER BY region, interval_start
    LIMIT 20
"""
print(f"Transaction time query: as-of {as_of_time}\n")
spark.sql(query1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Valid Time — "What is the current market price by region?"
# MAGIC
# MAGIC Filters on `is_current = true` to get the most recently known state.
# MAGIC This is the simplest and most common query pattern.

# COMMAND ----------

query2 = f"""
    SELECT entity_key, region, interval_start, price, price_type,
           valid_from, valid_to, recorded_at, is_current
    FROM {SILVER_SCHEMA}.silver_prices_bitemporal
    WHERE is_current = true    -- Only the most recent version of each entity
    ORDER BY region, interval_start DESC
    LIMIT 20
"""
print("Valid time query: current state of all prices\n")
spark.sql(query2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Full History — "Show me every version of a specific position"
# MAGIC
# MAGIC Returns all rows for a given entity (including historical versions with `is_current = false`).
# MAGIC This is the full bitemporal history — every state the position was ever in.

# COMMAND ----------

# Find a position to demonstrate with
sample = spark.sql(f"""
    SELECT entity_key, position_id FROM {SILVER_SCHEMA}.silver_positions_bitemporal LIMIT 1
""").collect()

if sample:
    sample_key = sample[0]["entity_key"]
    sample_id  = sample[0]["position_id"]

    query3 = f"""
        SELECT position_id, instrument, direction, quantity,
               valid_from, valid_to, recorded_at, superseded_at,
               is_current,
               -- Derived: how long this version was "current"
               TIMESTAMPDIFF(HOUR, recorded_at, superseded_at) AS hours_as_current_version
        FROM {SILVER_SCHEMA}.silver_positions_bitemporal
        WHERE entity_key = '{sample_key}'
        ORDER BY recorded_at   -- Chronological order of when we learned about each version
    """
    print(f"Full bitemporal history for position: {sample_id}\n")
    spark.sql(query3).display()
else:
    print("No positions loaded yet — run notebook 03 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Current Rows | Historical Rows | Query Pattern |
# MAGIC |-------|-------------|----------------|---------------|
# MAGIC | `silver_prices_bitemporal` | `is_current = true` | `is_current = false` | Filter on `entity_key`, `valid_from`, or `recorded_at` |
# MAGIC | `silver_positions_bitemporal` | `is_current = true` | `is_current = false` | Filter on `entity_key` or `position_id` |
# MAGIC
# MAGIC **Key Bitemporal Capabilities:**
# MAGIC - Transaction time queries: "What did our system know on date X?"
# MAGIC - Valid time queries: "What was actually true on date X?"
# MAGIC - Full history: every state we have ever recorded for any entity
# MAGIC
# MAGIC **Next:** Run `05_gold_aggregations` to build gold-layer summaries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Change History & Time Travel on Silver Tables
# MAGIC
# MAGIC Delta Lake automatically maintains a commit log for every table operation.
# MAGIC `DESCRIBE HISTORY` exposes this log, and `VERSION AS OF` / `TIMESTAMP AS OF`
# MAGIC allow you to query the table at any prior state — completely independently of
# MAGIC the bitemporal columns above.
# MAGIC
# MAGIC > **Two layers of history:**
# MAGIC > - **Bitemporal columns** (`valid_from`, `recorded_at`, etc.) — business-level history encoded in the data
# MAGIC > - **Delta time travel** (`VERSION AS OF`) — storage-level history of the table's physical state
# MAGIC > These are complementary: Delta time travel lets you recover from pipeline bugs;
# MAGIC > bitemporal columns answer business analytical questions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DESCRIBE HISTORY — Delta Commit Log

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows every operation ever run against this table:
# MAGIC -- CREATE, MERGE, OPTIMIZE, VACUUM, ALTER TABLE, etc.
# MAGIC -- Includes the user who ran it, the timestamp, operation parameters, and metrics.
# MAGIC DESCRIBE HISTORY delta_sharing_demo.silver.silver_prices_bitemporal;

# COMMAND ----------

# MAGIC %md
# MAGIC ### VERSION AS OF — Read Silver at a Prior Version
# MAGIC
# MAGIC Useful to verify what the table looked like before a specific MERGE ran,
# MAGIC or to roll back an investigation to a known-good state.

# COMMAND ----------

# Compare row counts across versions to see how the table grew with each MERGE
print("Row count at version 0 (initial CREATE TABLE — empty):")
spark.sql(f"""
    SELECT count(*) AS row_count
    FROM {SILVER_SCHEMA}.silver_prices_bitemporal VERSION AS OF 0
""").display()

# Get the latest version number for comparison
latest_version = spark.sql(
    f"DESCRIBE HISTORY {SILVER_SCHEMA}.silver_prices_bitemporal LIMIT 1"
).collect()[0]["version"]

print(f"\nRow count at version {latest_version} (current state):")
spark.sql(f"""
    SELECT count(*) AS row_count
    FROM {SILVER_SCHEMA}.silver_prices_bitemporal VERSION AS OF {latest_version}
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TIMESTAMP AS OF — Read Silver at a Prior Point in Time
# MAGIC
# MAGIC Useful when you know *when* something went wrong but not which version number.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment and adjust the timestamp to query historical state:
# MAGIC -- SELECT count(*) AS row_count
# MAGIC -- FROM delta_sharing_demo.silver.silver_prices_bitemporal
# MAGIC -- TIMESTAMP AS OF '2024-06-01T00:00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF `_change_type` Semantics
# MAGIC
# MAGIC When downstream stages read CDF from silver, each row includes a `_change_type` column:
# MAGIC
# MAGIC | `_change_type` | Meaning | Include in downstream processing? |
# MAGIC |----------------|---------|----------------------------------|
# MAGIC | `insert` | New row added to the table | Yes |
# MAGIC | `update_preimage` | Row state **before** an UPDATE | No — use postimage |
# MAGIC | `update_postimage` | Row state **after** an UPDATE | Yes |
# MAGIC | `delete` | Row was deleted | Depends on use case |
# MAGIC
# MAGIC For the gold layer, filter to `insert` + `update_postimage` to get the net new data.

# COMMAND ----------

# Read a sample of CDF from silver to demonstrate the change types
# This shows what the gold layer sees when it reads CDF from silver
try:
    cdf_sample = (
        spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)   # Start from version 1 (skip the CREATE TABLE at version 0)
        .table(f"{SILVER_SCHEMA}.silver_prices_bitemporal")
        .select("entity_key", "region", "price", "is_current",
                "_change_type", "_commit_version", "_commit_timestamp")
        .limit(10)
    )
    print("Sample CDF records from silver_prices_bitemporal:\n")
    cdf_sample.display()
except Exception as e:
    print(f"CDF note: {e}")
    print("This is expected if no changes have been made since table creation.")
