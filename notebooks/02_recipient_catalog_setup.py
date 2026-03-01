# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Recipient Catalog Setup (Recipient Workspace)
# MAGIC
# MAGIC **Run this notebook on the Recipient Workspace.**
# MAGIC
# MAGIC This is the first notebook to run on the recipient side. It sets up the full workspace
# MAGIC environment so subsequent notebooks can read shared data and build the medallion pipeline.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create a **provider** — a reference to the Provider Workspace in Unity Catalog
# MAGIC 2. Create a **foreign catalog** — a local catalog backed by the remote share
# MAGIC 3. Create the **demo catalog** (`delta_sharing_demo`) with all pipeline schemas
# MAGIC 4. Create the **CDF checkpoint** control table for incremental processing
# MAGIC 5. Verify shared tables are readable from the recipient side
# MAGIC
# MAGIC > **Prerequisites:** Notebook 01 must have been run on the Provider Workspace first.
# MAGIC > The share `energy_market_share` and a D2D recipient must already exist on the provider side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update the values below before running:
# MAGIC - `PROVIDER_SHARING_ID`: Find this in the Provider Workspace under
# MAGIC   **Settings → Workspace Settings → General → Sharing Identifier**

# COMMAND ----------

# The Provider Workspace's sharing identifier — uniquely identifies it in Databricks
# Format: "cloud:region:workspace-identifier"
# Find this at: Provider Workspace → Settings → Workspace Settings → General → Sharing Identifier
PROVIDER_SHARING_ID = "aws:us-west-2:your-provider-workspace"  # UPDATE THIS

# A local Unity Catalog name for the provider — used to reference it in SQL statements
PROVIDER_NAME = "energy_market_provider"

# The share name as configured by the provider in notebook 01
SHARE_NAME = "energy_market_share"

# Local name for the foreign catalog (read-only, backed by the remote share)
# This catalog appears in Unity Catalog just like any other local catalog
SHARED_CATALOG = "shared_energy_market"

# Our working catalog where the medallion pipeline will write its tables
DEMO_CATALOG = "delta_sharing_demo"

# All schemas we need in the demo catalog
# Each layer has its own schema to maintain clear separation of concerns
SCHEMAS = ["bronze", "silver", "gold", "audit", "control"]

print(f"Provider name    : {PROVIDER_NAME}")
print(f"Provider ID      : {PROVIDER_SHARING_ID}")
print(f"Share name       : {SHARE_NAME}")
print(f"Shared catalog   : {SHARED_CATALOG}   (read-only, backed by remote share)")
print(f"Demo catalog     : {DEMO_CATALOG}   (read-write, medallion pipeline output)")
print(f"Schemas          : {', '.join(SCHEMAS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Provider
# MAGIC
# MAGIC A **provider** in Unity Catalog is a reference to an external Databricks workspace
# MAGIC that has shared data with us. Creating the provider does not transfer any data —
# MAGIC it simply registers the remote workspace as a trusted source.
# MAGIC
# MAGIC After creating the provider, we can list its available shares and mount them as
# MAGIC foreign catalogs in Step 2.

# COMMAND ----------

print(f"Registering provider '{PROVIDER_NAME}'...")
print(f"  Provider sharing ID: {PROVIDER_SHARING_ID}")

try:
    spark.sql(f"""
        CREATE PROVIDER IF NOT EXISTS {PROVIDER_NAME}
        USING ID '{PROVIDER_SHARING_ID}'
    """)
    print(f"✓ Provider '{PROVIDER_NAME}' registered.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"⚠ Provider already exists — skipping.")
    else:
        raise e

# COMMAND ----------

# List shares available from the provider to confirm connectivity
# If this fails, check that the provider sharing ID is correct and
# that the provider has created a recipient for this workspace
print(f"Shares available from provider '{PROVIDER_NAME}':\n")
spark.sql(f"SHOW SHARES IN PROVIDER {PROVIDER_NAME}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Foreign Catalog from Share
# MAGIC
# MAGIC A **foreign catalog** mounts a remote share as a local Unity Catalog catalog.
# MAGIC Once created, you can query its tables with standard SQL — the foreign catalog
# MAGIC handles credential management and secure data transfer transparently.
# MAGIC
# MAGIC The foreign catalog is **read-only**: you cannot INSERT, UPDATE, or DELETE through it.
# MAGIC The provider retains full control of the underlying data.

# COMMAND ----------

print(f"Creating foreign catalog '{SHARED_CATALOG}'...")
print(f"  Backed by share: {PROVIDER_NAME}.{SHARE_NAME}")

try:
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {SHARED_CATALOG}
        USING SHARE {PROVIDER_NAME}.{SHARE_NAME}
    """)
    print(f"✓ Foreign catalog '{SHARED_CATALOG}' created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"⚠ Catalog already exists — skipping.")
    else:
        raise e

# COMMAND ----------

# Confirm the schemas exposed by the share are visible in the foreign catalog
print(f"Schemas in '{SHARED_CATALOG}':\n")
spark.sql(f"SHOW SCHEMAS IN {SHARED_CATALOG}").display()

# COMMAND ----------

# Enumerate all shared tables — useful to verify the share configuration matches expectations
# The provider controls what appears here; the recipient cannot modify this list
print(f"Tables in '{SHARED_CATALOG}':\n")
for schema_row in spark.sql(f"SHOW SCHEMAS IN {SHARED_CATALOG}").collect():
    schema_name = schema_row["databaseName"]
    if schema_name == "information_schema":
        continue  # Skip the system schema
    tables = spark.sql(f"SHOW TABLES IN {SHARED_CATALOG}.{schema_name}").collect()
    for t in tables:
        print(f"  {SHARED_CATALOG}.{schema_name}.{t['tableName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Demo Catalog and Schemas
# MAGIC
# MAGIC This is the **recipient's own catalog** — fully writable, owned by the recipient.
# MAGIC The medallion pipeline will write all processed data here.
# MAGIC
# MAGIC Schema layout:
# MAGIC | Schema | Purpose |
# MAGIC |--------|---------|
# MAGIC | `bronze` | Raw append-only data ingested from the share |
# MAGIC | `silver` | Curated SCD2 bitemporal tables |
# MAGIC | `gold` | Pre-aggregated summaries for analytics |
# MAGIC | `audit` | Permanent CDF archive for compliance |
# MAGIC | `control` | Operational metadata: DQ results, freshness, checkpoints |

# COMMAND ----------

print(f"Creating catalog '{DEMO_CATALOG}'...")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {DEMO_CATALOG}")
print(f"✓ Catalog '{DEMO_CATALOG}' created.\n")

# Create each schema — IF NOT EXISTS makes this idempotent for re-runs
for schema in SCHEMAS:
    full_schema = f"{DEMO_CATALOG}.{schema}"
    print(f"  Creating schema '{full_schema}'...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    print(f"  ✓ Schema '{full_schema}' created.")

print(f"\nAll schemas created in '{DEMO_CATALOG}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create CDF Checkpoint Table
# MAGIC
# MAGIC This control table tracks incremental processing state across all pipeline stages.
# MAGIC Each stage (bronze, silver, gold) records the last Delta **version** it successfully
# MAGIC processed. On the next run, the stage reads CDF starting from `last_processed_version + 1`
# MAGIC instead of re-reading the full table.
# MAGIC
# MAGIC **Why version and not timestamp?**
# MAGIC Delta versions are monotonically increasing integers — they uniquely identify a commit
# MAGIC and are more reliable than timestamps for tracking CDF position.

# COMMAND ----------

print("Creating control_cdf_checkpoint table...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DEMO_CATALOG}.control.control_cdf_checkpoint (
        pipeline_stage         STRING    COMMENT 'Stage identifier, e.g. bronze_market_prices, silver_prices, gold_daily_price_summary',
        last_processed_version LONG      COMMENT 'Last Delta table version successfully read and processed by this stage',
        last_processed_ts      TIMESTAMP COMMENT 'Wall-clock time when the checkpoint was last written',
        run_id                 STRING    COMMENT 'Batch ID or Databricks Job run ID for traceability'
    )
    USING DELTA
    COMMENT 'CDF incremental processing checkpoints — one row per pipeline stage'
""")
print("✓ control_cdf_checkpoint table created.")

# COMMAND ----------

# Verify everything was created correctly
print(f"Schemas in '{DEMO_CATALOG}':\n")
spark.sql(f"SHOW SCHEMAS IN {DEMO_CATALOG}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Shared Tables Are Readable
# MAGIC
# MAGIC A quick sanity check to confirm the foreign catalog is working and the recipient
# MAGIC has the correct permissions to read each shared table.
# MAGIC
# MAGIC If any table shows an error, check:
# MAGIC 1. The provider granted SELECT on the share to this recipient
# MAGIC 2. The foreign catalog is pointing to the correct share
# MAGIC 3. The table was actually added to the share in notebook 01

# COMMAND ----------

# Read row counts for all shared tables — a COUNT(*) query is sufficient
# to confirm connectivity and authorization without transferring all data
shared_tables = {
    "market_prices_pjm":          f"{SHARED_CATALOG}.energy_trading.market_prices_pjm",
    "daily_trading_summary":      f"{SHARED_CATALOG}.energy_trading.gold_daily_trading_summary",
    "positions":                  f"{SHARED_CATALOG}.energy_trading.positions",
    "iso_market":                 f"{SHARED_CATALOG}.power_generation.iso_market",
    "turbine_locations":          f"{SHARED_CATALOG}.power_generation.turbine_locations",
}

print("Row counts for shared tables:\n")
print(f"{'Table':<40} {'Row Count':>12}")
print("-" * 55)

for name, fqn in shared_tables.items():
    try:
        count = spark.table(fqn).count()
        print(f"{fqn:<40} {count:>12,}")
    except Exception as e:
        print(f"{fqn:<40} {'ERROR':>12}  ({e})")

# COMMAND ----------

# Preview sample rows from the most important shared table
# This confirms the schema is as expected and data is flowing
print("Sample: market_prices_pjm (5 rows)\n")
spark.table(f"{SHARED_CATALOG}.energy_trading.market_prices_pjm").limit(5).display()

# COMMAND ----------

# Preview positions table — used in the silver SCD2 demo
print("Sample: positions (5 rows)\n")
spark.table(f"{SHARED_CATALOG}.energy_trading.positions").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Item | Status |
# MAGIC |------|--------|
# MAGIC | Provider registered | ✓ |
# MAGIC | Foreign catalog `shared_energy_market` created (read-only) | ✓ |
# MAGIC | Demo catalog `delta_sharing_demo` with 5 schemas | ✓ |
# MAGIC | CDF checkpoint control table created | ✓ |
# MAGIC | Shared tables readable | ✓ |
# MAGIC
# MAGIC **Next:** Run `03_bronze_ingestion` to start building the medallion pipeline.
# MAGIC On the first run, use `force_full_reload = true` to load the full table baseline.
# MAGIC Subsequent runs will use CDF incremental reads automatically.
