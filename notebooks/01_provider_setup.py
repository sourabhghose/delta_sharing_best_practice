# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Provider Setup (Provider Workspace)
# MAGIC
# MAGIC **Run this notebook on the Provider Workspace.**
# MAGIC
# MAGIC This notebook configures the data sharing infrastructure on the provider side:
# MAGIC 1. Enables Change Data Feed (CDF) on provider tables so recipients can read incremental changes
# MAGIC 2. Creates a share named `energy_market_share` that groups tables to be exposed
# MAGIC 3. Adds tables to the share with history sharing enabled (required for CDF on shared tables)
# MAGIC 4. Creates a Databricks-to-Databricks (D2D) recipient referencing the Recipient Workspace
# MAGIC 5. Grants SELECT on the share to the recipient and verifies the configuration
# MAGIC
# MAGIC > **Prerequisite:** The tables in `energy_utilities` must exist before running this notebook.
# MAGIC > Run notebook `00_demo_overview` first to understand the overall architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update the values below before running:
# MAGIC - `RECIPIENT_SHARING_ID`: Find this in the Recipient Workspace under
# MAGIC   **Settings → Workspace Settings → General → Sharing Identifier**

# COMMAND ----------

# Provider catalog and schemas where the source data lives
PROVIDER_CATALOG   = "energy_utilities"
TRADING_SCHEMA     = "energy_trading"
GENERATION_SCHEMA  = "power_generation"

# Name of the Delta Share to create
# A "share" is a named collection of tables the provider makes available
SHARE_NAME = "energy_market_share"

# Recipient configuration
# The sharing identifier uniquely identifies the Recipient Workspace in Databricks
# Format: "cloud:region:workspace-name"
# Find it at: Recipient Workspace → Settings → General → Sharing Identifier
RECIPIENT_SHARING_ID = "aws:us-west-2:your-recipient-workspace"  # UPDATE THIS
RECIPIENT_NAME       = "energy_copilot_recipient"

# All tables to include in the share
# These will be accessible to the recipient via the foreign catalog
TABLES_TO_SHARE = [
    f"{PROVIDER_CATALOG}.{TRADING_SCHEMA}.market_prices_pjm",
    f"{PROVIDER_CATALOG}.{TRADING_SCHEMA}.gold_daily_trading_summary",
    f"{PROVIDER_CATALOG}.{TRADING_SCHEMA}.positions",
    f"{PROVIDER_CATALOG}.{GENERATION_SCHEMA}.iso_market",
    f"{PROVIDER_CATALOG}.{GENERATION_SCHEMA}.turbine_locations",
]

# Subset of tables where CDF will be enabled
# Static reference tables (e.g. turbine_locations) typically don't need CDF
# because they change infrequently and a full reload is acceptable
CDF_TABLES = [
    f"{PROVIDER_CATALOG}.{TRADING_SCHEMA}.market_prices_pjm",
    f"{PROVIDER_CATALOG}.{TRADING_SCHEMA}.gold_daily_trading_summary",
    f"{PROVIDER_CATALOG}.{TRADING_SCHEMA}.positions",
]

print("Share name       :", SHARE_NAME)
print("Recipient        :", RECIPIENT_NAME)
print("Tables to share  :", len(TABLES_TO_SHARE))
print("CDF-enabled      :", len(CDF_TABLES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Enable Change Data Feed on Provider Tables
# MAGIC
# MAGIC CDF must be enabled **on the provider** before it can be shared with history.
# MAGIC Once enabled, Delta records every row-level change as a log entry that
# MAGIC recipients can consume incrementally via `readChangeFeed`.
# MAGIC
# MAGIC This is a one-time setup — CDF persists until explicitly disabled.

# COMMAND ----------

for table in CDF_TABLES:
    print(f"Enabling CDF on {table}...")
    spark.sql(f"""
        ALTER TABLE {table}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"  ✓ CDF enabled on {table}")

print("\nAll CDF-eligible tables updated.")

# COMMAND ----------

# Verify CDF is actually enabled by reading the table properties
# This confirms the ALTER TABLE took effect before we proceed to sharing
for table in CDF_TABLES:
    props = spark.sql(f"SHOW TBLPROPERTIES {table}").filter("key = 'delta.enableChangeDataFeed'")
    status = props.collect()[0]["value"] if props.count() > 0 else "not set"
    print(f"{table}: delta.enableChangeDataFeed = {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Share
# MAGIC
# MAGIC A **share** is a secure, named container that groups one or more tables to expose.
# MAGIC Recipients connect to a share — not directly to tables — giving the provider
# MAGIC a single point of control to add/remove tables or revoke access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop and recreate for idempotency during development
# MAGIC -- In production, use CREATE SHARE IF NOT EXISTS and manage membership incrementally
# MAGIC DROP SHARE IF EXISTS energy_market_share;

# COMMAND ----------

spark.sql(f"CREATE SHARE IF NOT EXISTS {SHARE_NAME}")
print(f"✓ Share '{SHARE_NAME}' created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Tables to the Share
# MAGIC
# MAGIC `WITH HISTORY` is required to allow recipients to use Change Data Feed on shared tables.
# MAGIC Without it, the recipient can only do full table reads.
# MAGIC
# MAGIC The `AS schema.table` alias controls how the table appears in the recipient's foreign catalog.
# MAGIC Using `schema.table` (without the catalog prefix) keeps the alias clean and portable.

# COMMAND ----------

for table in TABLES_TO_SHARE:
    # Derive the alias as "schema.table" — strips the provider catalog prefix
    # so recipients see a clean namespace in their foreign catalog
    parts = table.split(".")
    alias = f"{parts[1]}.{parts[2]}"

    # Only add WITH HISTORY for CDF-enabled tables
    # History sharing allows recipients to read the CDF log, enabling incremental processing
    history_clause = "WITH HISTORY" if table in CDF_TABLES else ""

    sql = f"ALTER SHARE {SHARE_NAME} ADD TABLE {table} AS {alias} {history_clause}"
    print(f"Adding: {table}")
    print(f"  Alias  : {alias}")
    print(f"  History: {'yes' if history_clause else 'no (full reads only)'}")

    try:
        spark.sql(sql)
        print(f"  ✓ Added successfully\n")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ⚠ Already in share, skipping\n")
        else:
            print(f"  ✗ Error: {e}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Databricks-to-Databricks (D2D) Recipient
# MAGIC
# MAGIC A **recipient** represents the entity that will access the share.
# MAGIC For Databricks-to-Databricks sharing, the recipient is identified by the
# MAGIC **Sharing Identifier** of the recipient's Databricks workspace — no tokens needed.
# MAGIC
# MAGIC D2D is the most secure and operationally simple option:
# MAGIC - No credential files to manage or rotate
# MAGIC - Access is governed by Unity Catalog on both sides
# MAGIC - The recipient uses their normal Databricks identity

# COMMAND ----------

print(f"Creating D2D recipient '{RECIPIENT_NAME}'...")
print(f"  Sharing ID: {RECIPIENT_SHARING_ID}")

try:
    spark.sql(f"""
        CREATE RECIPIENT IF NOT EXISTS {RECIPIENT_NAME}
        USING ID '{RECIPIENT_SHARING_ID}'
    """)
    print(f"✓ Recipient '{RECIPIENT_NAME}' created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"⚠ Recipient already exists — no action needed.")
    else:
        raise e

# COMMAND ----------

# Grant SELECT on the share to the recipient
# This is the single access control decision — the share acts as the boundary
# For finer-grained control, add row filters or column masks on individual tables
print(f"Granting SELECT on share '{SHARE_NAME}' to recipient '{RECIPIENT_NAME}'...")

try:
    spark.sql(f"GRANT SELECT ON SHARE {SHARE_NAME} TO RECIPIENT {RECIPIENT_NAME}")
    print("✓ Access granted.")
except Exception as e:
    if "already" in str(e).lower():
        print("⚠ Access already granted — no action needed.")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Share Configuration

# COMMAND ----------

# Inspect the share details — shows metadata, comment, and creation time
print(f"=== Share Details: {SHARE_NAME} ===\n")
spark.sql(f"DESCRIBE SHARE {SHARE_NAME}").display()

# COMMAND ----------

# List all tables (and their aliases) currently in the share
# Confirm WITH HISTORY is set on CDF-eligible tables
print(f"\n=== Tables in Share ===\n")
spark.sql(f"SHOW ALL IN SHARE {SHARE_NAME}").display()

# COMMAND ----------

# Show which recipients have access — useful for access audits
print(f"\n=== Recipients with Access ===\n")
spark.sql(f"SHOW GRANTS ON SHARE {SHARE_NAME}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Item | Status |
# MAGIC |------|--------|
# MAGIC | CDF enabled on 3 trading tables | ✓ |
# MAGIC | Share `energy_market_share` created | ✓ |
# MAGIC | 5 tables added to share (3 with history) | ✓ |
# MAGIC | D2D recipient created | ✓ |
# MAGIC | SELECT access granted to recipient | ✓ |
# MAGIC
# MAGIC **Next:** Switch to the **Recipient Workspace** and run `02_recipient_catalog_setup`.
