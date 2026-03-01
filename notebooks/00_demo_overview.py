# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing Demo — Overview
# MAGIC
# MAGIC **Scenario:** A market operator (**Provider Workspace**) shares energy trading and generation
# MAGIC data with an energy retailer (**Recipient Workspace**) via Databricks Delta Sharing.
# MAGIC
# MAGIC The recipient builds a full **medallion architecture** on top of the shared data:
# MAGIC - **Bronze** — append-only ingestion with rich metadata for lineage tracking
# MAGIC - **Silver** — SCD Type 2 bitemporal tables with valid time + transaction time
# MAGIC - **Gold** — pre-aggregated summaries and KPIs for analytics
# MAGIC - **Audit** — permanent Change Data Feed archive for compliance
# MAGIC - **Control** — data quality checks and freshness SLA monitoring
# MAGIC
# MAGIC > This notebook can be run on **either workspace** — it is read-only and performs no writes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC ```
# MAGIC ┌─────────────────────────────────┐       Delta Sharing       ┌──────────────────────────────────┐
# MAGIC │       Provider Workspace        │ ────────────────────────► │      Recipient Workspace         │
# MAGIC │       (Market Operator)         │                          │      (Energy Retailer)           │
# MAGIC │                                 │                          │                                  │
# MAGIC │  energy_utilities catalog       │                          │  delta_sharing_demo catalog      │
# MAGIC │  ├─ energy_trading              │                          │  ├─ bronze (append-only)         │
# MAGIC │  │  ├─ market_prices_pjm       │                          │  ├─ silver (bitemporal SCD2)     │
# MAGIC │  │  ├─ gold_daily_trading_sum   │                          │  ├─ gold  (aggregations)         │
# MAGIC │  │  └─ positions                │                          │  ├─ audit (CDF archive)          │
# MAGIC │  └─ power_generation            │                          │  └─ control (freshness/DQ)       │
# MAGIC │     ├─ iso_market               │                          │                                  │
# MAGIC │     └─ turbine_locations        │                          │                                  │
# MAGIC └─────────────────────────────────┘                          └──────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key insight:** The recipient never gets direct access to the provider's storage.
# MAGIC Delta Sharing exposes tables through a secure open protocol — the provider retains
# MAGIC full control and can revoke access at any time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Flow
# MAGIC
# MAGIC | Step | Notebook | What Happens |
# MAGIC |------|----------|-------------|
# MAGIC | 1 | `01_provider_setup` | Enable CDF on provider tables, create share, add tables, create D2D recipient |
# MAGIC | 2 | `02_recipient_catalog_setup` | Create provider reference, foreign catalog, `delta_sharing_demo` catalog + schemas |
# MAGIC | 3 | `03_bronze_ingestion` | Read shared tables → append to bronze with metadata (`_ingested_at`, `_source_share`, etc.) |
# MAGIC | 4 | `04_silver_bitemporal` | CDF from bronze → SCD2 MERGE into silver with `valid_from/to` + `recorded_at/superseded_at` |
# MAGIC | 5 | `05_gold_aggregations` | Silver → aggregated gold summaries (daily prices, position rollups) |
# MAGIC | 6 | `06_data_quality_monitoring` | Row count deltas, null checks, schema drift detection, freshness SLAs |
# MAGIC | 7 | `07_audit_history` | CDF → permanent audit archive, reconciliation checks between bronze and shared tables |
# MAGIC | 8 | `08_scheduling_and_orchestration` | Jobs, Workflows, Streaming, DLT, open sharing patterns |
# MAGIC | 9 | `09_recipient_best_practices` | Access control, performance tuning, UC governance, operational runbook |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Tables (Provider Workspace)

# COMMAND ----------

# These are the 5 tables the Provider Workspace exposes via the Delta Share.
# The recipient accesses them through the foreign catalog `shared_energy_market`
# without ever touching the provider's underlying storage.
shared_tables = [
    ("energy_utilities", "energy_trading",  "market_prices_pjm",          "PJM market prices (LMP, region, interval)"),
    ("energy_utilities", "energy_trading",  "gold_daily_trading_summary",  "Daily trading summaries"),
    ("energy_utilities", "energy_trading",  "positions",                   "Trading positions (instrument, quantity, direction)"),
    ("energy_utilities", "power_generation","iso_market",                  "ISO market data"),
    ("energy_utilities", "power_generation","turbine_locations",           "Wind turbine locations"),
]

print("=" * 95)
print(f"{'Catalog':<25} {'Schema':<20} {'Table':<35} {'Description'}")
print("=" * 95)
for catalog, schema, table, desc in shared_tables:
    print(f"{catalog:<25} {schema:<20} {table:<35} {desc}")
print("=" * 95)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Connectivity
# MAGIC
# MAGIC Run this cell to confirm you can access the current workspace and see the Spark version.
# MAGIC This is useful to verify you are running on the correct workspace before proceeding.

# COMMAND ----------

import json

# Introspect the notebook context to get workspace metadata
context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
workspace_url = context.get("extraContext", {}).get("api_url", "unknown")

# Identify the currently logged-in user
user = spark.sql("SELECT current_user()").collect()[0][0]

print(f"Workspace URL : {workspace_url}")
print(f"Current user  : {user}")
print(f"Spark version : {spark.version}")
print()
print("If you see the Provider Workspace URL above → run notebook 01 next.")
print("If you see the Recipient Workspace URL above → run notebooks 02-09 next.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recipient Catalog Structure (to be created by notebooks 02–05)
# MAGIC
# MAGIC ```
# MAGIC delta_sharing_demo
# MAGIC ├── bronze                              ← Raw ingested data, append-only
# MAGIC │   ├── bronze_market_prices
# MAGIC │   ├── bronze_daily_trading_summary
# MAGIC │   ├── bronze_positions
# MAGIC │   ├── bronze_iso_market
# MAGIC │   └── bronze_turbine_locations
# MAGIC ├── silver                              ← Curated, SCD2 bitemporal
# MAGIC │   ├── silver_prices_bitemporal
# MAGIC │   └── silver_positions_bitemporal
# MAGIC ├── gold                               ← Pre-aggregated for analytics
# MAGIC │   ├── gold_daily_price_summary
# MAGIC │   └── gold_trading_position_summary
# MAGIC ├── audit                              ← Compliance archive (CDF history)
# MAGIC │   └── audit_changelog
# MAGIC └── control                            ← Operational metadata
# MAGIC     ├── control_freshness
# MAGIC     ├── control_data_quality
# MAGIC     └── control_cdf_checkpoint         ← Incremental processing state
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Concepts
# MAGIC
# MAGIC | Concept | What it means in this demo |
# MAGIC |---------|---------------------------|
# MAGIC | **Delta Sharing** | Open protocol for sharing Delta tables without copying data |
# MAGIC | **Foreign Catalog** | A Unity Catalog object that maps to a remote share — looks like a local catalog |
# MAGIC | **CDF (Change Data Feed)** | Delta feature that records every row-level change (insert/update/delete) as a readable stream |
# MAGIC | **Incremental Read** | Reading only CDF rows since the last checkpoint version, not the full table |
# MAGIC | **Bitemporal Modeling** | Tracking both *when a fact is true* (valid time) and *when we knew about it* (transaction time) |
# MAGIC | **SCD Type 2** | Slowly Changing Dimension — keeps all historical versions of each record |
# MAGIC | **Medallion Architecture** | Layered data lakehouse pattern: Bronze (raw) → Silver (curated) → Gold (aggregated) |
# MAGIC | **CDF Checkpoint** | A control table that records the last Delta version processed per pipeline stage |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Run **`01_provider_setup`** on the **Provider Workspace** to configure sharing infrastructure
# MAGIC 2. Run **`02_recipient_catalog_setup`** on the **Recipient Workspace** to create the recipient catalog
# MAGIC 3. Run **`03`–`07`** sequentially on the **Recipient Workspace** to build the full pipeline
# MAGIC 4. *(Optional)* Review **`08_scheduling_and_orchestration`** for production scheduling patterns
# MAGIC 5. *(Optional)* Review **`09_recipient_best_practices`** for governance and operational guidance
