# Recipient Catalog Setup Guide

## Overview

The **Recipient Workspace** creates a provider reference and a foreign catalog to access shared data, then creates a local catalog for the medallion pipeline. All steps in this guide are run on the Recipient Workspace.

## Prerequisites

- Admin access on the Recipient Workspace
- Unity Catalog enabled
- Provider Workspace has completed setup (notebook 01)
- Provider Workspace's sharing identifier available

## Steps

### 1. Create Provider

The provider object points to the Provider Workspace using its sharing identifier.
Find it at: **Provider Workspace → Settings → Workspace Settings → General → Sharing Identifier**

```sql
CREATE PROVIDER IF NOT EXISTS energy_market_provider
USING ID '<provider-sharing-identifier>';
```

Verify available shares:
```sql
SHOW SHARES IN PROVIDER energy_market_provider;
```

### 2. Create Foreign Catalog

A foreign catalog provides read-only access to the shared tables, appearing as a normal Unity Catalog catalog from the recipient's perspective:

```sql
CREATE CATALOG IF NOT EXISTS shared_energy_market
USING SHARE energy_market_provider.energy_market_share;
```

The shared tables will appear under their aliased schema/table names:
- `shared_energy_market.energy_trading.market_prices_pjm`
- `shared_energy_market.energy_trading.gold_daily_trading_summary`
- `shared_energy_market.energy_trading.positions`
- `shared_energy_market.power_generation.iso_market`
- `shared_energy_market.power_generation.turbine_locations`

### 3. Create Demo Catalog

The demo catalog holds all local tables organized by medallion layer:

```sql
CREATE CATALOG IF NOT EXISTS delta_sharing_demo;

CREATE SCHEMA IF NOT EXISTS delta_sharing_demo.bronze;
CREATE SCHEMA IF NOT EXISTS delta_sharing_demo.silver;
CREATE SCHEMA IF NOT EXISTS delta_sharing_demo.gold;
CREATE SCHEMA IF NOT EXISTS delta_sharing_demo.audit;
CREATE SCHEMA IF NOT EXISTS delta_sharing_demo.control;
```

### 4. Create CDF Checkpoint Table

This control table tracks the last Delta version processed by each pipeline stage, enabling incremental CDF reads on subsequent runs:

```sql
CREATE TABLE IF NOT EXISTS delta_sharing_demo.control.control_cdf_checkpoint (
    pipeline_stage         STRING    COMMENT 'Stage identifier, e.g. bronze_market_prices',
    last_processed_version LONG      COMMENT 'Last Delta version successfully processed',
    last_processed_ts      TIMESTAMP COMMENT 'Wall-clock time of last successful run',
    run_id                 STRING    COMMENT 'Batch ID or Job run ID'
)
USING DELTA;
```

### 5. Verify Access

```sql
-- Check shared tables are readable
SELECT count(*) FROM shared_energy_market.energy_trading.market_prices_pjm;
SELECT count(*) FROM shared_energy_market.energy_trading.positions;

-- Check local schemas were created
SHOW SCHEMAS IN delta_sharing_demo;
```

## Notes

- Foreign catalogs are **read-only** — you cannot write to shared tables
- The foreign catalog reflects the current state of the Provider Workspace's shared tables
- Schema/table names in the foreign catalog match the aliases set by the provider
- If the provider updates their tables, the foreign catalog sees the changes immediately
