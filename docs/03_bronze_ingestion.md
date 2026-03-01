# Bronze Ingestion Guide

## Overview

The bronze layer ingests shared data in an **append-only** pattern with rich metadata for lineage tracking, partitioned by ingestion date.

## Design Principles

1. **Append-only** — never update or delete bronze records
2. **Metadata enrichment** — every row tagged with ingestion context
3. **CDF enabled** — downstream layers can process incrementally
4. **Partitioned** — by `_ingested_date` for efficient pruning
5. **Schema evolution** — `mergeSchema` enabled for forward compatibility

## Metadata Columns

| Column | Type | Description |
|--------|------|-------------|
| `_ingested_at` | TIMESTAMP | UTC timestamp of ingestion |
| `_source_share` | STRING | Name of the Delta Share |
| `_source_version` | LONG | Provider table version at time of read |
| `_batch_id` | STRING | Unique identifier for this ingestion batch |
| `_ingested_date` | DATE | Partition key (date portion of `_ingested_at`) |

## Table Mappings

| Shared Table | Bronze Table |
|--------------|-------------|
| `shared_energy_market.energy_trading.market_prices_pjm` | `delta_sharing_demo.bronze.bronze_market_prices` |
| `shared_energy_market.energy_trading.gold_daily_trading_summary` | `delta_sharing_demo.bronze.bronze_daily_trading_summary` |
| `shared_energy_market.energy_trading.positions` | `delta_sharing_demo.bronze.bronze_positions` |
| `shared_energy_market.power_generation.iso_market` | `delta_sharing_demo.bronze.bronze_iso_market` |
| `shared_energy_market.power_generation.turbine_locations` | `delta_sharing_demo.bronze.bronze_turbine_locations` |

## Table Properties

All bronze tables have:
```sql
delta.enableChangeDataFeed = true
delta.autoOptimize.optimizeWrite = true
delta.autoOptimize.autoCompact = true
```

## Incremental Processing

For subsequent runs, you can switch from full-table reads to CDF-based incremental reads:

```python
# Read only changes since last version
df_changes = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_processed_version)
    .table("shared_energy_market.energy_trading.market_prices_pjm")
)
```

Track the last processed version in `delta_sharing_demo.control.control_freshness`.

## Reprocessing

Since bronze is append-only, you can reprocess any batch:
```sql
-- Find rows from a specific batch
SELECT * FROM delta_sharing_demo.bronze.bronze_market_prices
WHERE _batch_id = '<batch-id>';

-- Or from a specific date
SELECT * FROM delta_sharing_demo.bronze.bronze_market_prices
WHERE _ingested_date = '2024-01-15';
```
