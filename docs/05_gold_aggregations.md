# Gold Aggregations Guide

## Overview

The gold layer provides pre-computed, query-optimized summaries built from the silver bitemporal tables. Only **current** records (`is_current = true`) are used for aggregations.

## Tables

### gold_daily_price_summary

Aggregates market prices by date, region, and price type.

| Column | Type | Description |
|--------|------|-------------|
| `price_date` | DATE | Trading date |
| `region` | STRING | Market region |
| `price_type` | STRING | Price type |
| `avg_price` | DOUBLE | Average price ($/MWh) |
| `min_price` | DOUBLE | Minimum price |
| `max_price` | DOUBLE | Maximum price |
| `stddev_price` | DOUBLE | Standard deviation |
| `interval_count` | LONG | Number of intervals |
| `price_spread` | DOUBLE | Max - Min spread |
| `_updated_at` | TIMESTAMP | Last computation time |

### gold_trading_position_summary

Aggregates trading positions by instrument.

| Column | Type | Description |
|--------|------|-------------|
| `instrument` | STRING | Financial instrument |
| `total_long_qty` | DOUBLE | Total long quantity |
| `total_short_qty` | DOUBLE | Total short quantity |
| `net_position` | DOUBLE | Long - Short |
| `position_count` | LONG | Number of positions |
| `avg_quantity` | DOUBLE | Average position size |
| `_updated_at` | TIMESTAMP | Last computation time |

## Optimization

### Liquid Clustering

Both tables use `CLUSTER BY` for adaptive data layout:
```sql
-- Prices clustered by date + region (common query pattern)
CREATE TABLE ... CLUSTER BY (price_date, region)

-- Positions clustered by instrument
CREATE TABLE ... CLUSTER BY (instrument)
```

### Z-ORDER

Applied after data load for optimal file-level statistics:
```sql
OPTIMIZE gold_daily_price_summary ZORDER BY (price_date, region);
OPTIMIZE gold_trading_position_summary ZORDER BY (instrument);
```

## Refresh Strategy

Gold tables are currently rebuilt via `OVERWRITE`. For incremental refresh:

1. Read CDF from silver tables (changes since last gold refresh)
2. Recompute only affected partitions/groups
3. MERGE into gold tables

This is more efficient for large datasets but adds complexity.

## Sample Queries

```sql
-- Price volatility by region
SELECT region, avg(stddev_price) as avg_volatility
FROM gold_daily_price_summary
GROUP BY region
ORDER BY avg_volatility DESC;

-- Net exposure by instrument
SELECT instrument, net_position, position_count
FROM gold_trading_position_summary
WHERE abs(net_position) > 0
ORDER BY abs(net_position) DESC;
```
