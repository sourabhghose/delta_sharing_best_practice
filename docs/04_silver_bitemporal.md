# Silver Bitemporal Modeling Guide

## Overview

The silver layer implements **SCD Type 2 bitemporal** tables that track two independent time dimensions:

- **Valid time** (`valid_from`, `valid_to`) — when the fact is true in the real world
- **Transaction time** (`recorded_at`, `superseded_at`) — when our system recorded it

## Why Bitemporal?

| Scenario | Valid Time Only | Transaction Time Only | Bitemporal |
|----------|:-:|:-:|:-:|
| "What is the current price?" | Yes | Yes | Yes |
| "What was the price at 2pm on Feb 15?" | Yes | No | Yes |
| "What did we know yesterday?" | No | Yes | Yes |
| "Show me corrections/late arrivals" | No | No | Yes |

Bitemporal modeling is essential for:
- **Regulatory compliance** — prove what you knew and when
- **Late-arriving data** — correctly handle corrections
- **Point-in-time queries** — reproduce any historical state

## Table Schemas

### silver_prices_bitemporal

| Column | Type | Description |
|--------|------|-------------|
| `entity_key` | STRING | MD5 hash of region + interval_start |
| `region` | STRING | Market region / pricing node |
| `interval_start` | TIMESTAMP | Price interval start |
| `interval_end` | TIMESTAMP | Price interval end |
| `price_type` | STRING | LMP, MCP, etc. |
| `price` | DOUBLE | Price in $/MWh |
| `valid_from` | TIMESTAMP | When this price became effective |
| `valid_to` | TIMESTAMP | When this price stopped being effective |
| `recorded_at` | TIMESTAMP | When we first recorded this version |
| `superseded_at` | TIMESTAMP | When a newer version replaced this one |
| `is_current` | BOOLEAN | True for the latest known version |
| `_source_batch_id` | STRING | Lineage back to bronze batch |

### silver_positions_bitemporal

Similar structure with position-specific columns (position_id, instrument, direction, quantity, etc.).

## SCD2 MERGE Logic

```
For each incoming record:
  1. Match on entity_key WHERE is_current = true
  2. If matched AND values changed:
     - UPDATE existing: is_current = false, superseded_at = now, valid_to = now
     - INSERT new: is_current = true, recorded_at = now, superseded_at = 9999-12-31
  3. If not matched:
     - INSERT new: is_current = true, recorded_at = now
```

## Query Patterns

### Transaction Time Query ("As-of" — what did we know?)
```sql
SELECT * FROM silver_prices_bitemporal
WHERE recorded_at <= '2024-01-15T12:00:00'
  AND superseded_at > '2024-01-15T12:00:00';
```

### Valid Time Query ("Point-in-time" — what was true?)
```sql
SELECT * FROM silver_prices_bitemporal
WHERE valid_from <= '2024-01-15T14:00:00'
  AND valid_to > '2024-01-15T14:00:00'
  AND is_current = true;
```

### Full History Query
```sql
SELECT * FROM silver_positions_bitemporal
WHERE position_id = 'POS-123'
ORDER BY recorded_at;
```

### Bi-temporal Query (what did we know about a specific valid time?)
```sql
SELECT * FROM silver_prices_bitemporal
WHERE valid_from <= '2024-01-15T14:00:00'
  AND valid_to > '2024-01-15T14:00:00'
  AND recorded_at <= '2024-01-16T00:00:00'
  AND superseded_at > '2024-01-16T00:00:00';
```

## Performance Considerations

- `is_current = true` filter prunes most historical rows
- Entity key is an MD5 hash for consistent distribution
- CDF enabled for downstream incremental processing
- Deduplication window applied before MERGE to handle duplicates in bronze
