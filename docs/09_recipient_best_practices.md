# Recipient Best Practices — Production Readiness Checklist

## Access Control

### Decision Guide

| Question | If Yes | If No |
|----------|--------|-------|
| Does the team need to write data? | Grant `ALL PRIVILEGES` on their schemas | Grant `SELECT` only |
| Are there PII columns? | Apply column masks | No action needed |
| Is data region-restricted? | Apply row filters | No action needed |
| Multiple teams share the catalog? | Grant at schema level | Grant at catalog level |

### GRANT Reference

```sql
-- Catalog-level
GRANT USAGE ON CATALOG delta_sharing_demo TO `group`;
GRANT CREATE SCHEMA ON CATALOG delta_sharing_demo TO `admins`;

-- Schema-level
GRANT USAGE ON SCHEMA delta_sharing_demo.gold TO `analysts`;
GRANT SELECT ON SCHEMA delta_sharing_demo.gold TO `analysts`;

-- Table-level
GRANT SELECT ON TABLE delta_sharing_demo.gold.gold_daily_price_summary TO `dashboard-users`;

-- Deny (explicit block)
DENY SELECT ON TABLE delta_sharing_demo.bronze.bronze_positions TO `analysts`;

-- Verify
SHOW GRANTS ON SCHEMA delta_sharing_demo.gold;
SHOW GRANTS ON TABLE delta_sharing_demo.gold.gold_daily_price_summary;
```

### Row Filters

```sql
CREATE FUNCTION control.region_filter(region_val STRING)
RETURNS BOOLEAN
RETURN IF(is_member('unrestricted-access'), true, region_val IN ('PJM-WEST', 'PJM-EAST'));

ALTER TABLE gold.gold_daily_price_summary SET ROW FILTER control.region_filter ON (region);
```

### Column Masks

```sql
CREATE FUNCTION control.price_mask(price_val DOUBLE)
RETURNS DOUBLE
RETURN IF(is_member('price-access'), price_val, ROUND(price_val / 10) * 10);

ALTER TABLE gold.gold_daily_price_summary ALTER COLUMN avg_price SET MASK control.price_mask;
```

## Cluster Sizing

| Workload | Type | Workers | Node | Notes |
|----------|------|---------|------|-------|
| Daily batch (03-05) | Job cluster | 2-4 | `i3.xlarge` | Ephemeral, cost-efficient |
| Interactive analytics | All-purpose | 2-8 | `i3.xlarge` | Auto-scaling enabled |
| Streaming | Job cluster | 2-4 | `i3.xlarge` | Always-on for continuous mode |
| DLT pipeline | DLT-managed | Auto | Auto | Recommended for production |

Use **serverless** compute where available.

## Partition Pruning Rules

| Pattern | Pruning? | Example |
|---------|----------|---------|
| Filter on partition key | Yes | `WHERE _ingested_date = '2024-06-01'` |
| Filter on non-partition column | No | `WHERE _ingested_at > '2024-06-01'` |
| Filter with functions on partition key | No | `WHERE YEAR(_ingested_date) = 2024` |
| Z-ORDER column filter (gold) | Yes (file skipping) | `WHERE price_date = '2024-06-01'` |

Always include `_ingested_date` in bronze queries. Use `EXPLAIN COST` to verify.

## Unity Catalog Governance

### Lineage Queries

```sql
-- What reads from shared tables?
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE source_table_full_name LIKE 'shared_energy_market%'
ORDER BY event_time DESC LIMIT 20;

-- What feeds gold?
SELECT source_table_full_name, target_table_full_name
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'delta_sharing_demo.gold%';
```

### Audit Queries

```sql
-- Recent data access
SELECT event_time, user_identity.email, action_name, request_params.full_name_arg
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'delta_sharing_demo%'
ORDER BY event_time DESC LIMIT 20;
```

### Tags

```sql
ALTER CATALOG delta_sharing_demo SET TAGS ('data_source' = 'delta_sharing');
ALTER SCHEMA delta_sharing_demo.bronze SET TAGS ('layer' = 'bronze', 'pii' = 'false');
ALTER TABLE delta_sharing_demo.bronze.bronze_positions SET TAGS ('sensitivity' = 'high');
```

## Dashboard Blueprint

Build a SQL dashboard with these tiles:

### Tile 1: Pipeline Freshness
```sql
SELECT pipeline_stage, last_processed_version, last_processed_ts,
       TIMESTAMPDIFF(MINUTE, last_processed_ts, current_timestamp()) AS lag_minutes
FROM delta_sharing_demo.control.control_cdf_checkpoint
ORDER BY lag_minutes DESC;
```

### Tile 2: Data Quality Summary
```sql
SELECT check_name, table_name, status, checked_at, details
FROM delta_sharing_demo.control.control_data_quality
WHERE checked_at > current_date() - INTERVAL 1 DAY
ORDER BY checked_at DESC;
```

### Tile 3: CDF Lag KPI
```sql
SELECT pipeline_stage, last_processed_version,
       TIMESTAMPDIFF(MINUTE, last_processed_ts, current_timestamp()) AS lag_minutes,
       CASE WHEN TIMESTAMPDIFF(MINUTE, last_processed_ts, current_timestamp()) > 120
            THEN 'BREACH' ELSE 'OK' END AS sla_status
FROM delta_sharing_demo.control.control_cdf_checkpoint;
```

## Operational Runbook

### Share Access Revoked

1. Verify: `SHOW SHARES IN PROVIDER energy_market_provider`
2. Fall back to bronze tables (last good copy)
3. Contact provider to restore access
4. Run `03_bronze_ingestion` with `force_full_reload=true`

### CDF Disabled on Shared Table

1. Contact provider to re-enable CDF
2. Reset checkpoint: `DELETE FROM control.control_cdf_checkpoint WHERE pipeline_stage = '<stage>'`
3. Run with `force_full_reload=true`

### Schema Drift Detected

1. Compare: `DESCRIBE TABLE <shared>` vs `DESCRIBE TABLE <bronze>`
2. Bronze handles new columns via `mergeSchema=true` (automatic)
3. Update silver DDL: `ALTER TABLE ... ADD COLUMNS (...)`
4. Update MERGE logic in notebook 04

### SLA Breached

1. Check job runs: `databricks jobs list-runs --job-id <id> --limit 5`
2. Check provider freshness: `DESCRIBE HISTORY <shared_table> LIMIT 5`
3. If provider stale — document in audit
4. If pipeline failed — fix and run: `databricks jobs run-now --job-id <id>`

### CDF Version Gap (VACUUM)

1. Find earliest available: `DESCRIBE HISTORY <table>`
2. Reset checkpoint to earliest version
3. Set retention to prevent recurrence:
```sql
ALTER TABLE <table> SET TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 30 days'
);
```
