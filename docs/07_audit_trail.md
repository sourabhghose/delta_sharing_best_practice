# Audit Trail Guide

## Overview

The audit layer captures a **permanent record** of all data changes using Delta Change Data Feed (CDF). This supports compliance, reconciliation, and forensic analysis.

## Audit Changelog Table

`delta_sharing_demo.audit.audit_changelog`

| Column | Type | Description |
|--------|------|-------------|
| `changelog_id` | STRING | Unique entry ID |
| `source_table` | STRING | Table that changed |
| `change_type` | STRING | insert, update_preimage, update_postimage, delete |
| `change_version` | LONG | Delta version of the change |
| `change_timestamp` | TIMESTAMP | When the change occurred |
| `record_key` | STRING | Business key (best-effort) |
| `record_snapshot` | STRING | Full JSON snapshot of the record |
| `captured_at` | TIMESTAMP | When the audit entry was written |
| `batch_id` | STRING | Processing batch ID |

### Properties
- Partitioned by `source_table`
- 365-day log and file retention
- CDF enabled on the audit table itself

## CDF Change Types

| Type | Meaning |
|------|---------|
| `insert` | New row added |
| `update_preimage` | Row before an update |
| `update_postimage` | Row after an update |
| `delete` | Row was deleted |

## Reconciliation

The reconciliation check compares:
1. **Shared table count** — current row count in the provider's shared table
2. **Bronze latest batch count** — rows ingested in the most recent bronze batch

A mismatch indicates:
- Partial ingestion failure
- Schema filtering dropped rows
- Provider table changed between read and count

## Retention Policy

| Layer | Recommended Retention | Rationale |
|-------|----------------------|-----------|
| Bronze | 90 days | Can be re-ingested from share |
| Silver | 365 days | Bitemporal history is system of record |
| Gold | 90 days | Recomputable from silver |
| **Audit** | **7 years** | Compliance — permanent record |
| Control | 365 days | Operational monitoring |

### Setting Retention

```sql
ALTER TABLE delta_sharing_demo.audit.audit_changelog SET TBLPROPERTIES (
    delta.logRetentionDuration = 'interval 2555 days',
    delta.deletedFileRetentionDuration = 'interval 2555 days'
);
```

## Sample Audit Queries

```sql
-- All changes to a specific table in the last 7 days
SELECT * FROM delta_sharing_demo.audit.audit_changelog
WHERE source_table = 'bronze_positions'
  AND captured_at >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY change_timestamp DESC;

-- Count of changes by type per table
SELECT source_table, change_type, count(*) as change_count
FROM delta_sharing_demo.audit.audit_changelog
GROUP BY source_table, change_type
ORDER BY source_table, change_type;

-- Full history of a specific record
SELECT change_type, change_timestamp, record_snapshot
FROM delta_sharing_demo.audit.audit_changelog
WHERE record_key = '<record-key>'
ORDER BY change_timestamp;
```
