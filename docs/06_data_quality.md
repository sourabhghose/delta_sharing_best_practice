# Data Quality Framework Guide

## Overview

The data quality framework monitors the health of the medallion pipeline with automated checks, SLA tracking, and alerting.

## Check Types

### 1. Row Count Delta

Compares row counts between the current and previous ingestion batch. A large delta may indicate data loss or unexpected data volume.

- **Threshold:** 50% change (configurable)
- **Status:** PASS if within threshold, FAIL if exceeded

### 2. Null Rate

Checks for null values in required columns. High null rates indicate data quality issues at the source.

- **Threshold:** 5% null rate (configurable)
- **Checked columns:** `_ingested_at`, `_batch_id` (metadata), plus source-specific required fields

### 3. Schema Drift

Detects when the source schema changes (columns added or removed). Schema drift may require pipeline updates.

- **Baseline:** Recorded on first run
- **Detection:** Compares current columns against baseline (excluding metadata columns)

### 4. Freshness SLA

Tracks how recently data was ingested. Stale data may indicate a broken pipeline or provider issue.

- **Threshold:** 24 hours (configurable)
- **Status:** WITHIN_SLA or BREACHED

## Control Tables

### control_data_quality

Stores results of every DQ check run:

| Column | Description |
|--------|-------------|
| `check_id` | Unique identifier |
| `check_time` | When the check ran |
| `table_name` | Table checked |
| `check_type` | row_count, null_rate, schema_drift |
| `metric_value` | Measured value |
| `threshold_value` | Configured threshold |
| `status` | PASS or FAIL |
| `message` | Human-readable result |

### control_freshness

Tracks data freshness per table:

| Column | Description |
|--------|-------------|
| `table_name` | Bronze table name |
| `last_ingested_at` | Most recent ingestion timestamp |
| `staleness_hours` | Hours since last ingestion |
| `sla_status` | WITHIN_SLA or BREACHED |
| `last_source_version` | Provider table version |

## Configuring Thresholds

Edit the configuration section in `06_data_quality_monitoring.py`:

```python
ROW_COUNT_DELTA_THRESHOLD = 0.50   # 50% change triggers alert
NULL_RATE_THRESHOLD = 0.05          # 5% null rate triggers alert
FRESHNESS_SLA_HOURS = 24            # 24-hour freshness window
```

## Alert Integration

The current implementation prints alerts in the notebook output. For production, integrate with:
- Databricks SQL Alerts
- Slack/Teams webhooks
- PagerDuty or similar incident management
- Email notifications via Databricks Jobs
