# Scheduling & Orchestration Guide

## Overview

This guide covers all patterns for scheduling and automating the Delta Sharing demo pipeline, from simple single-notebook jobs to production-grade streaming and DLT.

## Widget Reference

| Widget | Notebook | Default | Purpose |
|--------|----------|---------|---------|
| `starting_version` | 03 | `""` | CDF starting version override (blank = auto from checkpoint) |
| `force_full_reload` | 03 | `"false"` | Skip CDF, perform full table read |

When scheduling via Jobs, pass widgets as `base_parameters`:
```json
{
  "base_parameters": {
    "force_full_reload": "false",
    "starting_version": ""
  }
}
```

## Jobs Configuration

### Single Notebook Job

Schedule notebook 03 as a daily ingestion job:

```json
{
  "name": "delta-sharing-demo-bronze-ingestion",
  "tasks": [{
    "task_key": "bronze_ingestion",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<user>/03_bronze_ingestion",
      "base_parameters": {"force_full_reload": "false"}
    },
    "job_cluster_key": "pipeline_cluster"
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "UTC"
  },
  "max_retries": 2,
  "email_notifications": {"on_failure": ["team@example.com"]}
}
```

### Multi-Task Workflow

Chain the full pipeline with dependency management:

```
03_bronze → 04_silver → 05_gold → {06_dq, 07_audit}
```

Each task uses `depends_on` to specify its upstream dependency. Tasks 06 and 07 both depend on 05 and run in parallel.

### Creating Jobs

```bash
# Via CLI
databricks jobs create --json-file job-config.json

# Via REST API
curl -X POST https://<workspace>/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -d @job-config.json
```

## State Passing Patterns

| Pattern | Scope | Persists | Use Case |
|---------|-------|----------|----------|
| `control_cdf_checkpoint` table | Cross-run | Yes | CDF version tracking |
| `dbutils.jobs.taskValues` | Single run | No | Row counts, batch IDs, status flags |

### Checkpoint Table
```python
# Read checkpoint
ckpt = get_checkpoint("bronze_market_prices")

# Write checkpoint after success
write_checkpoint("bronze_market_prices", version, batch_id)
```

### Task Values (within a workflow run)
```python
# Set in upstream task
dbutils.jobs.taskValues.set(key="row_count", value=12345)

# Get in downstream task
count = dbutils.jobs.taskValues.get(taskKey="bronze_ingestion", key="row_count")
```

## Streaming Trigger Modes

| Mode | Syntax | Latency | Cost | Use Case |
|------|--------|---------|------|----------|
| Available Now | `trigger(availableNow=True)` | Minutes | Low | Scheduled Jobs |
| Processing Time | `trigger(processingTime="30 seconds")` | Seconds | Medium | Near-real-time |
| Continuous | `trigger(processingTime="0 seconds")` | Sub-second | High | Continuous |

### Streaming Template

```python
(
    spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("delta_sharing_demo.bronze.bronze_market_prices")
    .filter("_change_type IN ('insert', 'update_postimage')")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/path/to/checkpoint")
    .trigger(availableNow=True)
    .toTable("delta_sharing_demo.silver.streaming_sink")
)
```

## DLT Migration Path

| Current Pattern | DLT Equivalent |
|----------------|----------------|
| `spark.table()` batch read | `spark.readStream` in `@dlt.table` |
| Manual CDF + checkpoint | Automatic with `dlt.read_stream()` |
| Manual MERGE SCD2 | `APPLY CHANGES INTO` |
| Manual DQ checks (notebook 06) | `@dlt.expect` / `@dlt.expect_or_drop` |
| Manual Job orchestration | DLT pipeline manages DAG automatically |

### DLT Example

```python
import dlt

@dlt.table(name="bronze_market_prices", table_properties={"quality": "bronze"})
def bronze():
    return spark.readStream.option("readChangeFeed", "true") \
        .table("shared_energy_market.energy_trading.market_prices_pjm")

@dlt.table(name="silver_prices", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
def silver():
    return dlt.read_stream("bronze_market_prices")
```

## Open Sharing Setup

For non-Databricks clients:

1. Install: `pip install delta-sharing`
2. Download credential file: Catalog Explorer → Shares → Recipients → Download
3. Load data:

```python
import delta_sharing

profile = "config.share"
table_url = f"{profile}#energy_market_share.energy_trading.market_prices_pjm"

# Pandas
df = delta_sharing.load_as_pandas(table_url)

# Spark (requires local Spark + delta-sharing connector)
df = delta_sharing.load_as_spark(table_url)
```
