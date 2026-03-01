# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Scheduling & Orchestration (Recipient Workspace)
# MAGIC
# MAGIC **Reference notebook — read and run individual cells as needed. No writes to production tables.**
# MAGIC
# MAGIC This notebook documents production scheduling and consumption patterns for the medallion pipeline.
# MAGIC It is designed to be read alongside the Databricks documentation and used as a template
# MAGIC when operationalizing the pipeline beyond the initial demo.
# MAGIC
# MAGIC Topics covered:
# MAGIC 1. **Widget parameterization** — how notebooks 03-05 accept Job parameters
# MAGIC 2. **Databricks Jobs** — scheduling a single notebook as a daily job
# MAGIC 3. **Multi-task Workflows** — chaining the full pipeline with dependency management
# MAGIC 4. **Passing state between tasks** — checkpoint table vs `dbutils.jobs.taskValues`
# MAGIC 5. **Structured Streaming** — consuming CDF as a stream (`availableNow` and `processingTime`)
# MAGIC 6. **Delta Live Tables (DLT)** — recommended production pipeline pattern
# MAGIC 7. **Open sharing** — consuming shared tables from non-Databricks environments
# MAGIC 8. **Consumption pattern summary** — decision guide for choosing the right pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widget Parameterization
# MAGIC
# MAGIC All pipeline notebooks support widget parameters for Jobs integration.
# MAGIC
# MAGIC | Widget | Notebook | Default | Description |
# MAGIC |--------|----------|---------|-------------|
# MAGIC | `starting_version` | 03 | `""` (auto) | CDF starting version override |
# MAGIC | `force_full_reload` | 03 | `"false"` | Skip CDF, do full table read |
# MAGIC
# MAGIC When scheduling via Jobs, pass parameters as key-value pairs:
# MAGIC ```json
# MAGIC {
# MAGIC   "notebook_params": {
# MAGIC     "force_full_reload": "false",
# MAGIC     "starting_version": ""
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Databricks Jobs — Schedule a Single Notebook
# MAGIC
# MAGIC Use the Databricks Jobs API or UI to schedule notebook 03 as a daily ingestion job.

# COMMAND ----------

# JSON template for creating a scheduled job via the Jobs API
import json

single_job_config = {
    "name": "delta-sharing-demo-bronze-ingestion",
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/<your-user>/delta-sharing-demo/03_bronze_ingestion",
                "base_parameters": {
                    "force_full_reload": "false",
                    "starting_version": ""
                }
            },
            "existing_cluster_id": "<cluster-id>"  # Or use job_cluster_key for ephemeral
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",  # Daily at 6 AM UTC
        "timezone_id": "UTC"
    },
    "max_retries": 2,
    "retry_on_timeout": True,
    "email_notifications": {
        "on_failure": ["team@example.com"]
    }
}

print("Single-notebook Job config:")
print(json.dumps(single_job_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the job via CLI or API
# MAGIC
# MAGIC ```bash
# MAGIC # Via Databricks CLI
# MAGIC databricks jobs create --json '<paste config above>'
# MAGIC
# MAGIC # Via REST API
# MAGIC curl -X POST https://<workspace>/api/2.1/jobs/create \
# MAGIC   -H "Authorization: Bearer <token>" \
# MAGIC   -d '<paste config above>'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Multi-Task Workflow
# MAGIC
# MAGIC Chain the full medallion pipeline: `03 → 04 → 05 → {06, 07}` with dependency management.

# COMMAND ----------

workflow_config = {
    "name": "delta-sharing-demo-full-pipeline",
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/<your-user>/delta-sharing-demo/03_bronze_ingestion",
                "base_parameters": {"force_full_reload": "false"}
            },
            "job_cluster_key": "pipeline_cluster"
        },
        {
            "task_key": "silver_bitemporal",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/<your-user>/delta-sharing-demo/04_silver_bitemporal"
            },
            "job_cluster_key": "pipeline_cluster"
        },
        {
            "task_key": "gold_aggregations",
            "depends_on": [{"task_key": "silver_bitemporal"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/<your-user>/delta-sharing-demo/05_gold_aggregations"
            },
            "job_cluster_key": "pipeline_cluster"
        },
        {
            "task_key": "data_quality",
            "depends_on": [{"task_key": "gold_aggregations"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/<your-user>/delta-sharing-demo/06_data_quality_monitoring"
            },
            "job_cluster_key": "pipeline_cluster"
        },
        {
            "task_key": "audit_history",
            "depends_on": [{"task_key": "gold_aggregations"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/<your-user>/delta-sharing-demo/07_audit_history"
            },
            "job_cluster_key": "pipeline_cluster"
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "pipeline_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "data_security_mode": "SINGLE_USER"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "UTC"
    }
}

print("Multi-task Workflow config:")
print(json.dumps(workflow_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflow DAG
# MAGIC
# MAGIC ```
# MAGIC 03_bronze_ingestion
# MAGIC        │
# MAGIC        ▼
# MAGIC 04_silver_bitemporal
# MAGIC        │
# MAGIC        ▼
# MAGIC 05_gold_aggregations
# MAGIC       ╱ ╲
# MAGIC      ▼   ▼
# MAGIC  06_dq  07_audit    (run in parallel)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Passing State Between Tasks
# MAGIC
# MAGIC ### Pattern A: `control_cdf_checkpoint` (Durable)
# MAGIC
# MAGIC The checkpoint table persists across runs and is the primary mechanism
# MAGIC for CDF incremental processing. Each task reads its checkpoint at start
# MAGIC and writes it after success.
# MAGIC
# MAGIC ```python
# MAGIC # At start of task
# MAGIC ckpt = get_checkpoint("silver_prices")  # Returns last_processed_version
# MAGIC
# MAGIC # After successful processing
# MAGIC write_checkpoint("silver_prices", current_version, batch_id)
# MAGIC ```
# MAGIC
# MAGIC ### Pattern B: `dbutils.jobs.taskValues` (Within-Run)
# MAGIC
# MAGIC For passing ephemeral values between tasks in the same workflow run:
# MAGIC
# MAGIC ```python
# MAGIC # In task "bronze_ingestion" — set a value
# MAGIC dbutils.jobs.taskValues.set(key="bronze_row_count", value=12345)
# MAGIC dbutils.jobs.taskValues.set(key="batch_id", value="abc123")
# MAGIC
# MAGIC # In task "silver_bitemporal" — read the value
# MAGIC bronze_rows = dbutils.jobs.taskValues.get(
# MAGIC     taskKey="bronze_ingestion",
# MAGIC     key="bronze_row_count",
# MAGIC     default=0
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | Pattern | Scope | Persists Across Runs | Use Case |
# MAGIC |---------|-------|---------------------|----------|
# MAGIC | `control_cdf_checkpoint` | Cross-run | Yes | CDF version tracking |
# MAGIC | `dbutils.jobs.taskValues` | Single run | No | Row counts, status flags |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Structured Streaming from CDF
# MAGIC
# MAGIC For near-real-time processing, use Spark Structured Streaming to consume
# MAGIC CDF from bronze tables as a continuous stream.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger mode: availableNow (micro-batch, then stop)
# MAGIC
# MAGIC Processes all available CDF changes and stops — ideal for scheduled jobs.

# COMMAND ----------

# Example: stream bronze CDF into a staging table
# Uncomment to run:

# checkpoint_path = "/tmp/delta-sharing-demo/streaming/bronze_prices_ckpt"
#
# (
#     spark.readStream
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .table("delta_sharing_demo.bronze.bronze_market_prices")
#     .filter("_change_type IN ('insert', 'update_postimage')")
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", checkpoint_path)
#     .trigger(availableNow=True)
#     .toTable("delta_sharing_demo.bronze.bronze_prices_streaming_sink")
# )
#
# print("✓ availableNow streaming batch complete")

print("(Streaming example — uncomment to run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger mode: processingTime (continuous polling)
# MAGIC
# MAGIC Processes CDF changes every N seconds — ideal for low-latency pipelines.

# COMMAND ----------

# Example: continuous stream with 30-second micro-batches
# Uncomment to run (will run indefinitely until cancelled):

# checkpoint_path = "/tmp/delta-sharing-demo/streaming/bronze_prices_continuous_ckpt"
#
# query = (
#     spark.readStream
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .table("delta_sharing_demo.bronze.bronze_market_prices")
#     .filter("_change_type IN ('insert', 'update_postimage')")
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", checkpoint_path)
#     .trigger(processingTime="30 seconds")
#     .toTable("delta_sharing_demo.bronze.bronze_prices_continuous_sink")
# )
#
# # To stop: query.stop()

print("(Continuous streaming example — uncomment to run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Mode Comparison
# MAGIC
# MAGIC | Mode | Latency | Cost | Use Case |
# MAGIC |------|---------|------|----------|
# MAGIC | `availableNow=True` | Minutes | Low (ephemeral) | Scheduled pipelines, Jobs |
# MAGIC | `processingTime="30 seconds"` | Seconds | Medium (always-on) | Near-real-time dashboards |
# MAGIC | `processingTime="0 seconds"` | Sub-second | High (always-on) | Continuous processing |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Live Tables (DLT)
# MAGIC
# MAGIC For production pipelines, **Delta Live Tables** is the recommended approach.
# MAGIC DLT handles orchestration, schema enforcement, data quality, and auto-scaling.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Equivalent of Bronze + Silver
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="bronze_market_prices",
# MAGIC     comment="Bronze: raw market prices from Delta Share",
# MAGIC     table_properties={"quality": "bronze"}
# MAGIC )
# MAGIC def bronze_market_prices():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .option("readChangeFeed", "true")
# MAGIC         .table("shared_energy_market.energy_trading.market_prices_pjm")
# MAGIC         .withColumn("_ingested_at", F.current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="silver_prices_bitemporal",
# MAGIC     comment="Silver: SCD2 bitemporal prices",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
# MAGIC def silver_prices():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze_market_prices")
# MAGIC         .withColumn("entity_key", F.md5(F.concat_ws("||", "region", "interval_start")))
# MAGIC         # ... SCD2 logic via APPLY CHANGES INTO
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### DLT Migration Path
# MAGIC
# MAGIC | Current Approach | DLT Equivalent |
# MAGIC |-----------------|----------------|
# MAGIC | `spark.table()` batch read | `spark.readStream` in `@dlt.table` |
# MAGIC | Manual CDF + checkpoint | Automatic with `dlt.read_stream()` |
# MAGIC | Manual MERGE SCD2 | `APPLY CHANGES INTO` |
# MAGIC | Manual DQ checks | `@dlt.expect` / `@dlt.expect_or_drop` |
# MAGIC | Manual orchestration | DLT pipeline manages DAG |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Open Sharing — Non-Databricks Clients
# MAGIC
# MAGIC Delta Sharing supports an **open protocol** for clients outside Databricks.
# MAGIC Use the `delta-sharing` Python library to consume shared data from any environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC
# MAGIC ```bash
# MAGIC pip install delta-sharing
# MAGIC ```
# MAGIC
# MAGIC Download the sharing profile file from the Databricks UI:
# MAGIC **Catalog Explorer → Shares → Recipients → Download credential file**
# MAGIC
# MAGIC The profile file looks like:
# MAGIC ```json
# MAGIC {
# MAGIC   "shareCredentialsVersion": 1,
# MAGIC   "endpoint": "https://<workspace>.cloud.databricks.com/api/2.0/delta-sharing/",
# MAGIC   "bearerToken": "<token>"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load as Pandas
# MAGIC
# MAGIC ```python
# MAGIC import delta_sharing
# MAGIC
# MAGIC profile = "config.share"  # Path to profile file
# MAGIC
# MAGIC # List available tables
# MAGIC client = delta_sharing.SharingClient(profile)
# MAGIC for share in client.list_shares():
# MAGIC     for schema in client.list_schemas(share):
# MAGIC         for table in client.list_tables(schema):
# MAGIC             print(f"{share.name}.{schema.name}.{table.name}")
# MAGIC
# MAGIC # Load into pandas
# MAGIC table_url = f"{profile}#energy_market_share.energy_trading.market_prices_pjm"
# MAGIC df = delta_sharing.load_as_pandas(table_url)
# MAGIC print(f"Loaded {len(df)} rows")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load as Spark (outside Databricks)
# MAGIC
# MAGIC ```python
# MAGIC import delta_sharing
# MAGIC
# MAGIC profile = "config.share"
# MAGIC table_url = f"{profile}#energy_market_share.energy_trading.market_prices_pjm"
# MAGIC
# MAGIC df = delta_sharing.load_as_spark(table_url)
# MAGIC df.show(5)
# MAGIC ```
# MAGIC
# MAGIC > **Note:** `load_as_spark` requires a local Spark session with the delta-sharing connector.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Consumption Pattern Summary
# MAGIC
# MAGIC | Pattern | Latency | Complexity | Best For |
# MAGIC |---------|---------|-----------|----------|
# MAGIC | **Batch full read** (`spark.table()`) | Minutes | Low | Initial load, small tables |
# MAGIC | **CDF incremental** (`readChangeFeed`) | Minutes | Medium | Daily/hourly pipelines |
# MAGIC | **Streaming `availableNow`** | Minutes | Medium | Scheduled micro-batch |
# MAGIC | **Streaming `processingTime`** | Seconds | High | Near-real-time |
# MAGIC | **SQL VIEW** | Real-time | Low | Analysts, dashboards |
# MAGIC | **SQL CTAS** (snapshot) | Point-in-time | Low | Reproducible analysis |
# MAGIC | **Time travel** (`VERSION/TIMESTAMP AS OF`) | Historical | Low | Audit, debugging |
# MAGIC | **Delta Live Tables** | Seconds-Minutes | Medium | Production pipelines |
# MAGIC | **Open sharing** (`delta-sharing` lib) | Minutes | Low | Non-Databricks clients |
