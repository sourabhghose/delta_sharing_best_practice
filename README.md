# Delta Sharing Demo — Cross-Workspace Energy Market Data

Demonstrates **Databricks Delta Sharing** across two workspaces using energy market data. A market operator (**Provider Workspace**) shares trading and generation data with an energy retailer (**Recipient Workspace**) who builds a full medallion architecture with bitemporal tracking, CDF incremental processing, data quality monitoring, and audit trails.

## Architecture

```
┌─────────────────────────────┐         Delta Sharing         ┌──────────────────────────────────┐
│      Provider Workspace     │ ──────────────────────────►   │      Recipient Workspace         │
│      (Market Operator)      │                               │      (Energy Retailer)           │
│                             │                               │                                  │
│  energy_utilities catalog   │                               │  delta_sharing_demo catalog      │
│  ├─ energy_trading          │                               │  ├─ bronze (append-only)         │
│  │  ├─ market_prices_pjm   │                               │  ├─ silver (bitemporal SCD2)     │
│  │  ├─ gold_daily_trading   │                               │  ├─ gold  (aggregations)         │
│  │  └─ positions            │                               │  ├─ audit (CDF archive)          │
│  └─ power_generation        │                               │  └─ control (freshness/DQ)       │
│     ├─ iso_market           │                               │                                  │
│     └─ turbine_locations    │                               │                                  │
└─────────────────────────────┘                               └──────────────────────────────────┘
```

## Workspace Details

| Role | Workspace | Catalog |
|------|-----------|---------|
| **Provider** | Provider Workspace (Market Operator) | `energy_utilities` |
| **Recipient** | Recipient Workspace (Energy Retailer) | `delta_sharing_demo` (created by notebook 02) |

## Shared Tables (Provider)

| Table | Description |
|-------|-------------|
| `energy_utilities.energy_trading.market_prices_pjm` | PJM market prices |
| `energy_utilities.energy_trading.gold_daily_trading_summary` | Daily trading summaries |
| `energy_utilities.energy_trading.positions` | Trading positions |
| `energy_utilities.power_generation.iso_market` | ISO market data |
| `energy_utilities.power_generation.turbine_locations` | Turbine locations |

## Notebooks

| # | Notebook | Run On | Description |
|---|----------|--------|-------------|
| 00 | `demo_overview` | Either | Architecture diagram, connectivity check, demo scenario |
| 01 | `provider_setup` | **Provider Workspace** | Enable CDF, create share, add tables, create recipient |
| 02 | `recipient_catalog_setup` | **Recipient Workspace** | Create provider, shared catalog, demo catalog + schemas |
| 03 | `bronze_ingestion` | **Recipient Workspace** | Append-only bronze layer with ingestion metadata |
| 04 | `silver_bitemporal` | **Recipient Workspace** | SCD2 bitemporal MERGE with valid/transaction time |
| 05 | `gold_aggregations` | **Recipient Workspace** | Daily summaries, KPIs, generation mix |
| 06 | `data_quality_monitoring` | **Recipient Workspace** | DQ checks, schema drift, freshness SLAs |
| 07 | `audit_history` | **Recipient Workspace** | Permanent CDF archive, reconciliation, compliance |
| 08 | `scheduling_and_orchestration` | **Recipient Workspace** | Jobs, Workflows, streaming, DLT, open sharing |
| 09 | `recipient_best_practices` | **Recipient Workspace** | Access control, performance, governance, runbook |

## How to Run

1. **Upload** `01_provider_setup.py` to the **Provider Workspace**
2. **Upload** all other notebooks (`00`, `02`–`09`) to the **Recipient Workspace**
3. **Run `01`** on the Provider Workspace to set up sharing infrastructure
4. **Run `02`–`07` sequentially** on the Recipient Workspace to build the medallion pipeline
5. **Verify** shared catalog has tables, bronze/silver/gold/audit tables are populated
6. **(Optional)** Review `08_scheduling_and_orchestration` for Jobs, streaming, and DLT patterns
7. **(Optional)** Review `09_recipient_best_practices` for access control, governance, and operational guidance

## Key Concepts Demonstrated

- **Delta Sharing** — Databricks-to-Databricks (D2D) cross-workspace sharing
- **Change Data Feed (CDF)** — Incremental processing of shared table changes
- **Incremental CDF Read** — Checkpoint-driven incremental reads across bronze/silver/gold
- **Bitemporal Modeling** — SCD Type 2 with valid time + transaction time
- **Medallion Architecture** — Bronze (raw) → Silver (curated) → Gold (aggregated)
- **SQL Consumption Patterns** — VIEWs, CTAS snapshots, time-travel (`VERSION AS OF`, `TIMESTAMP AS OF`)
- **Structured Streaming** — `readStream` from CDF with `availableNow` and `processingTime` triggers
- **Databricks Jobs & Workflows** — Single-notebook scheduling, multi-task DAGs, widget parameterization
- **Delta Live Tables** — Production-grade pipeline orchestration with `@dlt.table`
- **Data Quality** — Schema drift detection, null checks, freshness SLAs
- **Audit Trail** — Permanent CDF archive for compliance and reconciliation
- **Recipient Best Practices** — Access control (GRANT, row filters, column masks), performance tuning, UC governance, operational runbook
