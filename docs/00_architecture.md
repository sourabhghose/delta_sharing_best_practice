# Architecture Overview

## Demo Scenario

A **market operator** (e2-demo-west) shares energy trading and generation data with an **energy retailer** (fevm-energy-copilot) using Databricks Delta Sharing. The retailer builds a full medallion architecture on top of the shared data.

## Data Flow

```
┌──────────────────────────────────┐
│         e2-demo-west             │
│         (Provider)               │
│                                  │
│   energy_utilities catalog       │
│   ├─ energy_trading              │
│   │  ├─ market_prices_pjm       │  CDF enabled
│   │  ├─ gold_daily_trading_sum   │  CDF enabled
│   │  └─ positions                │  CDF enabled
│   └─ power_generation            │
│      ├─ iso_market               │
│      └─ turbine_locations        │
│                                  │
│   Share: energy_market_share     │
│   Recipient: fevm_energy_copilot │
└──────────────┬───────────────────┘
               │
               │  Delta Sharing (D2D)
               │  Cross-workspace, no data copy
               │
               ▼
┌──────────────────────────────────┐
│      fevm-energy-copilot         │
│      (Recipient)                 │
│                                  │
│   Provider: e2_demo_west_provider│
│   Foreign catalog:               │
│     shared_energy_market         │
│                                  │
│   delta_sharing_demo catalog     │
│   ├─ bronze (append-only)        │  Ingestion metadata, CDF
│   ├─ silver (bitemporal SCD2)    │  Valid time + transaction time
│   ├─ gold  (aggregations)        │  Daily summaries, KPIs
│   ├─ audit (CDF archive)         │  Permanent changelog
│   └─ control (DQ + freshness)    │  SLA tracking
└──────────────────────────────────┘
```

## Networking

Delta Sharing D2D uses Databricks-managed networking. No VPC peering or private link configuration is needed between workspaces.

Requirements:
- Both workspaces must be on Unity Catalog
- Provider creates a share and adds tables
- Provider creates a D2D recipient using the recipient workspace's sharing identifier
- Recipient creates a provider and a foreign catalog from the share

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Sharing method | D2D (Databricks-to-Databricks) | Both workspaces are on Databricks; D2D is simpler than open sharing |
| CDF on provider | Enabled on trading tables | Enables incremental processing and audit trails |
| Bronze strategy | Append-only with metadata | Preserves all versions, enables reprocessing |
| Silver modeling | Bitemporal SCD2 | Separates "when it happened" from "when we knew" |
| Gold optimization | Liquid clustering + Z-ORDER | Optimizes for date+region query patterns |
| Audit retention | 365 days (extendable) | Compliance-ready permanent record |

## Catalogs and Schemas

### Provider (e2-demo-west)
- `energy_utilities.energy_trading` — market prices, trading summaries, positions
- `energy_utilities.power_generation` — ISO market data, turbine locations

### Recipient (fevm-energy-copilot)
- `shared_energy_market` — foreign catalog (read-only view of shared data)
- `delta_sharing_demo.bronze` — raw ingested data with metadata
- `delta_sharing_demo.silver` — bitemporal curated data
- `delta_sharing_demo.gold` — aggregated summaries
- `delta_sharing_demo.audit` — permanent CDF changelog
- `delta_sharing_demo.control` — DQ results and freshness tracking
