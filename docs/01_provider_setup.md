# Provider Setup Guide

## Overview

The provider workspace (`e2-demo-west`) hosts the source data and configures Delta Sharing to make it available to the recipient workspace.

## Prerequisites

- Admin or data owner access on `e2-demo-west`
- Unity Catalog enabled
- Tables exist in `energy_utilities` catalog

## Steps

### 1. Enable Change Data Feed (CDF)

CDF must be enabled on tables where you want to track incremental changes. This is required for the recipient to do incremental processing and audit trail capture.

```sql
ALTER TABLE energy_utilities.energy_trading.market_prices_pjm
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE energy_utilities.energy_trading.gold_daily_trading_summary
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE energy_utilities.energy_trading.positions
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**Note:** CDF is not enabled on `iso_market` and `turbine_locations` as these are reference/static tables.

### 2. Create the Share

```sql
CREATE SHARE IF NOT EXISTS energy_market_share;
```

### 3. Add Tables to Share

Tables are added with aliases and optionally with history sharing:

```sql
ALTER SHARE energy_market_share ADD TABLE energy_utilities.energy_trading.market_prices_pjm
  AS energy_trading.market_prices_pjm WITH HISTORY;

ALTER SHARE energy_market_share ADD TABLE energy_utilities.energy_trading.gold_daily_trading_summary
  AS energy_trading.gold_daily_trading_summary WITH HISTORY;

ALTER SHARE energy_market_share ADD TABLE energy_utilities.energy_trading.positions
  AS energy_trading.positions WITH HISTORY;

ALTER SHARE energy_market_share ADD TABLE energy_utilities.power_generation.iso_market
  AS power_generation.iso_market;

ALTER SHARE energy_market_share ADD TABLE energy_utilities.power_generation.turbine_locations
  AS power_generation.turbine_locations;
```

`WITH HISTORY` enables the recipient to read CDF and time-travel queries.

### 4. Create D2D Recipient

Get the sharing identifier from the recipient workspace (Settings > General > Sharing Identifier):

```sql
CREATE RECIPIENT IF NOT EXISTS fevm_energy_copilot
USING ID '<recipient-sharing-identifier>';
```

### 5. Grant Access

```sql
GRANT SELECT ON SHARE energy_market_share TO RECIPIENT fevm_energy_copilot;
```

### 6. Verify

```sql
DESCRIBE SHARE energy_market_share;
SHOW ALL IN SHARE energy_market_share;
SHOW GRANTS ON SHARE energy_market_share;
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Share already exists" | Use `DROP SHARE IF EXISTS` first, or skip creation |
| "Recipient already exists" | Safe to skip — recipient creation is idempotent |
| CDF not available | Ensure table is Delta format and has been written to after CDF enablement |
| "Cannot add table with history" | Table must have CDF enabled first |
