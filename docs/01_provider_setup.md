# Provider Setup Guide

## Overview

The **Provider Workspace** hosts the source data and configures Delta Sharing to make it available to the Recipient Workspace. All steps in this guide are run on the Provider Workspace.

## Prerequisites

- Admin or data owner access on the Provider Workspace
- Unity Catalog enabled
- Tables exist in the `energy_utilities` catalog

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

**Note:** CDF is not enabled on `iso_market` and `turbine_locations` as these are reference/static tables that change infrequently and can be fully reloaded.

### 2. Create the Share

```sql
CREATE SHARE IF NOT EXISTS energy_market_share;
```

### 3. Add Tables to Share

Tables are added with schema aliases and optionally with history sharing enabled:

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

`WITH HISTORY` enables the Recipient Workspace to read CDF and use time-travel queries against shared tables.

### 4. Create D2D Recipient

Get the sharing identifier from the Recipient Workspace:
**Settings → Workspace Settings → General → Sharing Identifier**

```sql
CREATE RECIPIENT IF NOT EXISTS energy_copilot_recipient
USING ID '<recipient-sharing-identifier>';
```

### 5. Grant Access

```sql
GRANT SELECT ON SHARE energy_market_share TO RECIPIENT energy_copilot_recipient;
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
| "Cannot add table with history" | Table must have CDF enabled before adding `WITH HISTORY` |
