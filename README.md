# 003 - Azure Stock Market Analytics Pipeline with SCD Type 2

## Overview
An enterprise-grade Azure Data Engineering pipeline that ingests daily stock price
data for MSFT, AAPL and GOOGL via Alpha Vantage free API using ADF ForEach,
implements SCD Type 2 history tracking using Delta Lake MERGE, demonstrates
Delta time travel for historical auditing, applies incremental loading pattern,
uses PySpark broadcast variables for performance optimization, and loads curated
data into Azure SQL - automated every weekday at 6PM IST.

---

## Architecture
```
Alpha Vantage Free API — MSFT, AAPL, GOOGL
  Daily OHLCV: Open, High, Low, Close, Volume
         |
         v

ADF: pl_003_stock_analytics (Parameterized + ForEach)
  
  Parameters: stock_symbols (array), api_key, run_date
  
  ForEach (IterateSymbols) [parallel, all 3 symbols]
    Web Activity : FetchStockData (dynamic URL, Retry=3)
    Copy Data    : SaveRawToADLS (dataset parameter pattern)
    Path: raw/{symbol}/{yyyy-MM-dd}/stock_data.json
  
  Databricks: nb_01_ingest_stocks
    - Flattens nested Time Series Daily JSON
    - Incremental load: left anti join skips existing dates
    - Appends new records to processed Delta
  
  Databricks: nb_02_scd2_transform
    - Broadcast variables for price classification thresholds
    - Window function: daily price change % calculation
    - BULLISH / BEARISH / NEUTRAL classification
    - SCD Type 2 columns: effective_date, expiry_date, is_current
    - Delta MERGE: upsert with full history preservation
    - Delta Time Travel: verifies all versions preserved
  
  Databricks: nb_03_load_to_sql
    - dbo.stock_prices_current (is_current=True records only)
    - dbo.stock_prices_history (full SCD2 history)
  
  Web Activity: Gmail alerts on success and failure
         |
         v

Azure SQL: db-stocks
  dbo.stock_prices_current  -- latest prices for BI dashboards
  dbo.stock_prices_history  -- complete SCD2 audit trail

```

---

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory V2 | Orchestration, ForEach, parameterized pipeline, weekday trigger |
| Alpha Vantage API | Free daily OHLCV stock data - 25 calls/day free tier |
| Azure Data Lake Storage Gen2 | Raw, processed, analytics storage layers |
| Azure Databricks + PySpark | Ingestion, SCD2 transform, SQL load |
| Delta Lake | Versioned storage, MERGE, time travel, incremental append |
| Azure SQL Database | BI-ready current and historical tables |
| Azure Logic Apps | Gmail alerting on success and failure |

---

## Advanced Concepts
| Concept | Implementation |
|---------|---------------|
| SCD Type 2 | Delta MERGE - effective_date, expiry_date, is_current tracking |
| Delta Time Travel | versionAsOf - query any historical snapshot |
| Delta Table History | Full audit trail of all MERGE operations |
| Incremental Loading | Left anti join - only new trading dates processed |
| Broadcast Variables | sparkContext.broadcast() for threshold distribution |
| Price Movement Classification | BULLISH / BEARISH / NEUTRAL using broadcast thresholds |
| ADF ForEach Multi-Symbol | Parallel ingestion of 3 stock symbols |
| Dataset Parameters | item() passed via Dataset properties - correct ADF pattern |
| API Retry Handling | Retry=3, 60s interval on Web Activity |

---

## Data Source
Alpha Vantage: alphavantage.co
Free tier: 25 API calls/day - register at alphavantage.co/support/#api-key
Demo key: 'demo' returns MSFT data only (no registration needed for testing)
Symbols: MSFT (Microsoft) | AAPL (Apple) | GOOGL (Google)

---



## Repository Structure
```
003-stock-market-analytics/
├── notebooks/
|   ├── nb_01_ingest_stocks.py
|   ├── nb_02_scd2_transform.py
|   └── nb_03_load_to_sql.py
├── adf-pipelines/
|   └── pl_003_stock_analytics.json
├── data/
|   └── sample_msft_stock_data.json
├── docs/
|   ├── adf_pipeline_overview.png
|   ├── adf_foreach_3_symbols.png
|   ├── raw_stock_files.png
|   ├── incremental_load_output.png
|   ├── scd2_merge_output.png
|   ├── delta_time_travel.png
|   ├── sql_current_output.png
|   └── email_alert_received.png
└── README.md
```

---

## Contact
Purusottam Swain (***purusottam.builds@gmail.com***)
- ***[Upwork](https://www.upwork.com/freelancers/~017164fcff771e794c?mp_source=share)***
- ***[Fiverr](https://www.fiverr.com/purusottam_sn?public_mode=true)***

---