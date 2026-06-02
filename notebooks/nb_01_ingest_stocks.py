# Databricks notebook source
# ================================================================
# nb_01_ingest_stocks
# Project 003   : Stock Market Analytics with SCD Type 2
# Purpose       : Read raw JSON for all symbols, flatten Time Series data, incremental load to processed Delta Lake
# Folder        : dir_003_stock_market
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1 - Read Parameters from ADF

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("stock_symbols", "MSFT,AAPL,GOOGL")

run_date = dbutils.widgets.get("run_date")
stock_symbols = dbutils.widgets.get("stock_symbols").split(",")

if not run_date:
    from datetime import datetime

    run_date = datetime.now().strftime("%Y-%m-%d")

print(f"Run date    : {run_date}")
print(f"Symbols     : {stock_symbols}")


# COMMAND ----------

# CELL 2 - Storage Configuration

from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import DoubleType, LongType
from datetime import datetime
import json

storage_account_name = "sastocksps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

raw_base = f"abfss://raw@{storage_account_name}.dfs.core.windows.net"
processed_path = (
    f"abfss://processed@{storage_account_name}.dfs.core.windows.net/stock_prices_delta"
)

print(f"Storage configured: {storage_account_name}")


# COMMAND ----------

# CELL 3 - Read and Flatten Raw JSON for All Symbols

# Alpha Vantage returns nested: {'Time Series (Daily)': {'2026-05-23': {ohlcv}}}
# We flatten this into individual rows per trading date

all_symbol_dfs = []

for symbol in stock_symbols:
    symbol = symbol.strip()
    raw_path = f"{raw_base}/{symbol}/{run_date}/stock_data.json"

    try:
        df_raw = spark.read.option("multiline", "true").json(raw_path)

        # Convert to Python dict for easy nested parsing
        raw_content = df_raw.toJSON().first()
        data = json.loads(raw_content)
        time_series = data.get("Time Series (Daily)", {})

        if not time_series:
            print(
                f"  WARNING: No time series data for {symbol} — API may have returned error"
            )
            print(f"  Raw response: {str(data)[:200]}")
            continue

        rows = []
        for date_str, values in time_series.items():
            rows.append(
                {
                    "trade_date": date_str,
                    "symbol": symbol,
                    "open_price": float(values.get("1. open", 0)),
                    "high_price": float(values.get("2. high", 0)),
                    "low_price": float(values.get("3. low", 0)),
                    "close_price": float(values.get("4. close", 0)),
                    "volume": int(values.get("5. volume", 0)),
                }
            )

        df_symbol = spark.createDataFrame(rows)
        df_symbol = df_symbol.withColumn("trade_date", to_date(col("trade_date")))

        all_symbol_dfs.append(df_symbol)
        print(f"  {symbol}: {df_symbol.count()} trading days loaded")

    except Exception as e:
        print(f"  WARNING: Could not read {symbol} — {str(e)[:120]}")
        print(f"  Skipping {symbol} for this run")

if not all_symbol_dfs:
    raise Exception(
        "No symbol data could be read. Verify ADF ForEach ran successfully first."
    )

from functools import reduce
from pyspark.sql import DataFrame

df_all = reduce(DataFrame.union, all_symbol_dfs)
print(f"Total records across all symbols: {df_all.count()}")
display(df_all.orderBy("symbol", "trade_date").limit(10))


# COMMAND ----------

# CELL 4 - Incremental Load: only process new trading dates

# This is the incremental loading pattern — avoids reprocessing history

try:
    df_existing = spark.read.format("delta").load(processed_path)
    existing_dates = df_existing.select("symbol", "trade_date").distinct()

    # Left anti join: keep only rows NOT already in processed Delta
    df_new = df_all.join(existing_dates, on=["symbol", "trade_date"], how="left_anti")

    print(f"Existing records : {df_existing.count()}")
    print(f"New records      : {df_new.count()}")

except Exception as e:
    df_new = df_all
    print(f"First run — processing all {df_new.count()} records")


# COMMAND ----------

# CELL 5 - Append New Records to Processed Delta

if df_new.count() > 0:
    df_new.withColumn("ingested_at", current_timestamp()).write.format("delta").mode(
        "append"
    ).option("mergeSchema", "true").save(processed_path)
    print(f"Appended {df_new.count()} new records to processed Delta")
else:
    print("No new records — already up to date")

# Verify
df_verify = spark.read.format("delta").load(processed_path)
print(f"Processed Delta total: {df_verify.count()} records")
display(df_verify.groupBy("symbol").count().orderBy("symbol"))


# COMMAND ----------

# CELL 6 - Return Status to ADF

exit_value = json.dumps(
    {
        "status": "SUCCESS",
        "new_records": df_new.count(),
        "total_records": df_verify.count(),
        "run_date": run_date,
    }
)
dbutils.notebook.exit(exit_value)


# COMMAND ----------
