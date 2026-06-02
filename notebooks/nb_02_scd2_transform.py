# Databricks notebook source
# ================================================================
# nb_02_scd2_transform
# Project 003   : Stock Market Analytics with SCD Type 2
# Purpose       : SCD Type 2 using Delta MERGE, price movement classification, broadcast variables, Delta Time Travel verification
# Folder        : dir_003_stock_market
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1 - Read Parameters from ADF

dbutils.widgets.text("run_date", "")

run_date = dbutils.widgets.get("run_date")
if not run_date:
    from datetime import datetime

    run_date = datetime.now().strftime("%Y-%m-%d")

print(f"Run date: {run_date}")


# COMMAND ----------

# CELL 2 - Storage Configuration

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    current_date,
    when,
    lag,
    round as spark_round,
)
from pyspark.sql.types import BooleanType, StringType
from pyspark.sql import Window
import json

storage_account_name = "sastocksps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

processed_path = (
    f"abfss://processed@{storage_account_name}.dfs.core.windows.net/stock_prices_delta"
)
scd2_path = f"abfss://analytics@{storage_account_name}.dfs.core.windows.net/stock_scd2"

print("Storage configured")


# COMMAND ----------

# CELL 3 - Broadcast Variables for Price Classification Thresholds

thresholds = {
    "bullish_pct": 1.5,  # price up > 1.5% = BULLISH
    "bearish_pct": -1.5,  # price down > 1.5% = BEARISH
}

broadcast_thresholds = spark.sparkContext.broadcast(thresholds)
print(f"Broadcast thresholds: {broadcast_thresholds.value}")

bullish_threshold = broadcast_thresholds.value["bullish_pct"]
bearish_threshold = broadcast_thresholds.value["bearish_pct"]


# COMMAND ----------

# CELL 4 - Read Processed Data and Calculate Price Movement

df_processed = spark.read.format("delta").load(processed_path)

# Calculate daily price change percentage using Window function
window_symbol = Window.partitionBy("symbol").orderBy("trade_date")

df_with_movement = (
    df_processed.withColumn("prev_close", lag("close_price", 1).over(window_symbol))
    .withColumn(
        "price_change_pct",
        when(
            col("prev_close").isNotNull(),
            spark_round(
                (col("close_price") - col("prev_close")) / col("prev_close") * 100, 2
            ),
        ).otherwise(lit(0.0)),
    )
    .withColumn(
        "movement",
        when(col("price_change_pct") >= bullish_threshold, lit("BULLISH"))
        .when(col("price_change_pct") <= bearish_threshold, lit("BEARISH"))
        .otherwise(lit("NEUTRAL")),
    )
)

print("Price movement distribution:")
display(
    df_with_movement.groupBy("symbol", "movement").count().orderBy("symbol", "movement")
)


# COMMAND ----------

# CELL 5 - Add SCD Type 2 Columns

# SCD2 tracks complete history:
#   effective_date: when this record became current
#   expiry_date   : when this record was superseded (9999-12-31 = still current)
#   is_current    : True = current record, False = historical record

df_scd2_source = (
    df_with_movement.withColumn("effective_date", col("trade_date"))
    .withColumn("expiry_date", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True).cast(BooleanType()))
    .withColumn("updated_at", current_timestamp())
)

print(f"SCD2 source records: {df_scd2_source.count()}")
display(
    df_scd2_source.select(
        "symbol",
        "trade_date",
        "close_price",
        "movement",
        "effective_date",
        "expiry_date",
        "is_current",
    ).limit(5)
)


# COMMAND ----------

# CELL 6 - Delta MERGE for SCD Type 2

# MERGE logic:
#   When matched AND price changed: update existing record
#   When not matched: insert new record
# This preserves all historical versions

try:
    scd2_table = DeltaTable.forPath(spark, scd2_path)
    print("SCD2 table exists — running MERGE")

    scd2_table.alias("target").merge(
        df_scd2_source.alias("source"),
        "target.symbol = source.symbol AND target.trade_date = source.trade_date AND target.is_current = true",
    ).whenMatchedUpdate(
        set={
            "target.close_price": "source.close_price",
            "target.volume": "source.volume",
            "target.movement": "source.movement",
            "target.price_change_pct": "source.price_change_pct",
            "target.updated_at": "source.updated_at",
        }
    ).whenNotMatchedInsertAll().execute()

    print("SCD2 MERGE completed")

except Exception as e:
    print(f"Creating SCD2 table for first time: {str(e)[:80]}")
    df_scd2_source.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).save(scd2_path)
    print("SCD2 table created")


# COMMAND ----------

# CELL 7 - Delta Time Travel Verification

scd2_table_final = DeltaTable.forPath(spark, scd2_path)

print("Delta Table History:")
display(scd2_table_final.history())

# Read current version
df_current = spark.read.format("delta").load(scd2_path)
print(f"Current version records: {df_current.count()}")

# Read version 0 (original snapshot — time travel)
try:
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(scd2_path)
    print(f"Version 0 records: {df_v0.count()}")
    display(df_v0.select("symbol", "trade_date", "close_price", "is_current").limit(5))
except Exception as e:
    print(f"Only one version exists yet: {str(e)[:60]}")

# Show current records only
display(
    df_current.filter(col("is_current") == True)
    .orderBy("symbol", "trade_date", ascending=False)
    .limit(10)
)


# COMMAND ----------

# CELL 8 - Return Status to ADF

df_final = spark.read.format("delta").load(scd2_path)
exit_value = json.dumps(
    {"status": "SUCCESS", "scd2_records": df_final.count(), "run_date": run_date}
)
dbutils.notebook.exit(exit_value)


# COMMAND ----------
