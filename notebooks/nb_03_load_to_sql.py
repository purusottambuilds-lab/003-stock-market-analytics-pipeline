# Databricks notebook source
# ================================================================
# nb_03_load_to_sql
# Project 003   : Stock Market Analytics with SCD Type 2
# Purpose       : Load SCD2 analytics Delta to Azure SQL Database via JDBC
# Folder        : dir_003_stock_market
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1 - Storage and SQL Configuration

storage_account_name = "sastocksps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

sql_server = "sql-buildlab-de-ps01.database.windows.net"
sql_database = "db-stocks"
sql_user = "sqladmin-buildlab-de-ps01"
sql_password = "YOUR_SQL_PASSWORD_HERE"

sql_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    f"user={sql_user};"
    f"password={sql_password};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30"
)

scd2_path = f"abfss://analytics@{storage_account_name}.dfs.core.windows.net/stock_scd2"

print("Config set")


# COMMAND ----------

# CELL 2 - Test SQL Connection Before Writing

import json

try:
    df_test = (
        spark.read.format("jdbc")
        .option("url", sql_url)
        .option("query", "SELECT 1 AS test")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )
    print("SQL connection: SUCCESS")
except Exception as e:
    raise Exception(f"SQL connection FAILED: {e}")


# COMMAND ----------

# CELL 3 — Write Current Records to dbo.stock_prices_current

# Only is_current=True records - this is what BI tools and dashboards query
from pyspark.sql.functions import col

df_scd2 = spark.read.format("delta").load(scd2_path)
df_current = df_scd2.filter(col("is_current") == True)
print(f"Current records to load: {df_current.count()}")

df_current.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.stock_prices_current"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.stock_prices_current: written successfully")


# COMMAND ----------

# CELL 4 - Write Full SCD2 History to dbo.stock_prices_history

print(f"Full SCD2 history records: {df_scd2.count()}")

df_scd2.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.stock_prices_history"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.stock_prices_history: written successfully")


# COMMAND ----------

# CELL 5 - Verify SQL Tables

df_verify = (
    spark.read.format("jdbc")
    .option("url", sql_url)
    .option("dbtable", "dbo.stock_prices_current")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

print(f"SQL dbo.stock_prices_current rows: {df_verify.count()}")
display(
    df_verify.select(
        "symbol", "trade_date", "close_price", "movement", "price_change_pct"
    ).limit(10)
)


# COMMAND ----------

# CELL 6 - Return Status to ADF

exit_value = json.dumps(
    {
        "status": "SUCCESS",
        "current_rows": df_current.count(),
        "history_rows": df_scd2.count(),
    }
)
dbutils.notebook.exit(exit_value)


# COMMAND ----------
