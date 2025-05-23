# Databricks notebook source
from pyspark.sql.functions import date_trunc

# Read Silver data
silver_df = spark.read.format("delta").load("dbfs:/mnt/datalake/silver/nyc_taxi_cleaned")

# COMMAND ----------

# Time-Series Analysis (reporting table_logic_pre_aggregated_table)
# Daily aggregations by vendor
time_series = (
    silver_df
    .withColumn("day", date_trunc("day", "tpep_pickup_datetime"))
    .groupBy("VendorID", "day")
    .agg(
        {"fare_amount": "avg", "trip_distance": "sum"}
    )
    .withColumnRenamed("avg(fare_amount)", "avg_fare")  # Clean column names
    .withColumnRenamed("sum(trip_distance)", "total_distance")
)

# COMMAND ----------

# Write to Gold (Metrics) Zone
(
    time_series.write
    .format("delta")
    .mode("overwrite")  # Use "append" for incremental loads (full load and how analytics layer is different)
    .saveAsTable("nyc_taxi_gold.vendor_daily_metrics")
)

# COMMAND ----------

#verify