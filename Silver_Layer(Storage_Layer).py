# Databricks notebook source
# Read from Bronze
bronze_df = spark.read.format("delta").load("dbfs:/mnt/datalake/bronze/nyc_taxi_yellow/2019")

# COMMAND ----------

# Data Cleaning(null values, duplicates, filters, typecast)
from pyspark.sql.functions import col, to_timestamp

silver_df = bronze_df \
    .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
    .filter(col("passenger_count") > 0) \
    .dropDuplicates()

# COMMAND ----------

# Write to Silver (Cleaned) Zone
'''silver_df.write.format("delta") \
    .mode("overwrite") \
    .save("dbfs:/mnt/datalake/silver/nyc_taxi_cleaned")'''

silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_taxi_silver.cleaned_trips")

# COMMAND ----------

#verify