# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_taxi_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_taxi_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_taxi_gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_taxi_semantic;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_taxi_powerBI;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create empty table with schema definition
# MAGIC --DROP TABLE IF EXISTS nyc_taxi_bronze.yellow_taxi;
# MAGIC CREATE TABLE IF NOT EXISTS nyc_taxi_bronze.yellow_taxi (
# MAGIC   VendorID INT,
# MAGIC   tpep_pickup_datetime TIMESTAMP,
# MAGIC   tpep_dropoff_datetime TIMESTAMP,
# MAGIC   passenger_count INT,
# MAGIC   trip_distance DOUBLE,
# MAGIC   RatecodeID INT,
# MAGIC   store_and_fwd_flag STRING,
# MAGIC   PULocationID INT,
# MAGIC   DOLocationID INT,
# MAGIC   payment_type INT,
# MAGIC   fare_amount DOUBLE,
# MAGIC   extra DOUBLE,
# MAGIC   mta_tax DOUBLE,
# MAGIC   tip_amount DOUBLE,
# MAGIC   tolls_amount DOUBLE,
# MAGIC   improvement_surcharge DOUBLE,
# MAGIC   total_amount DOUBLE,
# MAGIC   congestion_surcharge DOUBLE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'NYC Yellow Taxi trips with all standard columns';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver Layer Table Creation (Correct Syntax)
# MAGIC CREATE TABLE IF NOT EXISTS nyc_taxi_silver.cleaned_trips (
# MAGIC   VendorID INT,
# MAGIC   tpep_pickup_datetime TIMESTAMP,
# MAGIC   tpep_dropoff_datetime TIMESTAMP,
# MAGIC   passenger_count INT,
# MAGIC   trip_distance DOUBLE,
# MAGIC   RatecodeID INT,
# MAGIC   store_and_fwd_flag STRING,
# MAGIC   PULocationID INT,
# MAGIC   DOLocationID INT,
# MAGIC   payment_type INT,
# MAGIC   fare_amount DOUBLE,
# MAGIC   extra DOUBLE,
# MAGIC   mta_tax DOUBLE,
# MAGIC   tip_amount DOUBLE,
# MAGIC   tolls_amount DOUBLE,
# MAGIC   improvement_surcharge DOUBLE,
# MAGIC   total_amount DOUBLE,
# MAGIC   congestion_surcharge DOUBLE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'NYC Yellow Taxi trips - Cleaned Silver Layer';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Layer (Daily Aggregations)
# MAGIC CREATE TABLE IF NOT EXISTS nyc_taxi_gold.daily_metrics (
# MAGIC   VendorID INT,
# MAGIC   day DATE,  -- Using DATE instead of TIMESTAMP for daily aggregates
# MAGIC   avg_fare DOUBLE,
# MAGIC   total_distance DOUBLE,
# MAGIC   trip_count BIGINT  -- Added missing metric
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Daily aggregated taxi metrics (Gold layer)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Semantic_Layer (For OLAP cube table)
# MAGIC --drop table if exists nyc_taxi_semantic.olap_cube
# MAGIC CREATE TABLE IF NOT EXISTS nyc_taxi_semantic.olap_cube (
# MAGIC   year INT,
# MAGIC   month INT,
# MAGIC   overall_avg_fare DOUBLE,
# MAGIC   total_distance DOUBLE,
# MAGIC   num_days LONG
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Pre-aggregated OLAP cube for taxi metrics';

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Create a simple view for Power BI
# MAGIC CREATE OR REPLACE VIEW nyc_taxi_powerBI.monthly_summary AS
# MAGIC SELECT 
# MAGIC   year, 
# MAGIC   month,
# MAGIC   total_distance,
# MAGIC   AVG(overall_avg_fare) AS avg_fare
# MAGIC FROM nyc_taxi_semantic.olap_cube
# MAGIC GROUP BY year, month, total_distance;