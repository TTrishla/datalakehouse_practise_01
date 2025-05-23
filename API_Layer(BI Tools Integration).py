# Databricks notebook source
# adb-3398454134399276.16.azuredatabricks.net
#/sql/1.0/warehouses/b2a59c05be2382f3



# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON VIEW nyc_taxi_powerBI.monthly_summary TO `yash.trivedi15@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_taxi_powerBI.monthly_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Run this in Databricks if data refreshes are slow
# MAGIC /*CREATE MATERIALIZED VIEW nyc_taxi_powerbi.monthly_summary_fast
# MAGIC TBLPROPERTIES ('refresh_interval' = '1 hour')
# MAGIC AS SELECT * FROM nyc_taxi_powerBI.monthly_summary; */