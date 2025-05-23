# Databricks notebook source
# not mandatory
display(dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow/"))

# COMMAND ----------

# Read all files with partitioning
full_df = spark.read.csv(
    "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz",
    header=True,
    inferSchema=True, #even if schema changes still we will get data  
)


# COMMAND ----------

display(full_df)

# COMMAND ----------

# Write to Bronze (Raw) Zone as Delta
# "append" for incremental refresh
full_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_taxi_bronze.yellow_taxi") # Uses pre-registered schema when not using adls2 

# COMMAND ----------

# Verify (data validation)
display(spark.read.format("delta").load("dbfs:/mnt/datalake/bronze/nyc_taxi_yellow/2019").limit(5))
# List all files in the 2019 partition
display(dbutils.fs.ls("dbfs:/mnt/datalake/bronze/nyc_taxi_yellow/2019"))
