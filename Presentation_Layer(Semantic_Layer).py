# Databricks notebook source
# Read Gold data
gold_df = spark.read.format("delta").load("dbfs:/mnt/datalake/gold/vendor_daily_metrics")


# COMMAND ----------

from pyspark.sql.functions import year, month, avg, sum, count

# Create OLAP cube with proper aggregations
olap_cube = (
    gold_df
    # Extract year/month from timestamp
    .withColumn("year", year("day"))
    .withColumn("month", month("day"))
    
    # Build cube dimensions
    .cube("year", "month")
    
    # Correct aggregations for your schema
    .agg(
        avg("avg_fare").alias("overall_avg_fare"),  # Average of daily averages
        sum("total_distance").alias("total_distance"),  # Sum of daily distances
        count("*").alias("num_days")  # Count of days in aggregation
    )
    .orderBy("year", "month")
)

# COMMAND ----------

# Save as Semantic Layer
# When schema keep changing
olap_cube.write.format("delta") \
    .option("overwriteSchema", "true")\
    .mode("overwrite") \
    .saveAsTable("nyc_taxi_semantic.olap_cube")  # Auto-registers"

# COMMAND ----------

# Visualize
display(olap_cube)