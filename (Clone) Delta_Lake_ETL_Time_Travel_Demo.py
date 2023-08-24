# Databricks notebook source
# Step 1: Install Required Libraries
!pip install -q delta-spark

# COMMAND ----------

# Step 2: Load Sample Data
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.appName('Delta_Lake_ETL_Time_Travel_Demo').getOrCreate()

# Load sample data
data = [
    ('Alice', '2023-08-01', 100),
    ('Bob', '2023-08-02', 200),
    ('Charlie', '2023-08-03', 300),
    ('David', '2023-08-04', 400),
    ('Eva', '2023-08-05', 500)
]
columns = ['Name', 'Date', 'Amount']
df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

# Step 3: Extract, Transform, and Load (ETL) with Delta Lake
from delta.tables import DeltaTable

# Define the path for the Delta table
delta_table_path = '/tmp/delta_table'

# Write the sample data to the Delta table
df.write.format('delta').mode('overwrite').save(delta_table_path)

# Transform the data (multiply the Amount by 2)
transformed_df = df.withColumn('Amount', col('Amount') * 2)

# Load the transformed data back to the Delta table
transformed_df.write.format('delta').mode('overwrite').save(delta_table_path)

# Verify the data in the Delta table
deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.toDF().show()

# COMMAND ----------

# Step 4: Time Travel with Delta Lake

# Make another transformation to the data (add 50 to the Amount)
new_transformed_df = df.withColumn('Amount', col('Amount') + 50)

# Load the new transformed data back to the Delta table
new_transformed_df.write.format('delta').mode('overwrite').save(delta_table_path)

# Access the previous version (version 0) of the data using Time Travel
version_0_df = spark.read.format('delta').option('versionAsOf', 0).load(delta_table_path)
version_0_df.show()

# Access the latest version of the data
latest_df = spark.read.format('delta').load(delta_table_path)
latest_df.show()

# COMMAND ----------

# Step 5: Query Historical Data

# Register the previous version (version 0) of the data as a temporary view
version_0_df.createOrReplaceTempView('version_0')

# Register the latest version of the data as a temporary view
latest_df.createOrReplaceTempView('latest_version')

# Run SQL queries to analyze the historical data
result_0 = spark.sql('SELECT * FROM version_0 WHERE Amount > 150')
result_0.show()

result_latest = spark.sql('SELECT * FROM latest_version WHERE Amount > 150')
result_latest.show()
