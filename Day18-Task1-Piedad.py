# Databricks notebook source

# Define the data
data = [("Alice", 28, "Engineer"),
        ("Bob", 35, "Data Scientist"),
        ("Charlie", 40, "Manager")]

# Define the schema
schema = ["Name", "Age", "Occupation"]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Define the output CSV file path
output_path = "/dbfs/tmp/data/sample_data001.csv"

# Write the DataFrame to a CSV file
df.write.option("header", "true").csv(output_path)

# COMMAND ----------

df.show()

