from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("LoadToBigQuery") \
    .getOrCreate()

# Read the cleaned CSV from GCS
df = spark.read.option("header", True).csv("gs://weather-pollution-data/static_data/pollution_data_stuttgart_cleaned.csv")

# Write to BigQuery
df.write \
    .format("bigquery") \
    .option("table", "weather-pollution-pipeline.pollution_data.static_pollution") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

print("✅ Data written to BigQuery successfully.")
