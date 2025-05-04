from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LoadToBigQuery") \
    .getOrCreate()

# Read the cleaned static pollution CSV file from GCS
df = spark.read.option("header", True).csv(
    "gs://weather-pollution-data/static_data/pollution_data_stuttgart_cleaned.csv"
)

# Write the DataFrame to BigQuery
df.write \
    .format("bigquery") \
    .option("table", "weather-pollution-pipeline.pollution_data.static_pollution") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

print("✅ Data successfully written to BigQuery.")
