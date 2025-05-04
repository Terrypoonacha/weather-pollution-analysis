from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Read from GCS
input_path = sys.argv[1]  # e.g., gs://weather-pollution-data/live_data/*.csv
output_path = sys.argv[2]  # e.g., gs://weather-pollution-data/cleaned_data/

spark = SparkSession.builder.appName("CleanPollutionData").getOrCreate()

# Load data
df = spark.read.option("header", True).csv(input_path)

# Basic cleaning: drop rows with nulls in key columns
df_cleaned = df.dropna(subset=["pm2_5", "pm10", "o3", "no2", "so2", "co"])

# Optional: Convert to correct data types
columns_to_float = ["pm2_5", "pm10", "o3", "no2", "so2", "co"]
for col_name in columns_to_float:
    df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast("float"))

# Save cleaned data
df_cleaned.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

print("✅ Cleaning job complete.")
spark.stop()
