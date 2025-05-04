from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("WeatherPollutionJoin") \
    .getOrCreate()

# Step 2: Read both datasets (from local files)
pollution_df = spark.read.option("header", True).csv("data/pollution_data_stuttgart_cleaned.csv")
weather_df = spark.read.option("header", True).csv("data/weather_data_stuttgart_cleaned.csv")

# Step 3: Convert date fields
pollution_df = pollution_df.withColumnRenamed("Start", "date")
pollution_df = pollution_df.withColumn("date", to_date("date", "yyyy-MM-dd"))
weather_df = weather_df.withColumn("date", to_date("date", "yyyy-MM-dd"))

# Step 4: Join on date
joined_df = pollution_df.join(weather_df, on="date", how="inner")

# Step 5: Save output as CSV
joined_df.coalesce(1).write.option("header", True).mode("overwrite").csv("data/joined_data")

print("✅ Spark join complete. Output written to: data/joined_data/")
spark.stop()
