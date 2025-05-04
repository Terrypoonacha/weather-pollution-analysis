import pandas as pd

# Load raw data
df = pd.read_csv("C:/Users/terry/weather-pollution-analysis/data/pollution_data_stuttgart.csv")

# Clean the data
df_cleaned = (
    df.dropna(how="all")
      .drop(columns=["DataCapture", "FkObservationLog"], errors="ignore")
      .drop_duplicates()
)

# Save cleaned file (for upload to GCS)
df_cleaned.to_csv("pollution_data_stuttgart_cleaned.csv", index=False)

print("✅ Static pollution data cleaned and saved.")
