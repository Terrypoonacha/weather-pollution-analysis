# src/extract/fetch_pollution_pandas.py

import pandas as pd
import glob

# Step 1: Path to all parquet files
parquet_files = glob.glob('data/E1a/*.parquet')

# Step 2: Read and combine all parquet files
dfs = []  # List to collect all small dataframes

for file in parquet_files:
    df = pd.read_parquet(file)
    dfs.append(df)

# Combine all small DataFrames into one big DataFrame
full_df = pd.concat(dfs, ignore_index=True)

# Step 3: Save combined data into CSV
full_df.to_csv('data/pollution_data_stuttgart.csv', index=False)

print("Pollution data converted and saved to CSV ✅")
