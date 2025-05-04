# src/clean/quick_data_check.py

import pandas as pd

# Step 1: Load the pollution CSV
df = pd.read_csv('data/pollution_data_stuttgart.csv')

# Step 2: Show first 5 rows
print("\nFirst 5 rows of the pollution data:")
print(df.head())

# Step 3: Show column names and data types
print("\nDataframe Info:")
print(df.info())

# Step 4: Check if there are any missing values
print("\nMissing Values (if any):")
print(df.isnull().sum())
