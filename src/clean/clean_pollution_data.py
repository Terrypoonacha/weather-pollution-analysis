# src/clean/clean_pollution_data.py

import pandas as pd

# Step 1: Load pollution data
df = pd.read_csv('data/pollution_data_stuttgart.csv')

# Step 2: Drop completely empty column 'DataCapture'
df = df.drop(columns=['DataCapture'])

# Step 3: Save the cleaned pollution data
df.to_csv('data/pollution_data_stuttgart_cleaned.csv', index=False)

print("Pollution data cleaned and saved as 'pollution_data_stuttgart_cleaned.csv' ✅")
