# src/clean/clean_weather_data.py

import pandas as pd
import json
from datetime import datetime

# Step 1: Load weather JSON
with open('data/weather_data.json', 'r') as f:
    weather_data = json.load(f)

# Step 2: Extract important fields
weather_cleaned = {
    "city": weather_data.get("name"),
    "date": datetime.utcfromtimestamp(weather_data.get("dt")).strftime('%Y-%m-%d'),
    "temp": weather_data["main"].get("temp"),
    "feels_like": weather_data["main"].get("feels_like"),
    "temp_min": weather_data["main"].get("temp_min"),
    "temp_max": weather_data["main"].get("temp_max"),
    "pressure": weather_data["main"].get("pressure"),
    "humidity": weather_data["main"].get("humidity"),
    "visibility": weather_data.get("visibility"),
    "wind_speed": weather_data["wind"].get("speed"),
    "wind_deg": weather_data["wind"].get("deg"),
    "weather_main": weather_data["weather"][0].get("main"),
    "weather_description": weather_data["weather"][0].get("description")
}

# Step 3: Save as CSV
weather_df = pd.DataFrame([weather_cleaned])
weather_df.to_csv('data/weather_data_stuttgart_cleaned.csv', index=False)

print("Weather data cleaned and saved as 'weather_data_stuttgart_cleaned.csv' ✅")
