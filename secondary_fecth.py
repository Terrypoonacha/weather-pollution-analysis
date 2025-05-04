import requests
import os
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load API key from .env
load_dotenv()
API_KEY = os.getenv("API_KEY") or "dcc5e4a95e415de8a13ddefd6884d19a"
LAT = "48.7758"  # Stuttgart latitude
LON = "9.1829"   # Stuttgart longitude
BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

data_rows = []

def fetch_air_pollution():
    params = {
        "lat": LAT,
        "lon": LON,
        "appid": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        pollution_data = response.json()
        components = pollution_data["list"][0]["components"]
        return {
            "datetime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "aqi": pollution_data["list"][0]["main"]["aqi"],
            "co": components.get("co"),
            "no": components.get("no"),
            "no2": components.get("no2"),
            "o3": components.get("o3"),
            "so2": components.get("so2"),
            "pm2_5": components.get("pm2_5"),
            "pm10": components.get("pm10"),
            "nh3": components.get("nh3")
        }
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

# Fetch 15 rows with 10-second intervals
for i in range(15):
    print(f"⏳ Fetching entry {i+1}/15...")
    result = fetch_air_pollution()
    if result:
        data_rows.append(result)
        print(f"✅ Fetched at {result['datetime']}")
    else:
        print("⚠️ Skipping this entry due to error.")
    time.sleep(10)  # Reduced delay

# Save results
if data_rows:
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(data_rows)
    df.to_csv("data/real_time_pollution_data.csv", index=False)
    print("✅ Data saved to data/real_time_pollution_data.csv")
else:
    print("❌ No data collected.")
