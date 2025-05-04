import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# Constants
API_KEY = os.environ.get("API_KEY", "dcc5e4a95e415de8a13ddefd6884d19a")
LAT = "48.7758"
LON = "9.1829"
BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"
PROJECT_ID = "weather-pollution-pipeline"
DATASET_ID = "pollution_data"
TABLE_ID = "weather_pollution"

def fetch_air_pollution():
    try:
        params = {"lat": LAT, "lon": LON, "appid": API_KEY}
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()["list"][0]
        components = data["components"]
        return {
            "datetime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "aqi": data["main"]["aqi"],
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
        print(f"❌ Fetch error: {e}")
        return None

def clean_data(df):
    df = df.dropna()
    df = df[(df["aqi"] >= 1) & (df["aqi"] <= 5)]
    return df

def append_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print("✅ Data pushed to BigQuery")

# ✅ This MUST be at top-level — this is the Cloud Function entry point!
def push_weather_data(request):
    row = fetch_air_pollution()
    if not row:
        return ("❌ Failed to fetch data", 500)

    df = pd.DataFrame([row])
    df = clean_data(df)
    if df.empty:
        return ("⚠️ No valid data after cleaning", 204)

    append_to_bigquery(df)
    return ("✅ Weather pollution data uploaded", 200)
