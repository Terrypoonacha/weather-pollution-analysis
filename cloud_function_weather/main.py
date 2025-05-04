import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import logging

# Enable structured logging
logging.basicConfig(level=logging.INFO)

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
        result = {
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
        logging.info(f"✅ API fetch successful at {result['datetime']}")
        return result
    except Exception as e:
        logging.error(f"❌ Fetch error: {e}", exc_info=True)
        return None

def clean_data(df):
    try:
        cleaned = df.dropna()
        cleaned = cleaned[(cleaned["aqi"] >= 1) & (cleaned["aqi"] <= 5)]
        logging.info(f"🧹 Cleaned data — {len(cleaned)} rows retained")
        return cleaned
    except Exception as e:
        logging.error(f"❌ Error cleaning data: {e}", exc_info=True)
        return pd.DataFrame()

def append_to_bigquery(df):
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info("✅ Data pushed to BigQuery")
    except Exception as e:
        logging.error(f"❌ Failed to append to BigQuery: {e}", exc_info=True)

# ✅ Cloud Function entry point
def push_weather_data(request):
    logging.info("🚀 Cloud Function triggered")

    row = fetch_air_pollution()
    if not row:
        return ("❌ Failed to fetch data", 500)

    df = pd.DataFrame([row])
    df = clean_data(df)
    if df.empty:
        logging.warning("⚠️ No valid data after cleaning")
        return ("⚠️ No valid data after cleaning", 204)

    append_to_bigquery(df)
    return ("✅ Weather pollution data uploaded", 200)
