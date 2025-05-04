import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

# --- CONFIGURATION ---
API_KEY = "dcc5e4a95e415de8a13ddefd6884d19a"
LAT = 48.7823  # Stuttgart latitude
LON = 9.1770   # Stuttgart longitude
GCS_BUCKET = "weather-pollution-data"
GCS_DESTINATION = f"live_data/pollution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
LOCAL_FILENAME = "pollution_latest.csv"
CREDENTIALS_PATH = "C:/Users/terry/weather-pollution-analysis/.gcp/dbt-service-account.json"
GCP_PROJECT_ID = "weather-pollution-pipeline"  # <-- replace this

# --- FETCH POLLUTION DATA ---
def fetch_pollution_data():
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    return response.json()

# --- PROCESS AND SAVE TO CSV ---
def save_to_csv(data):
    main = data['list'][0]['main']
    components = data['list'][0]['components']
    record = {
        "datetime": datetime.utcfromtimestamp(data['list'][0]['dt']),
        **main,
        **components
    }
    df = pd.DataFrame([record])
    df.to_csv(LOCAL_FILENAME, index=False)
    print(f"✅ Saved CSV: {LOCAL_FILENAME}")
    return LOCAL_FILENAME

# --- UPLOAD TO GCS ---
def upload_to_gcs(source_file, bucket_name, destination_blob):
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH
    )
    client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"☁️ Uploaded to GCS: gs://{bucket_name}/{destination_blob}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    data = fetch_pollution_data()
    local_file = save_to_csv(data)
    upload_to_gcs(local_file, GCS_BUCKET, GCS_DESTINATION)
