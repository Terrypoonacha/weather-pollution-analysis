import functions_framework
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

API_KEY = "dcc5e4a95e415de8a13ddefd6884d19a"
LAT = 48.7823
LON = 9.1770
GCS_BUCKET = "weather-pollution-data"

@functions_framework.http
def fetch_and_upload(request):
    now = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"pollution_{now}.csv"
    blob_path = f"live_data/{filename}"
    
    # Fetch pollution data
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Process
    record = {
        "datetime": datetime.utcfromtimestamp(data['list'][0]['dt']),
        **data['list'][0]['main'],
        **data['list'][0]['components']
    }
    df = pd.DataFrame([record])

    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")

    return f"✅ Uploaded: gs://{GCS_BUCKET}/{blob_path}"
