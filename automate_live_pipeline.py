import os
import subprocess
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage, bigquery
from google.oauth2 import service_account

# --- CONFIGURATION ---
API_KEY = "dcc5e4a95e415de8a13ddefd6884d19a"
LAT, LON = 48.7823, 9.1770
GCP_PROJECT_ID = "weather-pollution-pipeline"
GCS_BUCKET = "weather-pollution-data"
CREDENTIALS_PATH = "C:/Users/terry/weather-pollution-analysis/.gcp/dbt-service-account.json"

# Time-stamped file naming
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOCAL_RAW_FILENAME = "pollution_latest.csv"
GCS_RAW_BLOB = f"live_data/pollution_{timestamp}.csv"
SPARK_SCRIPT = "spark_jobs/spark_clean_pollution.py"
SPARK_SUBMIT_PATH = r"C:\spark\spark-3.5.5-bin-hadoop3\bin\spark-submit.cmd"
GCS_CLEANED_FILE = f"cleaned_data/part-00000.csv"  # Adjust this if your Spark job outputs differently
GCS_CLEANED_URI = f"gs://{GCS_BUCKET}/{GCS_CLEANED_FILE}"
BIGQUERY_TABLE = "weather-pollution-pipeline.pollution_data.weather_pollution"

# --- FETCH POLLUTION DATA ---
def fetch_pollution_data():
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# --- SAVE TO CSV ---
def save_to_csv(data):
    record = {
        "datetime": datetime.utcfromtimestamp(data['list'][0]['dt']),
        **data['list'][0]['main'],
        **data['list'][0]['components']
    }
    df = pd.DataFrame([record])
    df.to_csv(LOCAL_RAW_FILENAME, index=False)
    print(f"✅ Saved raw CSV: {LOCAL_RAW_FILENAME}")

# --- UPLOAD TO GCS ---
def upload_to_gcs(local_path, bucket_name, destination_blob):
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = storage.Client(credentials=creds, project=GCP_PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    print(f"☁️ Uploaded to GCS: gs://{bucket_name}/{destination_blob}")

# --- RUN SPARK CLEANING JOB ---
def run_spark_cleaning():
    print("⚙️ Running Spark cleaning job...")
    try:
        result = subprocess.run([SPARK_SUBMIT_PATH, SPARK_SCRIPT], check=True, capture_output=True, text=True)
        print("✅ Spark cleaning complete")
    except subprocess.CalledProcessError as e:
        print("❌ Spark job failed:", e.stderr)
        raise RuntimeError("Spark cleaning failed")

# --- LOAD TO BIGQUERY ---
def load_to_bigquery(gcs_uri):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )
    print("📤 Uploading cleaned data to BigQuery...")
    load_job = client.load_table_from_uri(gcs_uri, BIGQUERY_TABLE, job_config=job_config)
    load_job.result()
    print(f"✅ Upload complete: {load_job.output_rows} rows loaded to {BIGQUERY_TABLE}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        data = fetch_pollution_data()
        save_to_csv(data)
        upload_to_gcs(LOCAL_RAW_FILENAME, GCS_BUCKET, GCS_RAW_BLOB)
        run_spark_cleaning()
        load_to_bigquery(GCS_CLEANED_URI)
    except Exception as e:
        print("❌ Pipeline failed:", e)
