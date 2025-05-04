import os
import sys
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Set credentials path (Windows path with double backslashes)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\terry\\AppData\\Roaming\\gcloud\\application_default_credentials.json"

# Config
PROJECT_ID = "weather-pollution-pipeline"
DATASET_ID = "pollution_data"
TABLE_ID = "static_pollution"
GCS_URI = "gs://weather-pollution-data/static_data/pollution_data_stuttgart_cleaned.csv"

def load_csv_to_bigquery():
    try:
        print("🔁 Initializing BigQuery client...")
        client = bigquery.Client(project=PROJECT_ID)

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )

        print(f"🚀 Starting load job for {GCS_URI} → {table_ref}")
        load_job = client.load_table_from_uri(
            GCS_URI,
            table_ref,
            job_config=job_config,
        )

        load_job.result()  # Waits for job to finish
        print(f"✅ Loaded {load_job.output_rows} rows into {table_ref}")

    except GoogleAPIError as api_err:
        print(f"❌ Google API error occurred: {api_err}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    load_csv_to_bigquery()
