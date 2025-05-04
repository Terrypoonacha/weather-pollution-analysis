from google.cloud import bigquery
import os

# Set the path to your ADC (Application Default Credentials) JSON
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\terry\AppData\Roaming\gcloud\application_default_credentials.json"

# Initialize the BigQuery client
client = bigquery.Client(project="weather-pollution-pipeline")

# GCS CSV file path (from your Spark cleaned output)
gcs_uri = "gs://weather-pollution-data/cleaned_data/part-00000-6023f600-8b5a-4de8-9055-580b43b85bcd-c000.csv"  # put full file name here

# BigQuery target: dataset.table
table_id = "weather-pollution-pipeline.pollution_data.weather_pollution"

# Define load job configuration
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

# Load the CSV from GCS to BigQuery
print("🚀 Starting upload...")
load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
load_job.result()  # Wait for completion

print(f"✅ Upload complete: {load_job.output_rows} rows loaded to {table_id}")
    