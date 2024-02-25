import json
import os
import configparser

from google.cloud import bigquery, storage
from google.oauth2 import service_account

CONFIG_FILE = "local.conf"
# CONFIG_FILE = "production.conf"

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

DATA_FOLDER = "data"
BUSINESS_DOMAIN = "breakfast"
location = "asia-southeast1"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = parser.get("gcp", "upload_files_to_gcs_service_account")
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

keyfile_upload_bigquery = parser.get("gcp", "bigquery_only_service_account")
service_account_info_upload_bigquery = json.load(open(keyfile_upload_bigquery))
credentials_upload_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_upload_bigquery
)

keyfile_read_bigquery = parser.get(
    "gcp", "read_bigquery_data_only_service_account")
service_account_info_read_bigquery = json.load(open(keyfile_read_bigquery))
credentials_read_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_read_bigquery
)

project_id = "data-engineering-on-gcp-410507"

# Load data from Local to GCS
bucket_name = "jkp-pim-data-engineering"
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

# data = "products"
# data = "stores"
# data = "transactions"
all_data = ["products", "stores", "transactions"]
for data in all_data:
    file_path = f"{DATA_FOLDER}/breakfast_{data}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_upload_bigquery,
        location=location,
    )
    table_id = f"{project_id}.breakfast.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    read_bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_read_bigquery,
        location=location,
    )
    table = read_bigquery_client.get_table(table_id)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
