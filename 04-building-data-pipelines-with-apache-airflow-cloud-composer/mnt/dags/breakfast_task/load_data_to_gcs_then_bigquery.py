import json
import os
import configparser

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow.models import Variable

import numpy as np
import pandas as pd
import datetime

DAGS_FOLDER = Variable.get("DAGS_FOLDER")
# DAGS_FOLDER = f"{{{{ var.value.get('DAGS_FOLDER') }}}}"

BREAKFAST_TASK_FOLDER = f"{DAGS_FOLDER}/breakfast_task/"

CONFIG_FILE = f"{BREAKFAST_TASK_FOLDER}local.conf"
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


def product_size_str_to_float(str_val):
    float_val = 0
    if 'OZ' in str_val:
        float_val = float(str_val.split(' ')[0]) * 29.5735
    elif 'LT' in str_val:
        float_val = float(str_val.split(' ')[0]) * 1000
    elif 'CT' in str_val:
        float_val = float(str_val.split(' ')[0]) * 10.37
    else:
        float_val = float(str_val.split(' ')[0])

    return float_val


def load_data_to_gcs(data):
    file_path = f"{BREAKFAST_TASK_FOLDER}{DATA_FOLDER}/breakfast_{data}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def load_data_from_gcs_to_bigquery(data):
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    # file_path = f"gs://{destination_blob_name}"
    # if (data == 'products'):
    #     df = pd.read_csv(file_path, header=0, index_col=0)
    #     df['PRODUCT_SIZE'] = df['PRODUCT_SIZE'].apply(
    #         product_size_str_to_float)
    # elif (data == 'stores'):
    #     df = pd.read_csv(file_path, header=0, index_col=0)
    # elif (data == 'transactions'):
    #     df = pd.read_csv(file_path, header=0, index_col=[
    #                      1, 2], skipinitialspace=True)
    #     df['WEEK_END_DATE'] = df['WEEK_END_DATE'].astype(str).apply(
    #         lambda x: datetime.datetime.strptime(x, '%d-%b-%y').date())
    # blob.upload_from_string(df.to_csv(), 'text/csv')

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
    # job = bigquery_client.load_table_from_dataframe(
    #     df,
    #     table_id,
    #     job_config=job_config,
    #     location=location,
    # )
    job.result()


def _read_data_from_bigquery():
    pass
    # read_bigquery_client = bigquery.Client(
    #     project=project_id,
    #     credentials=credentials_read_bigquery,
    #     location=location,
    # )
    # table = read_bigquery_client.get_table(table_id)
    # print(
    #     f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
