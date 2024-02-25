from google.cloud import storage
from google.oauth2 import service_account
from airflow.models import Variable
import json


def load_to_gcs():
    BUSINESS_DOMAIN = "titanic"
    location = "us-central1"
    project_id = Variable.get('project_id')

    # Prepare and Load Credentials to Connect to GCP Services
    service_account_info_gcs = json.loads(Variable.get('upload_file_to_gcs_service_account'))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs,
        
    )

    # Load data from Local to GCS
    bucket_name = "jkp-pim-25022024"
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = "/opt/airflow/dags/data/titanic_dump.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/titanic.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
