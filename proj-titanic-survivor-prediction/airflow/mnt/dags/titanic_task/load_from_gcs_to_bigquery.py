from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.models import Variable
import json


def load_from_gcs_to_bigquery():
    BUSINESS_DOMAIN = "titanic"
    location = "us-central1"
    project_id = Variable.get('project_id')

    service_account_info_upload_bigquery = json.loads(Variable.get('bigquery_only_service_account'))
    credentials_upload_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_upload_bigquery
    )

    destination_blob_name = f"{BUSINESS_DOMAIN}/titanic.csv"
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
    table_id = f"{project_id}.jkp_pim.titanic"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    bucket_name = 'jkp-pim-25022024'
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
