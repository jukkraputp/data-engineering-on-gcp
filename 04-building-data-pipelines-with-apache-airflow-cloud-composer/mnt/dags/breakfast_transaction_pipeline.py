from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from breakfast_task.all_task import AllTask


with DAG(
    dag_id="breakfast_transaction_pipeline",
    schedule="0 0 * * *",
    start_date=timezone.datetime(2024, 1, 28),
    tags=["breakfast"],
    catchup=False
):

    start = EmptyOperator(task_id="start")

    load_transactions_to_gcs = PythonOperator(
        task_id="load_transactions_to_gcs",
        python_callable=AllTask._load_transactions_to_gcs
    )

    load_transactions_from_gcs_to_bigquery = PythonOperator(
        task_id="load_transactions_from_gcs_to_bigquery",
        python_callable=AllTask._load_transactions_from_gcs_to_bigquery
    )

    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_view",
        dataset_id="breakfast",
        table_id="transformed_transactions",
        gcp_conn_id="my_gcp_conn_id",
        view={
            "query": """
                select
                    extract(day from WEEK_END_DATE) as day
                    , extract(month from WEEK_END_DATE) as month
                    , extract(year from WEEK_END_DATE) as year
                    , WEEK_END_DATE as week_end_date
                    , STORE_NUM as store_num
                    , UPC as upc
                    , UNITS as units
                    , VISITS as visits
                    , BASE_PRICE as base_price
                from
                    `breakfast.transactions`
            """,
            "useLegacySql": False,
        },
    )

    end = EmptyOperator(task_id="end")

    start >> load_transactions_to_gcs >> load_transactions_from_gcs_to_bigquery >> create_view >> end
