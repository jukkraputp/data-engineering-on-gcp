from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from breakfast_task.all_task import AllTask


with DAG(
    dag_id="breakfast_store_pipeline",
    schedule="0 0 * * *",
    start_date=timezone.datetime(2024, 1, 28),
    tags=["breakfast"],
    catchup=False
):

    start = EmptyOperator(task_id="start")

    load_stores_to_gcs = PythonOperator(
        task_id="load_stores_to_gcs",
        python_callable=AllTask._load_stores_to_gcs
    )

    load_stores_from_gcs_to_bigquery = PythonOperator(
        task_id="load_stores_from_gcs_to_bigquery",
        python_callable=AllTask._load_stores_from_gcs_to_bigquery
    )

    end = EmptyOperator(task_id="end")

    start >> load_stores_to_gcs >> load_stores_from_gcs_to_bigquery >> end
