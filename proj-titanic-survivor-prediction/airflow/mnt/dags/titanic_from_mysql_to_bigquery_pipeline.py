from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone
from airflow.utils.dates import days_ago

import pandas as pd
import os

from titanic_task.all import Task


def _extract_from_mysql():
    hook = MySqlHook(mysql_conn_id="pim_titanic_mysql_conn")
    conn = hook.get_conn()

    df = pd.read_sql("SELECT * FROM titanic", con=conn)
    print(df.head())
    print(os.path.abspath('data/titanic.csv'))
    df.to_csv("/opt/airflow/dags/data/titanic_dump.csv", index=False)

default_args = {
    "owner": "jukkraputp",
    "start_date": days_ago(1),
}
with DAG(
    dag_id="titanic_from_mysql_to_bigquery_pipeline",
    schedule=None,
    tags=["titanic", "mysql", "bigquery"],
    default_args=default_args
):

    start = EmptyOperator(task_id="start")

    get_data_from_mysql = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=_extract_from_mysql
    )

    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=Task._load_to_gcs
    )

    load_from_gcs_to_bigquery = PythonOperator(
        task_id="load_from_gcs_to_bigquery",
        python_callable=Task._load_from_gcs_to_bigquery
    )

    end = EmptyOperator(task_id="end")

    start >> get_data_from_mysql >> load_to_gcs >> load_from_gcs_to_bigquery >> end
