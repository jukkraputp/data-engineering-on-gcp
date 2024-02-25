from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def print_world():
    print('world')


with DAG(
    dag_id="my_second_dag",
    schedule="*/10 * * * *",
    start_date=timezone.datetime(2024, 1, 27),
    catchup=False,
):

    start = EmptyOperator(task_id="start")

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello'",
    )

    world = PythonOperator(
        task_id="world",
        python_callable=print_world,
    )

    end = EmptyOperator(task_id="end")

    # start >> hello >> end
    # start >> world >> end

    start >> [hello, world] >> end
