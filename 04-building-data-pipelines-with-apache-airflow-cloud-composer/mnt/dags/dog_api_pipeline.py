from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
import json
import requests


def _get_dog_image_url():
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()

    # Your code here
    with open(f'''/opt/airflow/dags/dogs.json''', 'w') as fp:
        json.dump(data, fp)


with DAG(
    dag_id="dog_api_pipeline",
    schedule="*/30 * * * *",
    start_date=timezone.datetime(2024, 1, 28),
    catchup=False,
):

    start = EmptyOperator(task_id="start")

    get_dog_image_url = PythonOperator(
        task_id="get_dog_image_url",
        python_callable=_get_dog_image_url
    )

    load_to_jsonbin = BashOperator(
        task_id="load_to_jsonbin",
        bash_command='''
API_KEY={{ var.value.JSONBIN_API_KEY }}
COLLECTION_ID={{ var.value.JSONBIN_DOGS_COLLECTION_ID }}

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @/opt/airflow/dags/dogs.json \
    "https://api.jsonbin.io/v3/b"
'''
    )

    fetch_jsonbin = BashOperator(
        task_id="fetch_jsonbin",
        bash_command='''
    API_KEY={{ var.value.JSONBIN_API_KEY }}
COLLECTION_ID={{ var.value.JSONBIN_DOGS_COLLECTION_ID }}

curl -XGET \
    -H "X-Master-key: $API_KEY" \
    "https://api.jsonbin.io/v3/c/$COLLECTION_ID/bins"
'''
    )

    end = EmptyOperator(task_id="end")

    start >> get_dog_image_url >> load_to_jsonbin >> fetch_jsonbin >> end
