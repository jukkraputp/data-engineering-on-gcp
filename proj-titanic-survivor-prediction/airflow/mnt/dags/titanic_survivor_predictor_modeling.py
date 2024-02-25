from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

dataset_name = Variable.get('titanic_dataset')
model_name = 'survivor_predictor'
datasource = f'{dataset_name}.titanic'

default_args = {
    "owner": "jukkraputp",
    "start_date": days_ago(1),
}
with DAG(
    dag_id="titanic_survivor_predictor_modeling",
    schedule='@daily',
    tags=["titanic", "mysql", "bigquery"],
    default_args=default_args
):

    start = EmptyOperator(task_id="start")

    # PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model",
        sql=f"""
            CREATE OR REPLACE MODEL `{dataset_name}.{model_name}_{{{{ ds }}}}`
            OPTIONS(model_type='logistic_reg') AS
            SELECT
            Sex,
            Fare,
            Age,
            Pclass,
            Survived AS label
            FROM `{dataset_name}.titanic`
        """,
        # destination_dataset_table="gdg_cloud_devfest_bkk_2020.purchase_prediction_model",
        # write_disposition="WRITE_TRUNCATE",
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="gcp_conn_id",
    )

    get_model_training_statistics = BigQueryExecuteQueryOperator(
        task_id="get_model_training_statistics",
        sql=f"""
            CREATE OR REPLACE TABLE
                `{dataset_name}.{model_name}_training_statistics_{{{{ ds }}}}` AS
            SELECT
                *
            FROM
                ML.TRAINING_INFO(MODEL `{dataset_name}.{model_name}_{{{{ ds }}}}`)
            ORDER BY
                iteration DESC
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="gcp_conn_id",
    )

    evaluate_model = BigQueryExecuteQueryOperator(
        task_id="evaluate_model",
        sql=f"""
            CREATE OR REPLACE TABLE
                `{dataset_name}.{model_name}_evaluation_{{{{ ds }}}}` AS
            SELECT
                *
            FROM ML.EVALUATE(MODEL `{dataset_name}.{model_name}_{{{{ ds }}}}`, (
                SELECT
                    Sex,
                    Fare,
                    Age,
                    Pclass,
                    Survived AS label
                FROM
                    `{datasource}`))
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="gcp_conn_id",
    )

    compute_roc = BigQueryExecuteQueryOperator(
        task_id="compute_roc",
        sql=f"""
            CREATE OR REPLACE TABLE
                `{dataset_name}.{model_name}_roc_{{{{ ds }}}}` AS
            SELECT
                *
            FROM
                ML.ROC_CURVE(MODEL `{dataset_name}.{model_name}_{{{{ ds }}}}`)
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="gcp_conn_id",
    )

    end = EmptyOperator(task_id="end")

    start >> train_model >> [
        get_model_training_statistics, evaluate_model, compute_roc] >> end
    # start >> [get_model_training_statistics, evaluate_model, compute_roc] >> end
