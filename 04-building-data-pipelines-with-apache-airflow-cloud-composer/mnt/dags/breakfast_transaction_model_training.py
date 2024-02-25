from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from breakfast_task.raw_sql import SQL_generator

SQL = SQL_generator('linear_reg', 'breakfast', 'avg_weekly_baskets', 'breakfast.obt', 'year < 2011' , 'year >= 2011')

default_args = {
    "owner": "jukkraputp",
    "start_date": days_ago(1),
}
with DAG(
    dag_id="breakfast_transaction_model_training",
    schedule=None,
    tags=["PIM", "breakfast"],
    default_args=default_args
):

    start = EmptyOperator(task_id="start")

    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model",
        sql=SQL.create_replace_model("""
                AVG_WEEKLY_BASKETS as label,
                CATEGORY as category, 
                STORE_NAME as store_name, 
                IFNULL(PARKING_SPACE_QTY, 0) as parking_space_qty, 
                IFNULL(SALES_AREA_SIZE_NUM, 0) as sales_area_size_num
                                     """),
        # destination_dataset_table="gdg_cloud_devfest_bkk_2020.purchase_prediction_model",
        # write_disposition="WRITE_TRUNCATE",
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
    )

    get_model_training_statistics = BigQueryExecuteQueryOperator(
        task_id="get_model_training_statistics",
        sql=SQL.get_model_training_statistics(),
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
    )

    evaluate_model = BigQueryExecuteQueryOperator(
        task_id="evaluate_model",
        sql=SQL.eval_model("""
                AVG_WEEKLY_BASKETS as label,
                CATEGORY as category, 
                STORE_NAME as store_name, 
                IFNULL(PARKING_SPACE_QTY, 0) as parking_space_qty, 
                IFNULL(SALES_AREA_SIZE_NUM, 0) as sales_area_size_num
                           """),
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
    )

    end = EmptyOperator(task_id="end")

    start >> train_model >> [
        get_model_training_statistics, evaluate_model] >> end
