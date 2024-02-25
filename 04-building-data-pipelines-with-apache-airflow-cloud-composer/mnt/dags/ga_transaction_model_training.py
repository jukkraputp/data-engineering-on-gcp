from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from breakfast_task.raw_sql import SQL_generator

SQL = SQL_generator('logistic_reg', 'bqml', 'my_ga_transaction_model', 'bigquery-public-data.google_analytics_sample.ga_sessions_*', "_table_suffix between '20160801' and '20170631'" , "_table_suffix between '20170701' and '20170801'")

default_args = {
    "owner": "jukkraputp",
    "start_date": days_ago(1),
}
with DAG(
    dag_id="ga_transaction_model_training",
    schedule=None,
    tags=["PIM", "google-analytics"],
    default_args=default_args
):

    start = EmptyOperator(task_id="start")

    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model",
        sql=SQL.create_replace_model("""
                IF(totals.transactions IS NULL, 0, 1) AS label,
                IFNULL(device.operatingSystem, "") AS os,
                device.isMobile AS is_mobile,
                IFNULL(geoNetwork.country, "") AS country,
                IFNULL(totals.pageviews, 0) AS pageviews
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
                IF(totals.transactions IS NULL, 0, 1) AS label,
                IFNULL(device.operatingSystem, "") AS os,
                device.isMobile AS is_mobile,
                IFNULL(geoNetwork.country, "") AS country,
                IFNULL(totals.pageviews, 0) AS pageviews
                           """),
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
    )

    compute_roc = BigQueryExecuteQueryOperator(
        task_id="compute_roc",
        sql=SQL.compute_roc(),
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="bigquery_conn_id",
    )

    end = EmptyOperator(task_id="end")

    start >> train_model >> [
        get_model_training_statistics, evaluate_model, compute_roc] >> end
    # start >> [get_model_training_statistics, evaluate_model, compute_roc] >> end
