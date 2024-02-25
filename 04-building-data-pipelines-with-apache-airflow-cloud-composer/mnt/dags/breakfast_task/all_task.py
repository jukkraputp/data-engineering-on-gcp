from breakfast_task.load_data_to_gcs_then_bigquery import load_data_to_gcs, load_data_from_gcs_to_bigquery

class AllTask:
    def _load_products_to_gcs():
        load_data_to_gcs('products')

    def _load_stores_to_gcs():
        load_data_to_gcs('stores')

    def _load_transactions_to_gcs():
        load_data_to_gcs('transactions')

    def _load_products_from_gcs_to_bigquery():
        load_data_from_gcs_to_bigquery('products')

    def _load_stores_from_gcs_to_bigquery():
        load_data_from_gcs_to_bigquery('stores')

    def _load_transactions_from_gcs_to_bigquery():
        load_data_from_gcs_to_bigquery('transactions')