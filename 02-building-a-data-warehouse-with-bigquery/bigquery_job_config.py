from google.cloud import bigquery


class JobConfig:
    def create_table(file_name: str):
        if file_name == 'breakfast_products':
            return bigquery.LoadJobConfig(
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                schema=[
                    bigquery.SchemaField("upc", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "description", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "manufacturer", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "category", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "sub_category", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "product_size", bigquery.SqlTypeNames.STRING),
                ],
            )
        elif file_name == 'breakfast_stores':
            return bigquery.LoadJobConfig(
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                schema=[
                    bigquery.SchemaField(
                        "store_id", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "store_name", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField("address_city_name",
                                         bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField("address_state_prov_code",
                                         bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "msa_code", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "seg_value_name", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField("parking_space_qty",
                                         bigquery.SqlTypeNames.NUMERIC),
                    bigquery.SchemaField("sales_area_size_num",
                                         bigquery.SqlTypeNames.NUMERIC),
                    bigquery.SchemaField("avg_weekly_baskets",
                                         bigquery.SqlTypeNames.NUMERIC),
                ],
            )
        elif file_name == 'breakfast_transactions':
            return bigquery.LoadJobConfig(
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                schema=[
                    bigquery.SchemaField(
                        "week_end_date", bigquery.SqlTypeNames.DATE),
                    bigquery.SchemaField(
                        "store_num", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField("upc", bigquery.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "units", bigquery.SqlTypeNames.NUMERIC),
                    bigquery.SchemaField(
                        "visits", bigquery.SqlTypeNames.NUMERIC),
                    bigquery.SchemaField("hhs", bigquery.SqlTypeNames.NUMERIC),
                    bigquery.SchemaField(
                        "spend", bigquery.SqlTypeNames.FLOAT),
                    bigquery.SchemaField(
                        "price", bigquery.SqlTypeNames.FLOAT),
                    bigquery.SchemaField(
                        "base_price", bigquery.SqlTypeNames.FLOAT),
                    bigquery.SchemaField(
                        "feature", bigquery.SqlTypeNames.BOOLEAN),
                    bigquery.SchemaField(
                        "display", bigquery.SqlTypeNames.BOOLEAN),
                    bigquery.SchemaField(
                        "tpr_only", bigquery.SqlTypeNames.BOOLEAN),
                ],
                time_partitioning=bigquery.TimePartitioning()
            )
        else:
            return None
