class SQL_generator:
    def __init__(self, SQL_model_type, SQL_dataset_name, SQL_model_name: str, SQL_data_source, SQL_train_condition, SQL_eval_condition) -> None:
        self.model_type = SQL_model_type
        self.dataset_name = SQL_dataset_name
        self.model_name = SQL_model_name
        self.data_source = SQL_data_source
        self.train_condition = SQL_train_condition
        self.eval_condition = SQL_eval_condition 

    def create_replace_model(self, select: str):
        return f"""
            CREATE OR REPLACE MODEL `{self.dataset_name}.{self.model_name}_{{{{ ds }}}}`
            OPTIONS(model_type='{self.model_type}') AS
            SELECT
                {select}
            FROM
                `{self.data_source}`
            WHERE
                {self.train_condition}
        """

    def get_model_training_statistics(self):
        return f"""
            CREATE OR REPLACE TABLE
                `{self.dataset_name}.{self.model_name}_training_statistics_{{{{ ds }}}}` AS
            SELECT
                *
            FROM
                ML.TRAINING_INFO(MODEL `{self.dataset_name}.{self.model_name}_{{{{ ds }}}}`)
            ORDER BY
                iteration DESC
        """

    def eval_model(self, select):
        return f"""
            CREATE OR REPLACE TABLE
                `{self.dataset_name}.{self.model_name}_evaluation_{{{{ ds }}}}` AS
            SELECT
                *
            FROM ML.EVALUATE(MODEL `{self.dataset_name}.{self.model_name}_{{{{ ds }}}}`, (
                SELECT
                    {select}
                FROM
                    `{self.data_source}`
                WHERE
                    {self.eval_condition}))
        """

    def compute_roc(self):
        return f"""
            CREATE OR REPLACE TABLE
                `{self.dataset_name}.{self.model_name}_roc_{{{{ ds }}}}` AS
            SELECT
                *
            FROM
                ML.ROC_CURVE(MODEL `{self.dataset_name}.{self.model_name}_{{{{ ds }}}}`)
        """
