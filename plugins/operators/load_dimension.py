from typing import Optional
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class LoadDimensionOperator(BaseOperator):
    """
    Operator to load dimension data into a Redshift table using a SELECT query.

    Args:
        table (str): The table name.
        query_transformation (str): The SELECT query to load dimension data.
        redshift_conn_id (Optional[str], optional): The Redshift connection ID.
            Defaults to "redshift_default".
        autocommit (Optional[bool], optional): Whether to autocommit the query.
            Defaults to False.
        truncate_table (Optional[bool], optional): Whether to truncate the table
            before loading data. Defaults to False.
    """

    ui_color = '#80BD9E'

    def __init__(
        self,
        *,
        table: str,
        query_transformation: str,
        redshift_conn_id: Optional[str] = "redshift_default",
        autocommit: Optional[bool] = False,
        truncate_table: Optional[bool] = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.table = table
        self.query = query_transformation
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit
        self.truncate_table = truncate_table

    def execute(self, context):
        """
        Execute the LoadDimensionOperator.

        Args:
            context (dict): The execution context.
        """
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            truncate_statement = f"TRUNCATE TABLE {self.table};"
            self.log.info("Truncating table...")
            redshift_hook.run(truncate_statement, autocommit=self.autocommit)

        insert_statement = f"INSERT INTO {self.table} \n"

        self.log.info("Query executing....")
        redshift_hook.run(
            insert_statement + self.query,
            autocommit=self.autocommit
        )

        self.log.info("Query complete...")

