from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class LoadFactOperator(BaseOperator):
    """
    Operator to load fact data into a Redshift table using a SELECT query.

    Args:
        query (str): The SELECT query to load fact data.
        redshift_conn_id (Optional[str], optional): The Redshift connection ID.
            Defaults to "redshift_default".
        autocommit (Optional[bool], optional): Whether to autocommit the query.
            Defaults to False.
    """

    ui_color = "#F98866"

    def __init__(
        self,
        *,
        query: str,
        redshift_conn_id: Optional[str] = "redshift_default",
        autocommit: Optional[bool] = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        """
        Execute the LoadFactOperator.

        Args:
            context (dict): The execution context.
        """
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        self.log.info("Query executing....")
        redshift_hook.run(self.query, autocommit=self.autocommit)

        self.log.info("Query complete...")
