from typing import List, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block


class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load data from an S3 bucket to a Redshift table.

    Args:
        table (str): The table to load the data into.
        s3_bucket (str): The S3 bucket containing the data.
        s3_key (Optional[str], optional): The S3 key of the data file. Defaults
            to None.
        redshift_conn_id (str, optional): The Redshift connection ID.
            Defaults to "redshift_default".
        aws_conn_id (str, optional): The AWS connection ID.
            Defaults to "aws_default".
        verify (Union[bool, str, None], optional): Whether to verify SSL
            certificates for S3 connection. Defaults to None.
        autocommit (bool, optional): Whether to autocommit the COPY operation.
            Defaults to False.
        column_list (List[str], optional): The list of columns to be loaded, in
            the order they appear in the data file. None to load all columns.
            Defaults to None.
        copy_options (List[str], optional): Additional options for the COPY
            command. Defaults to None.
    """

    ui_color = "#358140"

    template_fields = ("s3_bucket", "s3_key", "copy_options")

    def __init__(
        self,
        *,
        table: str,
        s3_bucket: str,
        s3_key: Optional[str] = None,
        redshift_conn_id: str = "redshift_default",
        aws_conn_id: str = "aws_default",
        verify: Union[bool, str, None] = None,
        autocommit: bool = True,
        column_list: List[str] = None,
        copy_options: List[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.autocommit = autocommit
        self.column_list = column_list
        self.copy_options = copy_options or []

    def _build_copy_query(
        self, copy_destination: str, credentials_block: str, copy_options: str
    ) -> str:
        """
        Build the COPY SQL statement to load data from S3 to Redshift.

        Args:
            copy_destination (str): The schema and table to load the data into.
            credentials_block (str): The credentials block for the COPY
                statement.
            copy_options (str): Additional options for the COPY command.

        Returns:
            str: The COPY SQL statement.
        """
        column_names = (
            "(" + ", ".join(self.column_list) + ")" if self.column_list else ""
        )
        s3_key = self.s3_key if self.s3_key else ""
        return f"""
        COPY {copy_destination} {column_names}
        FROM 's3://{self.s3_bucket}/{s3_key}'
        credentials '{credentials_block}'
        {copy_options};
    """

    def execute(self, context):
        """
        Execute the StageToRedshiftOperator.

        Args:
            context (dict): The execution context.
        """
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = s3_hook.get_credentials()
        credentials_block = build_credentials_block(credentials)

        copy_options = "\n\t\t\t".join(self.copy_options)
        copy_destination = f"{self.table}"
        copy_statement = self._build_copy_query(
            copy_destination, credentials_block, copy_options
        )

        self.log.info("Executing COPY command...")
        redshift_hook.run(copy_statement, autocommit=self.autocommit)

        self.log.info("COPY command complete...")

