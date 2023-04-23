from typing import List

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 *,
                 tables: List[str],
                 redshift_conn_id: str = "redshift_default",
                 **kwargs
                 ):

        super().__init__(**kwargs)
        
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        # Test if the tables are empty
        redshift = RedshiftSQLHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

        
