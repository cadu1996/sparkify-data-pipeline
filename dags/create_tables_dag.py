from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

from helpers import SqlQueries


default_args = {
    "owner": "Carlos Eduardo de Souza",
    "start_date": datetime(2019, 1, 12),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "create_tables_dag",
    default_args=default_args,
    description="Crate tables in Redshift with Airflow",
    schedule_interval=None,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

create_staging_events_table = RedshiftSQLOperator(
    task_id="create_staging_events",
    sql=SqlQueries.staging_events_table_create,
    dag=dag,
)

create_staging_songs_table = RedshiftSQLOperator(
    task_id="create_staging_songs",
    sql=SqlQueries.staging_songs_table_create,
    dag=dag,
)

create_songplays_table = RedshiftSQLOperator(
    task_id="create_songplays_fact_table",
    sql=SqlQueries.songplay_table_create,
    dag=dag,
)

create_user_dimension_table = RedshiftSQLOperator(
    task_id="create_user_dim_table",
    sql=SqlQueries.user_table_create,
    dag=dag,
)

create_song_dimension_table = RedshiftSQLOperator(
    task_id="create_song_dim_table",
    sql=SqlQueries.song_table_create,
    dag=dag,
)

create_artist_dimension_table = RedshiftSQLOperator(
    task_id="create_artist_dim_table",
    sql=SqlQueries.artist_table_create,
    dag=dag,
)

create_time_dimension_table = RedshiftSQLOperator(
    task_id="create_time_dim_table",
    sql=SqlQueries.time_table_create,
    dag=dag,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

(
    start_operator
    >> [create_staging_events_table, create_staging_songs_table]
    >> create_user_dimension_table
    >> create_artist_dimension_table
    >> create_time_dimension_table
    >> create_song_dimension_table
    >> create_songplays_table
    >> end_operator
)

