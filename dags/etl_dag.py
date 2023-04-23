from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

default_args = {
    "owner": "Carlos Eduardo de Souza",
    "start_date": datetime(2018, 11, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": True,
}

dag = DAG(
    "etl_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 0 * * *",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}-events.json",
    copy_options=[
        "REGION 'us-west-2'",
        "FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
        "TIMEFORMAT AS 'epochmillisecs'",
    ],
    dag=dag,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    copy_options=[
        "REGION 'us-west-2'",
        "FORMAT AS JSON 'auto'",
    ],
    dag=dag,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    table="songplays",
    query_transformation=SqlQueries.songplay_table_insert,
    dag=dag,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    table="users",
    query_transformation=SqlQueries.user_table_insert,
    truncate_table=True,
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    table="songs",
    query_transformation=SqlQueries.song_table_insert,
    truncate_table=True,
    dag=dag,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    table="artists",
    query_transformation=SqlQueries.artist_table_insert,
    truncate_table=True,
    dag=dag,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    table="time",
    query_transformation=SqlQueries.time_table_insert,
    truncate_table=True,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        tables=["songplays", "users", "songs", "artists", "time"],
        dag=dag
        )

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

(
    start_operator
    >> [stage_events_to_redshift, stage_songs_to_redshift]
    >> load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    >> run_quality_checks
    >> end_operator
)
