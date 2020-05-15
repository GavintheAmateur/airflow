from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import (CreateTablesOperator, StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'gavin',
    'start_date': datetime(2020, 5, 5),
}

dag = DAG('sparkify_etl_dag_test',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date= default_args['start_date']
          # schedule_interval='0 * * * *',
        )



start_operator = DummyOperator(task_id='Start_execution',  dag=dag)

create_tables = CreateTablesOperator(task_id='create_tables_if_not_exists',  dag=dag,    redshift_conn_id='redshift')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    provide_context=True,
    s3_key='log_data/{execution_date.year}/{execution_date.month:02}/{ds}-events.json',
    region='us-west-2'  # for test, prod region is us-west-2
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    provide_context=True,
    s3_key='song_data',
    region='us-west-2' #for test, prod region is us-west-2
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='user',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='song',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artist',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table  >> run_quality_checks
load_song_dimension_table  >> run_quality_checks
load_artist_dimension_table  >> run_quality_checks
load_time_dimension_table  >> run_quality_checks
run_quality_checks >> end_operator













