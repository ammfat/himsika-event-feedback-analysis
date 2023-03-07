import google.auth
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime
from transformation.sql import Query

# SETUP CREDENTIALS

scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
service_account_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
CREDENTIALS, PROJECT_ID = google.auth.default(scopes=scopes)

# SETUP DAG

args = {
    'owner': 'ammfat',
}

bq_raw_data_dataset = 'raw_data'
bq_event_dataset_name = 'event_feedback'
bq_dwh_dataset_name = 'dwh_event'

query = Query(
    project_id=PROJECT_ID
    , bq_raw_data_dataset_name=bq_raw_data_dataset
    , bq_event_dataset_name=bq_event_dataset_name
    , bq_dwh_dataset_name=bq_dwh_dataset_name
)

with DAG(
        dag_id='bq_dwh_analytics'
        , default_args=args
        , schedule_interval='0 3 * * 1' # every monday at 3am
        , start_date=datetime(2022, 6, 1)
        # , end_date=datetime(2023, 2, 28)
        , description='Create data warehouse tables in BigQuery'
    ) as dag:

    get_success_signal = ExternalTaskSensor(
        task_id='get_success_signal'
        , poke_interval=10
        , retries=2
        # , timeout=60
        # , mode='reschedule'
        , soft_fail=True
        , external_dag_id='gsheet_to_bq'
        , external_task_id='signal_to_dwh_dag'
    )

    create_bq_dim_events_history = BigQueryOperator(
        task_id='create_bq_dim_events_history'
        , sql=query.get_sql_dim_events_history()
        , use_legacy_sql=False
    )

    create_bq_dim_instances_history = BigQueryOperator(
        task_id='create_bq_dim_instances_history'
        , sql=query.get_sql_dim_instances_history()
        , use_legacy_sql=False
    )

    create_bq_dim_degree_programs_history = BigQueryOperator(
        task_id='create_bq_dim_degree_programs_history'
        , sql=query.get_sql_dim_degree_programs_history()
        , use_legacy_sql=False
    )

    create_bq_professions_history = BigQueryOperator(
        task_id='create_bq_professions_history'
        , sql=query.get_sql_dim_professions_history()
        , use_legacy_sql=False
    )

    create_bq_dim_events = BigQueryOperator(
        task_id='create_bq_dim_events'
        , sql=query.get_sql_dim_events()
        , use_legacy_sql=False
    )

    create_bq_dim_instances = BigQueryOperator(
        task_id='create_bq_dim_instances'
        , sql=query.get_sql_dim_instances()
        , use_legacy_sql=False
    )

    create_bq_dim_degree_programs = BigQueryOperator(
        task_id='create_bq_dim_degree_programs'
        , sql=query.get_sql_dim_degree_programs()
        , use_legacy_sql=False
    )

    create_bq_dim_professions = BigQueryOperator(
        task_id='create_bq_dim_professions'
        , sql=query.get_sql_dim_professions()
        , use_legacy_sql=False
    )

    create_fact_rates_by_responses = BigQueryOperator(
        task_id='create_fact_rates_by_responses'
        , sql=query.get_sql_fact_rates_by_responses()
        , use_legacy_sql=False
    )

    create_sql_view_rates_by_responses = BigQueryOperator(
        task_id='create_sql_view_rates_by_responses'
        , sql=query.get_sql_view_rates_by_responses()
        , use_legacy_sql=False
    )

    get_success_signal >> [
        create_bq_dim_events_history
        , create_bq_dim_instances_history
        , create_bq_dim_degree_programs_history
        , create_bq_professions_history
    ]

    create_bq_dim_events_history >> create_bq_dim_events
    create_bq_dim_instances_history >> create_bq_dim_instances
    create_bq_dim_degree_programs_history >> create_bq_dim_degree_programs
    create_bq_professions_history >> create_bq_dim_professions

    [
        create_bq_dim_events
        , create_bq_dim_instances
        , create_bq_dim_degree_programs
        , create_bq_dim_professions
    ] >> create_fact_rates_by_responses >> create_sql_view_rates_by_responses