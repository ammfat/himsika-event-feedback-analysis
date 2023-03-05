import google.auth
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

# SETUP CREDENTIALS

scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
service_account_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
CREDENTIALS, PROJECT_ID = google.auth.default(scopes=scopes)

# SETUP DAG

args = {
    'owner': 'ammfat',
}

bq_raw_data_dataset = 'raw_data_experiment'
bq_event_feedback_dataset = 'event_feedback_experiment'
bq_dwh_event_dataset = 'dwh_event_experiment'


with DAG(
        dag_id='bq_dwh_analytics'
        , default_args=args
        , schedule_interval='0 3 * * 1' # every monday at 3am
        , start_date=datetime(2022, 6, 1)
        # , end_date=datetime(2023, 2, 28)
        , description='Load data warehouse tables in BigQuery for analytics'
    ) as dag:

    get_success_signal = FileSensor(
        task_id='get_success_signal'
        , filepath='/signal/{{ ds_nodash }}/_SUCCESS'
        , poke_interval=60
        , timeout=60*30
    )

    task_resume_decision_maker = ShortCircuitOperator(
        task_id='task_resume_decision_maker'
        , python_callable=lambda: False
    )

    get_status = BashOperator(
        task_id='get_status'
        , bash_command='echo "File exists"'
    )

    get_success_signal >> task_resume_decision_maker >> get_status
