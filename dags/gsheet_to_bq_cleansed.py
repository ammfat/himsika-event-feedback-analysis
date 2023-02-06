import os
import google.auth

from airflow import DAG
from airflow import PythonOperator
from airflow import datetime, timedelta

# SETUP CREDENTIALS

scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
service_account_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
CREDENTIALS, PROJECT_ID = google.auth.default(scopes=scopes)

# SETUP DAG

args = {
    'owner': 'ammfat',
}

bq_cleased_dataset_name = 'event-feedback-experiment'

# DAG

def _get_dag_details(**kwargs):
    return {
        'project_id': PROJECT_ID
        , 'bq_cleased_dataset_name': bq_cleased_dataset_name
        , 'exec_timestamp': kwargs.get('ts')
    }

def _event_data_sensor(ti, **kwargs):
    sheets = ti.xcom_pull(dag_id='gsheet_to_bq_raw', task_ids='gsheet_sensor', key='sheets')

    if not sheets:
        return "No sheets to process"

    return sheets


with DAG(
        dag_id='gsheet_to_bq_cleansed'
        , default_args=args
        , schedule_interval='0 5 * * *'
        , start_date=datetime.datetime(2022, 6, 1)
        , end_date=datetime.datetime(2022, 11, 1)
        , description='Transform-Load event feedback data from Google Sheet to BigQuery'
    ) as dag:

    get_dag_details = PythonOperator(
        task_id='get_dag_details',
        python_callable=_get_dag_details
    )

    event_data_sensor = PythonOperator(
        task_id='event_data_sensor',
        python_callable=_event_data_sensor,
    )

    print_dag_details >> event_data_sensor

if __name__ == '__main__':
    dag.cli()