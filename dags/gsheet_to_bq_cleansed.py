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
    
with DAG(
        dag_id='gsheet_to_bq_cleansed'
        , default_args=args
        , schedule_interval='0 5 * * *'
        , start_date=datetime.datetime(2021, 1, 1)
        , catchup=False
    ) as dag:

    get_dag_details = PythonOperator(
        task_id='get_dag_details',
        python_callable=_get_dag_details
    )
    
    print_dag_details

if __name__ == '__main__':
    dag.cli()