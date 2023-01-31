import os
import google.auth
import json

from airflow import DAG, macros
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from googleapiclient.discovery import build

# SETUP CREDENTIALS

scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
service_account_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
CREDENTIALS, PROJECT_ID = google.auth.default(scopes=scopes)
gdrive_service = build('drive', 'v3', credentials=CREDENTIALS)

# SETUP DAG

args = {
    'owner': 'ammfat',
}

bq_raw_data_dataset = 'event_feedback'
EXEC_DATE = '{{ ds }}'
EXEC_DATE_WEEK_AGO = '{{ macros.ds_add(ds, -7) }}'

# DAG

def get_dag_details(**kwargs):
    return {
        'project_id': PROJECT_ID
        , 'bq_raw_data_dataset': bq_raw_data_dataset
        , 'execution_date': kwargs.get('ds')
    }

def gsheet_sensor(start_date, end_date):
    print(f'gsheet_sensor: start_date={start_date}, end_date={end_date}')
    print(f'gsheet_sensor: start_date={type(start_date)}, end_date={type(end_date)}')

    tmp_sheets = []
    pageToken = ""

    while pageToken is not None:
        response = gdrive_service.files().list(
            q=f"""
                mimeType='application/vnd.google-apps.spreadsheet'
                and not name contains 'output'
                and createdTime >= '{ start_date }'
                and createdTime <= '{ end_date }'
            """
            , fields="nextPageToken, files(id, name, owners, createdTime, modifiedTime)"
        ).execute()

        tmp_sheets.extend(response.get('files', []))

        pageToken = response.get('nextPageToken')

    sheets = [sheet for sheet in tmp_sheets if not sheet['owners'][0]['me']]

    if sheets:
        for sheet in sheets:
            print(sheet['id'].ljust(45), sheet['createdTime'].ljust(25), sheet['name'])
    else:
        print('No sheets found.')

    return True

def gsheet_to_bq_raw_data():
    return True


with DAG(
        dag_id='gsheet_to_bq_raw'
        , default_args=args
        , schedule_interval='0 3 * * 1'
        , start_date=datetime(2022, 7, 1)
        , end_date=datetime(2022, 8, 1)
        , concurrency=2
        , max_active_runs=2
    ) as dag:

    print_dag_details = PythonOperator(
        task_id='print_dag_details'
        , python_callable=get_dag_details
    )

    gsheet_sensor = PythonOperator(
        task_id='gsheet_sensor'
        , python_callable=gsheet_sensor
        , op_kwargs={
            'start_date': EXEC_DATE_WEEK_AGO
            , 'end_date': EXEC_DATE
        }
    )

    gsheet_to_bq_raw_data = PythonOperator(
        task_id='gsheet_to_bq_raw_data'
        , python_callable=gsheet_to_bq_raw_data
    )

    print_dag_details >> gsheet_sensor >> gsheet_to_bq_raw_data

if __name__ == '__main__':
    dag.cli()