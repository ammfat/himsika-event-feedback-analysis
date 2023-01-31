import os
import google.auth
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# SETUP CREDENTIALS

scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
service_account_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
CREDENTIALS, PROJECT_ID = google.auth.default(scopes=scopes)

# SETUP DAG

args = {
    'owner': 'ammfat',
}

bq_raw_data_dataset = 'event_feedback'

# DAG

def _get_dag_details(**kwargs):
    return {
        'project_id': PROJECT_ID
        , 'bq_raw_data_dataset': bq_raw_data_dataset
        , 'exec_timestamp': kwargs.get('ts')
    }

def _gsheet_sensor(**kwargs):
    from googleapiclient.discovery import build

    gdrive_service = build('drive', 'v3', credentials=CREDENTIALS)

    search_start_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z') - timedelta(days=7)
    search_end_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z')
    search_start_date = search_start_date.strftime('%Y-%m-%dT%H:%M:%S')
    search_end_date = search_end_date.strftime('%Y-%m-%dT%H:%M:%S')

    print(f"gsheet_sensor: { search_start_date } - { search_end_date }")

    tmp_sheets = []
    pageToken = ""

    while pageToken is not None:
        response = gdrive_service.files().list(
            q=f"""
                mimeType='application/vnd.google-apps.spreadsheet'
                and not name contains 'output'
                and createdTime >= '{ search_start_date }'
                and createdTime <= '{ search_end_date }'
            """
            , fields="nextPageToken, files(id, name, owners, createdTime, modifiedTime)"
        ).execute()

        tmp_sheets.extend(response.get('files', []))

        pageToken = response.get('nextPageToken')

    sheets = [sheet for sheet in tmp_sheets if not sheet['owners'][0]['me']]

    if sheets:
        for sheet in sheets:
            print(sheet['id'].ljust(45), sheet['createdTime'].ljust(25), sheet['name'])
        
        return True
    else:
        print('No sheets found.')
        return False

    # return True

def _gsheet_to_bq_raw_data():
    return True


with DAG(
        dag_id='gsheet_to_bq_raw'
        , default_args=args
        , schedule_interval='0 3 * * 1'
        , start_date=datetime(2022, 6, 1)
        , end_date=datetime(2022, 11, 1)
    ) as dag:

    get_dag_details= PythonOperator(
        task_id='get_dag_details'
        , python_callable=_get_dag_details
    )

    gsheet_sensor = PythonOperator(
        task_id='gsheet_sensor'
        , python_callable=_gsheet_sensor
    )

    gsheet_to_bq_raw_data = PythonOperator(
        task_id='gsheet_to_bq_raw_data'
        , python_callable=_gsheet_to_bq_raw_data
    )

    print_dag_details >> gsheet_sensor >> gsheet_to_bq_raw_data

if __name__ == '__main__':
    dag.cli()