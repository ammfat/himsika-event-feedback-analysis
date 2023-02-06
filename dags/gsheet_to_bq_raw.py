import os
import google.auth

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

bq_raw_data_dataset = 'raw_data_experiment'

# DAG

def _get_dag_details(**kwargs):
    return {
        'project_id': PROJECT_ID
        , 'bq_raw_data_dataset': bq_raw_data_dataset
        , 'exec_timestamp': kwargs.get('ts')
    }

def _gsheet_sensor(ti, **kwargs):
    from googleapiclient.discovery import build

    gdrive_service = build('drive', 'v3', credentials=CREDENTIALS)

    search_start_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z') - timedelta(days=7)
    search_end_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z')
    search_start_date = search_start_date.strftime('%Y-%m-%dT%H:%M:%S')
    search_end_date = search_end_date.strftime('%Y-%m-%dT%H:%M:%S')

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
    
    ti.xcom_push(key='date', value=[search_start_date, search_end_date])
    ti.xcom_push(key='sheets', value=sheets)

def _gsheet_to_pandas(ti, **kwargs):
    date = ti.xcom_pull(key='date', task_ids='gsheet_sensor')
    sheets = ti.xcom_pull(key='sheets', task_ids='gsheet_sensor')

    print('Date Range: ', date[0], ' - ', date[1])
    for sheet in sheets:
        print(sheet['id'].ljust(45), sheet['createdTime'].ljust(25), sheet['name'])

    """ If len sheets = 0, then mark tasks as success 
    else, continue to load data to BQ """
    if len(sheets) == 0:
        return 'No sheets found.'

    import json
    import gspread
    import pandas as pd

    events = [
        sheet['name'].replace('Feedback ', '')
                .replace('(Jawaban)', '')
                .replace('(Responses)', '')
                .strip()
        for sheet in sheets
    ]

    dfs, df_json_objects = [], []

    for sheet in sheets:
        gsheet = gspread.Client(auth=CREDENTIALS).open_by_key(sheet['id'])
        worksheet = gsheet.sheet1

        df = pd.DataFrame(worksheet.get_all_records()).reset_index(drop=True)

        df['Load Date'] = date[1]
        df['Timestamp'] = df['Timestamp'].astype('datetime64').astype('str').str.replace(' ', 'T')
        df.columns = df.columns.str.replace('[^A-Za-z0-9\s]+', '').str.lower().str.replace(' ', '_')

        try:
            df = df.drop(columns='')
        except:
            pass

        df_json_data = df.to_json(orient='records')
        df_json_object = json.loads(df_json_data)

        dfs.append(df)
        df_json_objects.append(df_json_object)

    ti.xcom_push(key='data', value=[df_json_objects, events])

def _pandas_to_bq_raw_data(ti, **kwargs):
    date = ti.xcom_pull(key='date', task_ids='gsheet_sensor')
    sheets = ti.xcom_pull(key='sheets', task_ids='gsheet_sensor')

    print('Date Range: ', date[0], ' - ', date[1])
    
    if len(sheets) == 0:
        return 'No sheets found.'

    from google.cloud import bigquery

    df_json_objects, events = ti.xcom_pull(key='data', task_ids='gsheet_to_pandas')
    bq_client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

    with bq_client:
        from time import sleep
        
        for df_json_object, event in zip(df_json_objects, events):
            table_name = event.lower()\
                        .replace(' -', '')\
                        .replace('\'', '')\
                        .replace('/', '_')\
                        .replace('(', '')\
                        .replace(')', '')\
                        .replace(' ', '_')

            table_id = f'{bq_raw_data_dataset}.{table_name}'

            job_config = bigquery.LoadJobConfig(
                autodetect=True
                , source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                , write_disposition="WRITE_TRUNCATE"
            )

            job = bq_client.load_table_from_json(df_json_object, table_id, job_config=job_config)

            try:
                job.result()
            except Exception as e:
                print(e)

                sleep(120)
                job.result()                

            print('Table {} successfully loaded.'.format(table_id))            

with DAG(
        dag_id='gsheet_to_bq_raw'
        , default_args=args
        , schedule_interval='0 3 * * 1' # every monday at 3am
        , start_date=datetime(2022, 6, 1)
        , end_date=datetime(2022, 11, 1)
        , description='Load event feedback data from Google Sheet to BigQuery'
    ) as dag:

    get_dag_details = PythonOperator(
        task_id='get_dag_details'
        , python_callable=_get_dag_details
    )

    gsheet_sensor = PythonOperator(
        task_id='gsheet_sensor'
        , python_callable=_gsheet_sensor
        , retries=3
        , retry_delay=60
    )

    gsheet_to_pandas = PythonOperator(
        task_id='gsheet_to_pandas'
        , python_callable=_gsheet_to_pandas
        , retries=3
        , retry_delay=60
    )

    pandas_to_bq_raw_data = PythonOperator(
        task_id='pandas_to_bq_raw_data'
        , python_callable=_pandas_to_bq_raw_data
        , retries=3
        , retry_delay=60
    )

    get_dag_details >> gsheet_sensor >> gsheet_to_pandas >> pandas_to_bq_raw_data

if __name__ == '__main__':
    dag.cli()