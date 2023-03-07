import google.auth
import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from transformation.transformers import column_transformer_for_bq
from transformation.transformers import _transformer_header, _transformer_hide_pii
from transformation.transformers import _transformer_data_enrichment, _transformer_data_cleansing

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

# DAG

def _get_dag_details(**kwargs):
    return {
        'project_id': PROJECT_ID
        , 'bq_raw_data_dataset': bq_raw_data_dataset
        , 'bq_event_feedback_dataset': bq_event_feedback_dataset
        , 'exec_timestamp': kwargs.get('ts')
    }

def _gsheet_sensor(ti, **kwargs):
    from googleapiclient.discovery import build

    gdrive_service = build('drive', 'v3', credentials=CREDENTIALS)

    search_start_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z') # - timedelta(days=7)
    search_end_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z') + timedelta(days=7)
    search_start_date = search_start_date.strftime('%Y-%m-%dT%H:%M:%S')
    search_end_date = search_end_date.strftime('%Y-%m-%dT%H:%M:%S')

    tmp_sheets = []
    pageToken = ""

    while pageToken is not None:
        response = gdrive_service.files().list(
            q=f"""
                mimeType='application/vnd.google-apps.spreadsheet'
                and not name contains 'output'
                and (
                    (createdTime >= '{ search_start_date }'
                    and createdTime <= '{ search_end_date }')
                    or (modifiedTime >= '{ search_start_date }'
                    and modifiedTime <= '{ search_end_date }')
                )
            """
            , fields="nextPageToken, files(id, name, owners, createdTime, modifiedTime)"
        ).execute()

        tmp_sheets.extend(response.get('files', []))

        pageToken = response.get('nextPageToken')

    sheets = [sheet for sheet in tmp_sheets if not sheet['owners'][0]['me']]
    
    ti.xcom_push(key='date', value=[search_start_date, search_end_date])
    ti.xcom_push(key='sheets', value=sheets)

def _task_resume_decision_maker(ti, **kwargs):
    date = ti.xcom_pull(key='date', task_ids='gsheet_sensor')
    sheets = ti.xcom_pull(key='sheets', task_ids='gsheet_sensor')

    print('Date Range: ', date[0], ' - ', date[1])

    if len(sheets) == 0:
        raise AirflowSkipException

    for sheet in sheets:
        print(sheet['id'].ljust(45), sheet['modifiedTime'].ljust(25), sheet['name'])

def _gsheet_to_json_object(ti, **kwargs):
    import json
    import gspread
    import pandas as pd

    date = ti.xcom_pull(key='date', task_ids='gsheet_sensor')
    sheets = ti.xcom_pull(key='sheets', task_ids='gsheet_sensor')

    events = [
        sheet['name'].replace('Feedback ', '')
                .replace('(Jawaban)', '')
                .replace('(Responses)', '')
                .strip()
        for sheet in sheets
    ]

    df_json_objects = []

    for sheet in sheets:
        gsheet = gspread.Client(auth=CREDENTIALS).open_by_key(sheet['id'])
        worksheet = gsheet.sheet1

        df = pd.DataFrame(worksheet.get_all_records()).reset_index(drop=True)

        df['Load Timestamp'] = date[1]
        df['Timestamp'] = df['Timestamp'].astype('datetime64').astype('str').str.replace(' ', 'T')
        df['Date'] = df['Timestamp'].str.split('T').str[0]

        try:
            df = df.drop(columns='')
        except:
            pass

        df_json_data = df.to_json(orient='records')
        df_json_object = json.loads(df_json_data)

        df_json_objects.append(df_json_object)

    ti.xcom_push(key='data', value=[df_json_objects, events])

def _json_object_to_bq_table(ti, **kwargs):
    from google.cloud import bigquery

    key = kwargs.get('key')
    task_id = kwargs.get('task_id')
    bq_dataset_name = kwargs.get('bq_dataset_name')

    df_json_objects, events = ti.xcom_pull(key=key, task_ids=task_id)
    bq_client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

    with bq_client:
        from time import sleep
        
        for df_json_object, event in zip(df_json_objects, events):
            df_json_object = column_transformer_for_bq(df_json_object)

            table_name = event.lower()\
                        .replace(' -', '')\
                        .replace('\'', '')\
                        .replace('/', '_')\
                        .replace('(', '')\
                        .replace(')', '')\
                        .replace(' ', '_')

            table_id = f'{bq_dataset_name}.{table_name}'

            job_config = bigquery.LoadJobConfig(
                autodetect=True
                , source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                , time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY
                    , field='date'
                )
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

def _event_data_cleansed_to_json_object(ti, **kwargs):
    import json
    import pandas as pd

    df, events = ti.xcom_pull(key='event_data_cleansed', task_ids='transformer_data_cleansing')
    df = pd.DataFrame(df)

    # Split dataframe into list of dataframes based on event
    dfs = [
        df.loc[df['Event'] == event]
            .dropna(axis=1, how='all')
            .dropna(axis=0, how='all')
        for event in events
    ]

    df_json_objects = []

    for df in dfs:
        df_json_data = df.to_json(orient='records')
        df_json_object = json.loads(df_json_data)

        df_json_objects.append(df_json_object)

    ti.xcom_push(key='data_cleansed', value=[df_json_objects, events])    

    return 'event_data_cleansed_to_json_object'

with DAG(
        dag_id='gsheet_to_bq'
        , default_args=args
        , schedule_interval='0 3 * * 1' # every monday at 3am
        , start_date=datetime(2022, 6, 1)
        # , end_date=datetime(2023, 2, 28)
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

    task_resume_decision_maker = PythonOperator(
        task_id='task_resume_decision_maker'
        , python_callable=_task_resume_decision_maker
        , retries=3
        , retry_delay=60
    )

    gsheet_to_json_object = PythonOperator(
        task_id='gsheet_to_json_object'
        , python_callable=_gsheet_to_json_object
        , retries=3
        , retry_delay=60
    )

    json_object_to_bq_raw_data = PythonOperator(
        task_id='json_object_to_bq_raw_data'
        , python_callable=_json_object_to_bq_table
        , op_kwargs={
            'key': 'data'
            , 'task_id': 'gsheet_to_json_object'
            , 'bq_dataset_name': bq_raw_data_dataset
        }
        # , retries=3
        # , retry_delay=60
    )

    transformer_header = PythonOperator(
        task_id='transformer_header'
        , python_callable=_transformer_header
        # , retries=3
        # , retry_delay=60
    )

    transformer_data_enrichment = PythonOperator(
        task_id='transformer_data_enrichment'
        , python_callable=_transformer_data_enrichment
        # , retries=3
        # , retry_delay=60
    )

    transformer_hide_pii = PythonOperator(
        task_id='transformer_hide_pii'
        , python_callable=_transformer_hide_pii
        # , retries=3
        # , retry_delay=60
    )

    transformer_data_cleansing = PythonOperator(
        task_id='transformer_data_cleansing'
        , python_callable=_transformer_data_cleansing
        # , retries=3
        # , retry_delay=60
    )

    event_data_cleansed_to_json_object = PythonOperator(
        task_id='event_data_cleansed_to_json_object'
        , python_callable=_event_data_cleansed_to_json_object
        # , retries=3
        # , retry_delay=60
    )

    json_object_to_bq_event_feedback = PythonOperator(
        task_id='json_object_to_bq_event_feedback'
        , python_callable=_json_object_to_bq_table
        , op_kwargs={
            'key': 'data_cleansed'
            , 'task_id': 'event_data_cleansed_to_json_object'
            , 'bq_dataset_name': bq_event_feedback_dataset
        }
        # , retries=3
        # , retry_delay=60
    )

    signal_to_dwh_dag = DummyOperator(
        task_id='signal_to_dwh_dag'
    )

    get_dag_details >> gsheet_sensor >> task_resume_decision_maker >> gsheet_to_json_object
    gsheet_to_json_object >> [json_object_to_bq_raw_data, transformer_header]
    transformer_header >> transformer_data_enrichment >> transformer_hide_pii
    transformer_hide_pii >> transformer_data_cleansing >> event_data_cleansed_to_json_object
    event_data_cleansed_to_json_object >> json_object_to_bq_event_feedback
    [json_object_to_bq_raw_data, json_object_to_bq_event_feedback] >> signal_to_dwh_dag

if __name__ == '__main__':
    dag.cli()