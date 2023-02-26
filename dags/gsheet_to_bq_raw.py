import google.auth
import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
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

def _task_resume_decision_maker(ti, **kwargs):
    date = ti.xcom_pull(key='date', task_ids='gsheet_sensor')
    sheets = ti.xcom_pull(key='sheets', task_ids='gsheet_sensor')

    print('Date Range: ', date[0], ' - ', date[1])

    if len(sheets) == 0:
        raise AirflowSkipException

    for sheet in sheets:
        print(sheet['id'].ljust(45), sheet['createdTime'].ljust(25), sheet['name'])

def _event_column_transformer_for_bq(df):
    import json
    import pandas as pd

    print("!!!!", type(df))

    df = pd.DataFrame(df)
    df.columns = df.columns.str.replace('[^A-Za-z0-9\s]+', '').str.lower().str.replace(' ', '_')

    print("!!!!", type(df))

    return json.loads(df.to_json(orient='records'))

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

        df['Load Date'] = date[1]
        df['Timestamp'] = df['Timestamp'].astype('datetime64').astype('str').str.replace(' ', 'T')
        # df.columns = df.columns.str.replace('[^A-Za-z0-9\s]+', '').str.lower().str.replace(' ', '_')  # causes event_column_name to not match

        try:
            df = df.drop(columns='')
        except:
            pass

        df_json_data = df.to_json(orient='records')
        df_json_object = json.loads(df_json_data)

        df_json_objects.append(df_json_object)

    ti.xcom_push(key='data', value=[df_json_objects, events])

def _json_object_to_bq_raw_data(ti, **kwargs):
    from google.cloud import bigquery

    df_json_objects, events = ti.xcom_pull(key='data', task_ids='gsheet_to_json_object')
    bq_client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

    with bq_client:
        from time import sleep
        
        for df_json_object, event in zip(df_json_objects, events):
            df_json_object = _event_column_transformer_for_bq(df_json_object)

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

def _event_header_transformer(ti, **kwargs):
    import json
    import pandas as pd
    
    dfs = []
    dag_folder = os.path.dirname(__file__)
    json_file_path = os.path.join(dag_folder, 'transformation/event_columns_to_replace.json')
    df_json_objects, events = ti.xcom_pull(key='data', task_ids='gsheet_to_json_object')
    
    with open(json_file_path, 'r') as f:
        event_columns_to_replace = json.load(f)
    
    # define the regex patterns to drop certain columns
    drop_patterns = ['[Ff]ollow', '[Mm]erge', 'no', '[Pp]resensi']

    for df, event in zip(df_json_objects, events):
        df = pd.DataFrame(df)

        print("Before transformation: ", df.columns)

        df['Event'] = event.title()
    
        for pattern in drop_patterns:
            df = df[df.columns.drop(list(df.filter(regex=pattern)))]
        
        df = df.rename(columns=event_columns_to_replace)

        tmp_cols = [col for col in df.columns if 'Pemaparan materi oleh' in col]
        if len(tmp_cols) > 0:
            df['Kepuasan terhadap Pemateri'] = df[tmp_cols].mean(axis=1).round()
            df = df[df.columns.drop(list(df.filter(like='Pemaparan materi oleh ')))]

        print("After transformation: ", df.columns)
        dfs.append(df)

    ti.xcom_push(key='event_header_cleansed', value=dfs)

    return 'Event HEADER Transformer'

def _event_data_transformer(ti, **kwargs):
    import pandas as pd

    dfs = ti.xcom_pull(key='event_header_cleansed', task_ids='event_header_transformer')
    df = pd.concat(dfs).reset_index(drop=True)

    return 'Event Data Transformer'

with DAG(
        dag_id='gsheet_to_bq_raw'
        , default_args=args
        , schedule_interval='0 3 * * 1' # every monday at 3am
        , start_date=datetime(2022, 6, 1)
        , end_date=datetime(2022, 8, 1)
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
        , python_callable=_json_object_to_bq_raw_data
        , retries=3
        , retry_delay=60
    )

    event_header_transformer = PythonOperator(
        task_id='event_header_transformer'
        , python_callable=_event_data_transformer
        # , retries=3
        # , retry_delay=60
    )

    event_data_transformer = PythonOperator(
        task_id='event_data_transformer'
        , python_callable=_event_data_transformer
        # , retries=3
        # , retry_delay=60
    )

    get_dag_details >> gsheet_sensor >> gsheet_to_json_object >> task_resume_decision_maker
    task_resume_decision_maker >> [json_object_to_bq_raw_data, event_header_transformer]
    event_header_transformer >> event_data_transformer

if __name__ == '__main__':
    dag.cli()