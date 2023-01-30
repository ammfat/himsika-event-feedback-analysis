import os
import google.auth
import json

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# SETUP CREDENTIALS

scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
service_account_file_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
credentials, project_id = google.auth.default(scopes=scopes)

# SETUP DAG

args: dict = {
    'owner': 'ammfat',
}

dataset_name = 'raw_data'
source_table_name = 'cabinet'
source_table_id = project_id + '.' + dataset_name + '.' + source_table_name
dest_table_name = 'temporary_table'
dest_table_id = project_id + '.' + dataset_name + '.' + dest_table_name

# DAG

def get_dag_details():
    return {
        'project_id': project_id,
        'dataset_name': dataset_name,
        'source_table_name': source_table_name,
        'source_table_id': source_table_id,
        'dest_table_name': dest_table_name,
        'dest_table_id': dest_table_id
    }

with DAG(
        dag_id='experiment_bq_to_bq'
        , default_args=args
        , schedule_interval='0 5 * * *'
        , start_date=days_ago(2)
    ) as dag:

    print_dag_details = PythonOperator(
        task_id='print_dag_details',
        python_callable=get_dag_details
    )

    bq_event_to_bq_event = BigQueryOperator(
        task_id='bq_event_to_bq_event',
        sql = f"""
            SELECT * FROM `{source_table_id}` LIMIT 10
            """,
        use_legacy_sql=False,
        destination_dataset_table=dest_table_id,
        write_disposition='WRITE_TRUNCATE'
    )

    print_dag_details >> bq_event_to_bq_event

if __name__ == '__main__':
    dag.cli()