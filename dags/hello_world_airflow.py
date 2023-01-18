from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'ammfat-developer',
}

with DAG(
    dag_id='hello_world_airflow',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(7),
) as dag:

    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo Hello',
    )

    print_world = BashOperator(
        task_id='print_world',
        bash_command='echo World',
    )

    print_ammfat = BashOperator(
        task_id='print_ammfat',
        bash_command='echo Hello from Ammfat!'
    )

    print_hello >> print_world >> print_ammfat

if __name__ == "__main__":
    dag.cli()

