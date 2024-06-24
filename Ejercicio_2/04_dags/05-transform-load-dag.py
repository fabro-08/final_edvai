from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Obtener la hora actual
current_time = datetime.now()

with DAG(
    dag_id='ej4-transform-load-dag',
    default_args=default_args,
    description='DAG para transformar y cargar datos en Hive',
    schedule_interval=None,
    start_date=current_time + timedelta(seconds=10),
    dagrun_timeout=timedelta(minutes=60),
    tags=['transform', 'ej-4', 'edvai'],
) as dag:

    start_process = DummyOperator(
        task_id='start_process'
    )

    finish_process = DummyOperator(
        task_id='finish_process',
    )    

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='sshpass -p "edvai" ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/03_transform.py ',
    )    

    start_process >> transform_data >> finish_process


if __name__ == "__main__":
    dag.cli()
