from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


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
    dag_id='ej4-ingest-file',
    default_args=default_args,
    description='DAG para ingestar, cargar y transformar datos',
    schedule_interval=None,
    start_date=current_time + timedelta(seconds=10),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'ej-4', 'edvai'],
) as dag:

    start_process = DummyOperator(
        task_id='start_process'
    )

    download_files = BashOperator(
        task_id='download_files',
        bash_command='sshpass -p "edvai" ssh hadoop@172.17.0.2 bash /home/hadoop/scripts/02_ingest_data.sh ',
    )

    with TaskGroup('put_files_to_hdfs', tooltip='Subir archivos a HDFS') as ingest_hdfs:
        files_to_upload = [
            "car_rental_data.csv",
            "georef-united-states-of-america-state.csv"
        ]

        for file_name in files_to_upload:
            BashOperator(
                task_id=f'put_{file_name.split(".")[0]}_to_hdfs',
                bash_command=f'sshpass -p "edvai" ssh hadoop@172.17.0.2 /home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/{file_name} /ingest/{file_name}',
            )

    trigger_transform_load_dag = TriggerDagRunOperator(
        task_id='trigger_transform_load_dag',
        trigger_dag_id='ej4-transform-load-dag',  # El ID del DAG hijo
    )

    start_process >> download_files >> ingest_hdfs >> trigger_transform_load_dag


if __name__ == "__main__":
    dag.cli()
