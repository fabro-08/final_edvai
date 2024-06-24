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
    dag_id='elt-dag',
    default_args=default_args,
    description='DAG para ingestar, cargar y transformar datos',
    schedule_interval=None,
    start_date=current_time + timedelta(seconds=10),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform', 'load', 'edvai'],
) as dag:

    start_process = DummyOperator(
        task_id='start_process'
    )

    finish_process = DummyOperator(
        task_id='finish_process',
    )

    download_data = BashOperator(
        task_id='download_data',
        bash_command='sshpass -p "edvai" ssh hadoop@172.17.0.2 bash /home/hadoop/scripts/01_ingest_final.sh ',
    )

    with TaskGroup('put_files_to_hdfs', tooltip='Subir archivos a HDFS') as ingest_hdfs:
        files_to_upload = [
            '2021-informe-ministerio.csv',
            '202206-informe-ministerio.csv',
            'aeropuertos_detalle.csv',
        ]

        for file_name in files_to_upload:
            BashOperator(
                task_id=f'put_{file_name.split(".")[0]}_to_hdfs',
                bash_command=f'sshpass -p "edvai" ssh hadoop@172.17.0.2 /home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/{file_name} /ingest/{file_name}',
            )


    load_data = BashOperator(
        task_id='load_data',
        bash_command='sshpass -p "edvai" ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/04_load.py ',
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='sshpass -p "edvai" ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit /home/hadoop/scripts/05_transform.py ',
    )


    start_process >> download_data >> ingest_hdfs >> load_data >> transform_data >> finish_process


if __name__ == "__main__":
    dag.cli()
