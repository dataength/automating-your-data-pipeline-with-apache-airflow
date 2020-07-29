import random

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'zkan',
}
with DAG(dag_id='file_sensor',
         default_args=args,
         start_date=days_ago(2),
         schedule_interval="@daily",
         tags=['example']) as dag:

    sensor_task = FileSensor(task_id='my_file_sensor_task',
                             poke_interval=5, 
                             fs_conn_id='my_fs',
                             filepath='data.txt')

    run_this_later = DummyOperator(
        task_id='run_this_later',
    )

    sensor_task >> run_this_later