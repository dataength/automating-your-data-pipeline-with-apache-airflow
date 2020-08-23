from datetime import datetime, timedelta

from airflow import DAG

from operators.my_operator import MyOperator
from sensors.my_sensor import MySensor


default_args = {
    'owner': 'zkan',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('custom_hooks_and_operators',
         max_active_runs=3,
         schedule_interval='@once',
         default_args=default_args,
         catchup=False) as dag:

    sens = MySensor(
        task_id='taskA'
    )

    op = MyOperator(
        task_id='taskB',
        my_field='my custom operator'
    )

    sens >> op