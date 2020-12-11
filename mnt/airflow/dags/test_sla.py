import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

default_args = {
    'owner': 'zkan',
}

dag = DAG(
    'test_sla',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 1),
    catchup=False,
)


def hello():
    time.sleep(5)
    return 'Hello'

t1 = PythonOperator(
    task_id='my_1st_dummy_task',
    python_callable=hello,
    sla=timedelta(seconds=3),
    dag=dag,
)

t1
