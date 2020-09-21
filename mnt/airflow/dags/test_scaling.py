import time

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan'
}
dag = DAG(
    'test_scaling',
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)


def get_sleep():
    time.sleep(10)


t1 = PythonOperator(
    task_id='t1',
    python_callable=get_sleep,
    dag=dag,
)

t2 = PythonOperator(
    task_id='t2',
    python_callable=get_sleep,
    dag=dag,
)

t3 = PythonOperator(
    task_id='t3',
    python_callable=get_sleep,
    dag=dag,
)

t4 = PythonOperator(
    task_id='t4',
    python_callable=get_sleep,
    dag=dag,
)

t5 = PythonOperator(
    task_id='t5',
    python_callable=get_sleep,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> [t1, t2, t3, t4, t5] >> end
