import random

from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'zkan',
    'email': ['kan@odds.team',],
}
with DAG('test_big_dag',
         start_date=days_ago(1),
         default_args=default_args,
         schedule_interval='*/30 * * * *') as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    for i in range(20):
        first = DummyOperator(task_id=f'next_{i}')
        start >> first
        for j in range(random.randint(5, 15)):
            next = DummyOperator(task_id=f'next_{i}_{j}')
            first >> next
            first = next
        next >> end
