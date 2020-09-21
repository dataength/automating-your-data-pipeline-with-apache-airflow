import logging
import random
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils import timezone

import pandas as pd


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
    'sla': timedelta(seconds=30),
}
dag = DAG(
    'branching',
    schedule_interval='0 0 * * THU',
    default_args=default_args,
    start_date=timezone.datetime(2009, 1, 1),
    catchup=False,
)


def test_condition():
    options = ['branch_a', 'branch_b']
    return random.choice(options)


branching = BranchPythonOperator(
    task_id='branching',
    python_callable=test_condition,
    dag=dag,
)

branch_a = DummyOperator(
    task_id='branch_a',
    dag=dag,
)

branch_b = DummyOperator(
    task_id='branch_b',
    dag=dag,
)

t0 = DummyOperator(
    task_id='t0',
    dag=dag,
)

t1 = DummyOperator(
    task_id='t1',
    dag=dag,
)

join = DummyOperator(
    task_id='join',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

for i in range(3):
    i = DummyOperator(
        task_id=f'task-{i}',
        dag=dag,
    )
    branching >> i

branching >> [branch_a, branch_b]
branch_a >> join
branch_b >> t0 >> t1 >> join