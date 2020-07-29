import random

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'zkan',
}
with DAG(dag_id='branching_dag',
         default_args=args,
         start_date=days_ago(2),
         schedule_interval="@daily",
         tags=['example']) as dag:

    run_this_first = DummyOperator(
        task_id='run_this_first',
        dag=dag,
    )

    options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: random.choice(options),
    )
    run_this_first >> branching

    join = DummyOperator(
        task_id='join',
        trigger_rule='one_success',
    )

    for option in options:
        t = DummyOperator(
            task_id=option,
        )

        dummy_follow = DummyOperator(
            task_id='follow_' + option,
        )

        branching >> t >> dummy_follow >> join