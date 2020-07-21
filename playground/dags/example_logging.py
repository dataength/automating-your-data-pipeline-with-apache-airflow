import logging
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}

dag = DAG('example_logging', schedule_interval="@once", default_args=args)


def print_context(**context):
    print(context)
    time.sleep(7)

    logging.debug('This is a debug message')
    logging.info('This is an info message')
    logging.warning('This is a warning message')
    logging.error('This is an error message')
    logging.critical('This is a critical message')

    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=print_context,
    # op_kwargs={'random_base': 1 / 10},
    sla=timedelta(seconds=3),
    dag=dag,
)

run_this
