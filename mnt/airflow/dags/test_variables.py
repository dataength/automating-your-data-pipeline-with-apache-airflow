import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan'
}
dag = DAG(
    'test_variables',
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

def get_var():
    foo = Variable.get('foo', default_var=None)
    logging.info(foo)

    bar = Variable.get('bar', deserialize_json=True, default_var=None)
    logging.info(bar)


t1 = PythonOperator(
    task_id='t1',
    python_callable=get_var,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> t1 >> end
