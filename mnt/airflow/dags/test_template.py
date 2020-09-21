from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan'
}
dag = DAG(
    'test_template',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False
)


def hello(ds, prev_ds):
    return (ds, prev_ds)


t1 = PythonOperator(
    task_id='t1',
    python_callable=hello,
    op_args=[
        '{{ macros.ds_format(ds, "%Y-%m-%d", "%d-%b-%y") }}',
        '{{ macros.ds_add(ds, -1) }}'
    ],
    # op_kwargs={'name': 'Kan'},
    dag=dag,
)

t1
