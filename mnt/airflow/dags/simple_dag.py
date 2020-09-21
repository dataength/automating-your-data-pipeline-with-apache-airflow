from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan'
}
dag = DAG(
    'simple_dag',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False
)

t1 = BashOperator(
    task_id='t1',
    bash_command='echo hello',
    dag=dag,
)


def hello(name=''):
    return f'Hello, {name}'


t2 = PythonOperator(
    task_id='t2',
    python_callable=hello,
    op_args=['Kan'],
    # op_kwargs={'name': 'Kan'},
    dag=dag,
)

t3 = DummyOperator(
    task_id='t3',
    dag=dag,
)

t4 = DummyOperator(
    task_id='t4',
    dag=dag,
)

t5 = DummyOperator(
    task_id='t5',
    dag=dag,
)

t6 = DummyOperator(
    task_id='t6',
    dag=dag,
)

t7 = DummyOperator(
    task_id='t7',
    dag=dag,
)

t8 = DummyOperator(
    task_id='t8',
    dag=dag,
)

t9 = DummyOperator(
    task_id='t9',
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t9
t2 >> t6 >> t8 >> t9
t1 >> t5 >> [t6, t7]
t7 >> t8
