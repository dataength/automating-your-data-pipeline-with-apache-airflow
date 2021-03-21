from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


def hello(name=''):
    return f'Hello, {name}'


default_args = {
    'owner': 'zkan'
}
with DAG('simple_dag',
         schedule_interval='*/5 * * * *',
         default_args=default_args,
         start_date=timezone.datetime(2020, 8, 15),
         catchup=False) as dag:
    t1 = BashOperator(
        task_id='t1',
        bash_command='echo hello',
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=hello,
        op_args=['Kan'],
        # op_kwargs={'name': 'Kan'},
    )
    t3 = DummyOperator(task_id='t3')
    t4 = DummyOperator(task_id='t4')
    t5 = DummyOperator(task_id='t5')
    t6 = DummyOperator(task_id='t6')
    t7 = DummyOperator(task_id='t7')
    t8 = DummyOperator(task_id='t8')
    t9 = DummyOperator(task_id='t9')

    t1 >> t2 >> t3 >> t4 >> t9
    t2 >> t6 >> t8 >> t9
    t1 >> t5 >> [t6, t7]
    t7 >> t8
