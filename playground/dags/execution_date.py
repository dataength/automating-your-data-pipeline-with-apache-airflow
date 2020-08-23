

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def split_execution_date(**kwargs):
    execution_date = kwargs['execution_date']
    print(execution_date, type(execution_date))


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}
with DAG('execution_date', schedule_interval='*/5 * * * *', default_args=args, catchup=False) as dag:
    t0 = BashOperator(
        task_id='print_execution_date',
        bash_command='echo {{ ds }} {{ execution_date }} {{ ts }}',
    )

    t1 = PythonOperator(
        task_id='split_execution_date',
        python_callable=split_execution_date,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    t0 >> t1
