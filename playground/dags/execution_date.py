from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


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

    t0
