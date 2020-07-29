from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'zkan',
    'start_date': days_ago(2),
    'provide_context': True,
}
with DAG('try_ssh_operator',
         schedule_interval='@once',
         default_args=args) as dag:

    t0 = SSHOperator(
        task_id='run_ssh',
        ssh_conn_id='ssh_default',
        command='ls',
    )

    t0