from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan'
}
dag = DAG(
    'test_scheduling_notebooks',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False
)

NOTEBOOKS_FOLDER = '/usr/local/airflow/dags/notebooks'

t1 = PapermillOperator(
    task_id='t1',
    input_nb=f'{NOTEBOOKS_FOLDER}/input.ipynb',
    output_nb=f'{NOTEBOOKS_FOLDER}/output.ipynb',
    parameters=dict(name='ODDS', x=0.1, y=10),
    dag=dag,
)
t2 = PapermillOperator(
    task_id='t2',
    input_nb=f'{NOTEBOOKS_FOLDER}/input.ipynb',
    output_nb=f'{NOTEBOOKS_FOLDER}/output-{{{{ execution_date }}}}.ipynb',
    parameters={'msg': 'Ran from Airflow at {{ execution_date }}!'},
    dag=dag,
)

t1 >> t2