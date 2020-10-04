from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone


default_args = {
  'owner': 'ODDS',
}
dag = DAG(
    'my_dummy_dag',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 1),
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)
do_something = DummyOperator(task_id='do_something', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> do_something >> end
