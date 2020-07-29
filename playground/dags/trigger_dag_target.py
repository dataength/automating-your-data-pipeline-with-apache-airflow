import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


dag = DAG(
    dag_id='trigger_dag_target',
    default_args={'start_date': airflow.utils.dates.days_ago(2), 'owner': 'zkan'},
    schedule_interval=None,
)

# conf (dict) – Dict containing configuration/parameters to pass to the DAG
# https://airflow.apache.org/docs/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG.create_dagrun
def run_this_func(*args, **kwargs):
    print(f'Remotely received a message: {kwargs["dag_run"].conf["message"]}')
    print(f'external_trigger: {kwargs["dag_run"].external_trigger}')
    print(kwargs['dag_run'])
    print(dir(kwargs['dag_run']))
    return kwargs['dag_run'].conf


run_this = PythonOperator(
    task_id='run_this',
    python_callable=run_this_func,
    provide_context=True,
    dag=dag,
)