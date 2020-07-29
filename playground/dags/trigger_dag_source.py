import airflow
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator


dag = DAG(
    dag_id='trigger_dag_source',
    default_args={'start_date': airflow.utils.dates.days_ago(2), 'owner': 'zkan'},
    schedule_interval='@once',
)


def trigger(context, dag_run_obj):
    print(context)
    dag_run_obj.payload = {'message': context['params']['message']}
    return dag_run_obj


trigger = TriggerDagRunOperator(
    dag=dag,
    task_id='test_trigger_dagrun',
    trigger_dag_id="trigger_dag_target",
    python_callable=trigger,
    params={'message': 'Hello World'}
)