from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def call_xcom_push(**kwargs):
    kwargs['ti'].xcom_push(key='name', value='zkan')


def get_xcom_pull(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='get_op', key='return_value')
    print(xcom_value)
    xcom_value = ti.xcom_pull(task_ids='xcom_push', key='name')
    print(xcom_value)


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}
with DAG('example_xcom_push_pull', schedule_interval="@once", default_args=args) as dag:
    t0 = SimpleHttpOperator(
        task_id='get_op',
        # http_conn_id='http_covid19',
        method='GET',
        endpoint='get',
        data={"param1": "value1", "param2": "value2"},
        # headers={"Content-Type": "application/json"},
        xcom_push=True,
    )

    ty = PythonOperator(
        task_id='xcom_push',
        python_callable=call_xcom_push,
        provide_context=True,
    )

    tx = PythonOperator(
        task_id='xcom_pull',
        python_callable=get_xcom_pull,
        provide_context=True,
    )

    t0 >> ty >> tx