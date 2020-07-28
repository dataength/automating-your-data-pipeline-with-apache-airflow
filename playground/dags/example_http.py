from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}
with DAG('example_my_http', schedule_interval="@once", default_args=args) as dag:
    t0 = SimpleHttpOperator(
        task_id='get_op',
        # http_conn_id='http_covid19',
        method='GET',
        endpoint='get',
        data={"param1": "value1", "param2": "value2"},
        # headers={"Content-Type": "application/json"},
        xcom_push=True,
    )

    t0