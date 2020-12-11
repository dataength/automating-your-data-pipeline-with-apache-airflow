from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com',]
}

dag = DAG(
    'test_xcom',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 1),
    catchup=False,
)


def hello(**kwargs):
    kwargs['ti'].xcom_push(key='course', value='Airflow')
    return 'Hello'


t1 = PythonOperator(
    task_id='print_hello',
    python_callable=hello,
    provide_context=True,
    dag=dag,
)


def get_course_name_func(**kwargs):
    course_name = kwargs['ti'].xcom_pull(key='course')
    print(course_name)


t2 = PythonOperator(
    task_id='get_course_name',
    python_callable=get_course_name_func,
    provide_context=True,
    dag=dag,
)

t1 >> t2
