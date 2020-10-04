from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
  'owner': 'ODDS',
}
dag = DAG(
    'get_database_to_csv',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 1),
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)


def query_data_func():
    source  = PostgresHook(postgres_conn_id='my_app')
    conn = source.get_conn()
    cursor = conn.cursor()
    sql = """
        SELECT * FROM words_word
    """
    cursor.execute(sql)
    records = cursor.fetchall()
    for row in records:
        print(row)


query_data = PythonOperator(
    task_id='query_data',
    python_callable=query_data_func,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> query_data >> end
