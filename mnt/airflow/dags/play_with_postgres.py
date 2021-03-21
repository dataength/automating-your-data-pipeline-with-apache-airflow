import datetime

from airflow import macros
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


def get_data():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        SELECT * FROM product
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)


def dump_data(table: str):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    pg_hook.bulk_dump(table, f'/usr/local/airflow/dags/{table}_export')


def dump_transaction_data_each_week(yesterday_ds, **kwargs):
    week_end_date = macros.ds_format(yesterday_ds, '%Y-%m-%d', '%d-%b-%y')

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"""
        SELECT * FROM transaction WHERE week_end_date = '{week_end_date}'
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)


with DAG(
        dag_id='play_with_postgres',
        default_args={'start_date': datetime.datetime(2017, 5, 5)},
        # schedule_interval='*/10 * * * *',
        schedule_interval='0 0 * * THU',
        catchup=False) as dag:

    t1 = PostgresOperator(
        task_id='query_some_data',
        postgres_conn_id='my_postgres_conn',
        sql="""
            SELECT * FROM transaction LIMIT 10;
        """,
    )

    t2 = PythonOperator(
        task_id='hook_task',
        python_callable=get_data,
    )

    t3 = PythonOperator(
        task_id='dump_product_task',
        python_callable=dump_data,
        op_kwargs={'table': 'product'},
    )

    t4 = PythonOperator(
        task_id='dump_store_task',
        python_callable=dump_data,
        op_kwargs={'table': 'store'},
    )

    t5 = PythonOperator(
        task_id='dump_transaction_task',
        python_callable=dump_data,
        op_kwargs={'table': 'transaction'},
    )

    t6 = PythonOperator(
        task_id='dump_transaction_each_week',
        python_callable=dump_transaction_data_each_week,
        provide_context=True,
    )

    t1 >> t2 >> [t3, t4, t5] >> t6
