import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Airflow',
    'start_date': days_ago(1),
}
with DAG('postgres_operator_dag',
         schedule_interval='*/15 * * * *',
         default_args=args,
         catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_airflow',
        sql='''
        CREATE TABLE IF NOT EXISTS new_table(
            custom_id integer NOT NULL, 
            timestamp TIMESTAMP NOT NULL, 
            user_id VARCHAR (50) NOT NULL
        );
        ''',
    )

    insert_row = PostgresOperator(
        task_id='insert_row',
        postgres_conn_id='postgres_airflow',
        sql='INSERT INTO new_table VALUES(%s, %s, %s)',
        parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10])
    )

    create_table >> insert_row