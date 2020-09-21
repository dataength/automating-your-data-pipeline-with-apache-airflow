import logging
from datetime import timedelta

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

import pandas as pd


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
}
dag = DAG(
    'product_lookup_pipeline',
    schedule_interval='@hourly',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

is_file_available = FileSensor(
    task_id='is_file_available',
    fs_conn_id='my_file_conn',
    filepath='product-lookup-table.csv',
    poke_interval=5,
    timeout=100,
    dag=dag,
)

DATA_FOLDER = '/usr/local/airflow/dags/files'

def remove_empty_columns_func():
    df = pd.read_csv(f'{DATA_FOLDER}/product-lookup-table.csv', header=1)
    logging.info(df.head())
    df[[
        'UPC', 
        'DESCRIPTION', 
        'MANUFACTURER', 
        'CATEGORY', 
        'SUB_CATEGORY', 
        'PRODUCT_SIZE'
    ]].to_csv(f'{DATA_FOLDER}/products-with-good-columns.csv', index=False, header=False)


remove_empty_columns = PythonOperator(
    task_id='remove_empty_columns',
    python_callable=remove_empty_columns_func,
    # sla=timedelta(seconds=3),
    dag=dag,
)

# Upload to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/products-with-good-columns.csv /products-with-good-columns.csv',
    dag=dag,
)

# Create Hive table
create_product_lookup_table = HiveOperator(
    task_id='create_product_lookup_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS dim_product_lookup (
            UPC          STRING, 
            DESCRIPTION  STRING, 
            MANUFACTURER STRING, 
            CATEGORY     STRING, 
            SUB_CATEGORY STRING, 
            PRODUCT_SIZE STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    ''',
    dag=dag,
)

# Load data in Hive table
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        LOAD DATA INPATH '/products-with-good-columns.csv' OVERWRITE INTO TABLE dim_product_lookup;
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> is_file_available >> remove_empty_columns >> upload_to_hdfs >> create_product_lookup_table >> load_data_to_hive_table >> end
