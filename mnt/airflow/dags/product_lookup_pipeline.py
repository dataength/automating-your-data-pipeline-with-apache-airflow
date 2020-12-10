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


DATA_FOLDER = '/usr/local/airflow/dags/files'
LANDING_ZONE = '/landing'
CLEANED_ZONE = '/cleaned'

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

# Download data from HDFS
download_data_to_local = BashOperator(
    task_id='download_data_to_local',
    bash_command=f'hdfs dfs -get -f {LANDING_ZONE}/product-lookup-table-original.csv {DATA_FOLDER}/product-lookup-table.csv',
    dag=dag,
)

# Check if file is available for further processing
is_file_available = FileSensor(
    task_id='is_file_available',
    fs_conn_id='my_file_conn',
    filepath='product-lookup-table.csv',
    poke_interval=5,
    timeout=100,
    dag=dag,
)

def _remove_empty_columns():
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
    python_callable=_remove_empty_columns,
    # sla=timedelta(seconds=3),
    dag=dag,
)

# Upload processed data to cleaned zone
upload_to_cleaned_zone = BashOperator(
    task_id='upload_to_cleaned_zone',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/products-with-good-columns.csv {CLEANED_ZONE}/products-with-good-columns.csv',
    dag=dag,
)

# Create Hive table
create_product_lookup_table = HiveOperator(
    task_id='create_product_lookup_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS dim_product_lookup (
            upc           VARCHAR(100),
            description   VARCHAR(300),
            manufacturer  VARCHAR(100),
            category      VARCHAR(100),
            sub_category  VARCHAR(100),
            product_size  DECIMAL(38, 2)
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
    ''',
    dag=dag,
)

# Load data in Hive table
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql=f'''
        LOAD DATA INPATH '{CLEANED_ZONE}/products-with-good-columns.csv' OVERWRITE INTO TABLE dim_product_lookup;
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> download_data_to_local >> is_file_available >> remove_empty_columns >> \
    upload_to_cleaned_zone >> create_product_lookup_table >> \
    load_data_to_hive_table >> end
