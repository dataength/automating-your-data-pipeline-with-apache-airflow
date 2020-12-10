import logging
from datetime import timedelta

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'sla': timedelta(seconds=30),
}
dag = DAG(
    'store_lookup_pipeline',
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
    bash_command=f'hdfs dfs -get -f {LANDING_ZONE}/store-lookup-table-original.csv {DATA_FOLDER}/store-lookup-table.csv',
    dag=dag,
)

# Check if file is available for further processing
is_file_available = FileSensor(
    task_id='is_file_available',
    fs_conn_id='my_file_conn',
    filepath='store-lookup-table.csv',
    poke_interval=5,
    timeout=100,
    dag=dag,
)

def _remove_empty_columns():
    df = pd.read_csv(f'{DATA_FOLDER}/store-lookup-table.csv', header=1)
    logging.info(df.head())
    df[
        [
            'STORE_ID', 
            'STORE_NAME', 
            'ADDRESS_CITY_NAME', 
            'ADDRESS_STATE_PROV_CODE', 
            'MSA_CODE', 
            'SEG_VALUE_NAME', 
            'PARKING_SPACE_QTY', 
            'SALES_AREA_SIZE_NUM', 
            'AVG_WEEKLY_BASKETS'
        ]
    ].to_csv(f'{DATA_FOLDER}/stores-with-good-columns.csv', index=False, header=False)


remove_empty_columns = PythonOperator(
    task_id='remove_empty_columns',
    python_callable=_remove_empty_columns,
    # sla=timedelta(seconds=3),
    dag=dag,
)

# Upload processed data to cleaned zone
upload_to_cleaned_zone = BashOperator(
    task_id='upload_to_cleaned_zone',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/stores-with-good-columns.csv {CLEANED_ZONE}/stores-with-good-columns.csv',
    dag=dag,
)

# Create Hive table
create_store_lookup_table = HiveOperator(
    task_id='create_store_lookup_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS dim_store_lookup (
            store_id                 INT,
            store_name               VARCHAR(100),
            address_city_name        VARCHAR(300),
            address_state_prov_code  VARCHAR(2),
            msa_code                 VARCHAR(100),
            seg_value_name           VARCHAR(100),
            parking_space_qty        DECIMAL(38, 2),
            sales_area_size_num      INT,
            avg_weekly_baskets       DECIMAL(38, 2)
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;
    ''',
    dag=dag,
)

# Load data in Hive table
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql=f'''
        LOAD DATA INPATH '{CLEANED_ZONE}/stores-with-good-columns.csv' OVERWRITE INTO TABLE dim_store_lookup;
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> download_data_to_local >> is_file_available >> remove_empty_columns >> \
    upload_to_cleaned_zone >> create_store_lookup_table >> \
    load_data_to_hive_table >> end
