import logging
from datetime import timedelta

from airflow import DAG, macros
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
    'sla': timedelta(seconds=30),
}
dag = DAG(
    'transaction_load_pipeline',
    schedule_interval='0 0 * * THU', # We can then use {{ macros.ds_add(ds, -1) }}
    default_args=default_args,
    start_date=timezone.datetime(2009, 1, 1),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)


def _query_data_by_week_end_date(ds):
    week_end_date = macros.ds_format(ds, '%Y-%m-%d', '%d-%b-%y')

    df = pd.read_csv(f'{DATA_FOLDER}/transaction-data-table-original.csv', header=1)
    new_df = df[df.WEEK_END_DATE == week_end_date]
    new_df.to_csv(f'{DATA_FOLDER}/transaction-{ds}-original.csv', index=False, header=True)


# Query data by week end date
query_data_by_week_end_date = PythonOperator(
    task_id='query_data_by_week_end_date',
    python_callable=_query_data_by_week_end_date,
    op_args=['{{ macros.ds_add(ds, -1) }}'],
    dag=dag,
)

# Upload raw data to landing zone
upload_to_landing_zone = BashOperator(
    task_id='upload_to_landing_zone',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/transaction-{{{{ macros.ds_add(ds, -1) }}}}-original.csv {LANDING_ZONE}/transaction-{{{{ macros.ds_add(ds, -1) }}}}-original.csv',
    dag=dag,
)

# Download data from HDFS
download_data_to_local = BashOperator(
    task_id='download_data_to_local',
    bash_command=f'hdfs dfs -get -f {LANDING_ZONE}/transaction-{{{{ macros.ds_add(ds, -1) }}}}-original.csv {DATA_FOLDER}/transaction-{{{{ macros.ds_add(ds, -1) }}}}.csv',
    dag=dag,
)

# Check if file is available for further processing
is_file_available = FileSensor(
    task_id='is_file_available',
    fs_conn_id='my_file_conn',
    filepath='transaction-{{ macros.ds_add(ds, -1) }}.csv',
    poke_interval=5,
    timeout=100,
    dag=dag,
)


def _remove_empty_columns(ds):
    df = pd.read_csv(f'{DATA_FOLDER}/transaction-{ds}.csv')
    logging.info(df.head())
    df[
        [
            'WEEK_END_DATE', 
            'STORE_NUM', 
            'UPC', 
            'UNITS', 
            'VISITS', 
            'HHS', 
            'SPEND', 
            'PRICE', 
            'BASE_PRICE', 
            'FEATURE', 
            'DISPLAY', 
            'TPR_ONLY'
        ]
    ].to_csv(f'{DATA_FOLDER}/transaction-cleaned-{ds}.csv', index=False, header=False)


remove_empty_columns = PythonOperator(
    task_id='remove_empty_columns',
    python_callable=_remove_empty_columns,
    op_args=['{{ macros.ds_add(ds, -1) }}'],
    dag=dag,
)

# Upload processed data to cleaned zone
upload_to_cleaned_zone = BashOperator(
    task_id='upload_to_cleaned_zone',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv {CLEANED_ZONE}/transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv',
    dag=dag,
)

# Create Hive table with partition
create_transations_table = HiveOperator(
    task_id='create_transations_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS fact_transactions (
            week_end_date VARCHAR(40),
            store_num     INT,
            upc           VARCHAR(100),
            units         INT,
            visits        INT,
            hhs           INT,
            spend         DECIMAL(38, 2),
            price         DECIMAL(38, 2),
            base_price    DECIMAL(38, 2),
            feature       INT,
            display       INT,
            tpr_only      INT
        )
        PARTITIONED BY (execution_date DATE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;
    ''',
    dag=dag,
)

# Load data in Hive table with partition
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql=f'''
        LOAD DATA INPATH '{CLEANED_ZONE}/transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv' OVERWRITE INTO TABLE fact_transactions PARTITION (execution_date=date'{{{{ macros.ds_add(ds, -1) }}}}');
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> query_data_by_week_end_date >> upload_to_landing_zone >> \
    download_data_to_local >> is_file_available >> remove_empty_columns >> upload_to_cleaned_zone >> \
    create_transations_table >> load_data_to_hive_table >> end
