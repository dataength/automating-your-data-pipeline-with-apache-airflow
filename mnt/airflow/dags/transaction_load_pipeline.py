import logging
from datetime import timedelta

from airflow import DAG, macros
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

import pandas as pd


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

DATA_FOLDER = '/usr/local/airflow/dags/files'

def query_data_by_week_end_date_func(ds):
    week_end_date = macros.ds_format(ds, '%Y-%m-%d', '%d-%b-%y')

    df = pd.read_csv(f'{DATA_FOLDER}/transaction-data-table.csv', header=1)
    new_df = df[df.WEEK_END_DATE == week_end_date]
    new_df.to_csv(f'{DATA_FOLDER}/transaction-{ds}.csv', index=False, header=True)
    

# Query data by week end date
query_data_by_week_end_date = PythonOperator(
    task_id='query_data_by_week_end_date',
    python_callable=query_data_by_week_end_date_func,
    op_args=['{{ macros.ds_add(ds, -1) }}'],
    dag=dag,
)

# Remove empty columns
def remove_empty_columns_func(ds):
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
    python_callable=remove_empty_columns_func,
    op_args=['{{ macros.ds_add(ds, -1) }}'],
    dag=dag,
)

# Upload to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv /transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv',
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
    hql='''
        LOAD DATA INPATH '/transaction-cleaned-{{ macros.ds_add(ds, -1) }}.csv' OVERWRITE INTO TABLE fact_transactions PARTITION (execution_date=date'{{ macros.ds_add(ds, -1) }}');
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> query_data_by_week_end_date >> remove_empty_columns >> upload_to_hdfs >> create_transations_table >> load_data_to_hive_table >> end
