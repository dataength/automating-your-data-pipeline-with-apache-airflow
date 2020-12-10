import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.utils import timezone

import pandas as pd


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
    'sla': timedelta(seconds=30),
}
dag = DAG(
    'product_price_range_pipeline',
    schedule_interval='0 0 * * THU',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Define your pipeline here
check_named_partition = NamedHivePartitionSensor(
    task_id='check_named_partition',
    partition_names=['fact_transactions/execution_date={{ macros.ds_add(ds, -1) }}'],
    metastore_conn_id='my_hive_metastore_conn',
    poke_interval=30,
    dag=dag,
)

create_product_transactions_table = HiveOperator(
    task_id='create_product_transactions_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS zkan_product_transactions (
            product_description STRING,
            price               DECIMAL(10, 2),
            units               INT,
            visits              INT
        )
        PARTITIONED BY (execution_date DATE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;
    ''',
    dag=dag,
)

add_new_product_transactions = HiveOperator(
    task_id='add_new_product_transactions',
    hive_cli_conn_id='my_hive_conn',
    hiveconfs={'hive.exec.dynamic.partition.mode': 'nonstrict'},
    hql='''
        INSERT INTO TABLE zkan_product_transactions
        SELECT dim_product_lookup.description,
            fact_transactions.price,
            fact_transactions.units,
            fact_transactions.visits,
            fact_transactions.execution_date
        FROM fact_transactions
        JOIN dim_product_lookup ON fact_transactions.upc = dim_product_lookup.upc
        WHERE fact_transactions.execution_date = '{{ macros.ds_add(ds, -1) }}'
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> check_named_partition >> create_product_transactions_table >> \
    add_new_product_transactions >> end
