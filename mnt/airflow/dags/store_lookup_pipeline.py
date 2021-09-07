import csv
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils import timezone

import pandas as pd


DATA_FOLDER = "/usr/local/airflow/dags"
LANDING_ZONE = "/landing"
CLEANED_ZONE = "/cleaned"


def _get_data_from_postgres():
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="breakfastatthefrat"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        SELECT * FROM store
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    with open(f"{DATA_FOLDER}/store-lookup-table.csv", "w") as f:
        writer = csv.writer(f)
        columns = [(
            "STORE_ID",
            "STORE_NAME",
            "ADDRESS_CITY_NAME",
            "ADDRESS_STATE_PROV_CODE",
            "MSA_CODE",
            "SEG_VALUE_NAME",
            "PARKING_SPACE_QTY",
            "SALES_AREA_SIZE_NUM",
            "AVG_WEEKLY_BASKETS",
        )]
        writer.writerows(columns)
        writer.writerows(results)

    logging.info("Extracted the data successfully")


def _remove_column_header():
    df = pd.read_csv(f"{DATA_FOLDER}/store-lookup-table.csv", header=1)
    logging.info(df.head())
    df.to_csv(f"{DATA_FOLDER}/stores-without-column-header.csv", index=False, header=False)


default_args = {
    "owner": "zkan",
    "email": ["zkan@hey.com"],
    "sla": timedelta(seconds=30),
}
with DAG(
    "store_lookup_pipeline",
    schedule_interval="@hourly",
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    get_data_from_postgres = PythonOperator(
        task_id="get_data_from_postgres",
        python_callable=_get_data_from_postgres,
    )

    is_file_available = FileSensor(
        task_id="is_file_available",
        fs_conn_id="my_file_conn",
        filepath=f"{DATA_FOLDER}/store-lookup-table.csv",
        poke_interval=5,
        timeout=100,
    )

    upload_to_landing_zone = BashOperator(
        task_id="upload_to_landing_zone",
        bash_command=f"hdfs dfs -put -f {DATA_FOLDER}/store-lookup-table.csv {LANDING_ZONE}/store-lookup-table.csv",
    )

    remove_column_header = PythonOperator(
        task_id="remove_column_header",
        python_callable=_remove_column_header,
    )

    upload_to_cleaned_zone = BashOperator(
        task_id="upload_to_cleaned_zone",
        bash_command=f"hdfs dfs -put -f {DATA_FOLDER}/stores-without-column-header.csv {CLEANED_ZONE}/stores-without-column-header.csv",
    )

    create_store_lookup_table = HiveOperator(
        task_id="create_store_lookup_table",
        hive_cli_conn_id="my_hive_conn",
        hql="""
            CREATE TABLE IF NOT EXISTS dim_store_lookup (
                store_id                 INT,
                store_name               VARCHAR(100),
                address_city_name        VARCHAR(300),
                address_state_prov_code  VARCHAR(2),
                msa_code                 VARCHAR(100),
                seg_value_name           VARCHAR(100),
                parking_space_qty        INT,
                sales_area_size_num      INT,
                avg_weekly_baskets       DECIMAL(38, 2)
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE;
        """,
    )

    load_data_to_hive_table = HiveOperator(
        task_id="load_data_to_hive_table",
        hive_cli_conn_id="my_hive_conn",
        hql=f"""
            LOAD DATA INPATH '{CLEANED_ZONE}/stores-without-column-header.csv' OVERWRITE INTO TABLE dim_store_lookup;
        """,
    )

    end = DummyOperator(task_id="end")

    start >> get_data_from_postgres >> is_file_available >> upload_to_landing_zone >> remove_column_header >> upload_to_cleaned_zone >> create_store_lookup_table >> load_data_to_hive_table >> end