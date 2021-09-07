import csv
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.python import PythonOperator
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
        SELECT * FROM product
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    with open(f"{DATA_FOLDER}/product-lookup-table.csv", "w") as f:
        writer = csv.writer(f)
        columns = [(
            "UPC",
            "DESCRIPTION",
            "MANUFACTURER",
            "CATEGORY",
            "SUB_CATEGORY",
            "PRODUCT_SIZE",
        )]
        writer.writerows(columns)
        writer.writerows(results)

    logging.info("Extracted the data successfully")


def _remove_column_header():
    df = pd.read_csv(f"{DATA_FOLDER}/product-lookup-table.csv", header=1)
    logging.info(df.head())
    df.to_csv(f"{DATA_FOLDER}/products-without-column-header.csv", index=False, header=False)


default_args = {
    "owner": "zkan",
    "email": ["zkan@hey.com"],
}
with DAG(
    "product_lookup_pipeline",
    schedule_interval="@hourly",
    default_args=default_args,
    start_date=timezone.datetime(2021, 9, 1),
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
        filepath=f"{DATA_FOLDER}/product-lookup-table.csv",
        poke_interval=5,
        timeout=100,
    )

    upload_to_landing_zone = BashOperator(
        task_id="upload_to_landing_zone",
        bash_command=f"hdfs dfs -put -f {DATA_FOLDER}/product-lookup-table.csv {LANDING_ZONE}/product-lookup-table.csv",
    )

    remove_column_header = PythonOperator(
        task_id="remove_column_header",
        python_callable=_remove_column_header,
    )

    upload_to_cleaned_zone = BashOperator(
        task_id="upload_to_cleaned_zone",
        bash_command=f"hdfs dfs -put -f {DATA_FOLDER}/products-without-column-header.csv {CLEANED_ZONE}/products-without-column-header.csv",
    )

    create_product_lookup_table = HiveOperator(
        task_id="create_product_lookup_table",
        hive_cli_conn_id="my_hive_conn",
        hql="""
            CREATE TABLE IF NOT EXISTS dim_product_lookup (
                upc           VARCHAR(100),
                description   VARCHAR(300),
                manufacturer  VARCHAR(100),
                category      VARCHAR(100),
                sub_category  VARCHAR(100),
                product_size  VARCHAR(100)
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
        """,
    )

    load_data_to_hive_table = HiveOperator(
        task_id="load_data_to_hive_table",
        hive_cli_conn_id="my_hive_conn",
        hql=f"""
            LOAD DATA INPATH '{CLEANED_ZONE}/products-without-column-header.csv' OVERWRITE INTO TABLE dim_product_lookup;
        """,
    )

    end = DummyOperator(task_id="end")

    start >> get_data_from_postgres >> is_file_available >> upload_to_landing_zone >> remove_column_header >> upload_to_cleaned_zone >> create_product_lookup_table >> load_data_to_hive_table >> end