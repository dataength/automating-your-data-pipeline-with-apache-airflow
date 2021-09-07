
import csv
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone

from airflow import macros
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

import pandas as pd


DATA_FOLDER = "/usr/local/airflow/dags"
LANDING_ZONE = "/landing"
CLEANED_ZONE = "/cleaned"


def _get_data_from_postgres_by_week_end_date(datestamp):
    week_end_date = macros.ds_format(datestamp, "%Y-%m-%d", "%d-%b-%y")

    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="breakfastatthefrat"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"""
        select * from transaction where week_end_date = '{week_end_date}'
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    with open(f"{DATA_FOLDER}/transaction-data-{datestamp}.csv", "w") as f:
        writer = csv.writer(f)
        columns = [(
            "WEEK_END_DATE",
            "STORE_NUM",
            "UPC",
            "UNITS",
            "VISITS",
            "HHS",
            "SPEND",
            "PRICE",
            "BASE_PRICE",
            "FEATURE",
            "DISPLAY",
            "TPR_ONLY",
        )]
        writer.writerows(columns)
        writer.writerows(results)

    logging.info("Extracted the data successfully")


def _remove_column_header(datestamp):
    try:
        df = pd.read_csv(f"{DATA_FOLDER}/transaction-data-{datestamp}.csv", header=1)
        logging.info(df.head())
    except pd.errors.ParserError:
        df = pd.read_csv(f"{DATA_FOLDER}/transaction-data-{datestamp}.csv")
        logging.info("No data found")

    df.to_csv(f"{DATA_FOLDER}/transaction-data-cleaned-{datestamp}.csv", index=False, header=False)


default_args = {
    "owner": "zkan",
    "email": ["zkan@hey.com"],
}
with DAG(
    "transaction_load_pipeline",
    schedule_interval="0 0 * * THU", # We can then use {{ macros.ds_add(ds, -1) }}
    default_args=default_args,
    start_date=timezone.datetime(2009, 1, 1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    get_data_from_postgres_by_week_end_date = PythonOperator(
        task_id="get_data_from_postgres_by_week_end_date",
        python_callable=_get_data_from_postgres_by_week_end_date,
        op_args=["{{ macros.ds_add(ds, -1) }}"],
    )

    is_file_available = FileSensor(
        task_id="is_file_available",
        fs_conn_id="my_file_conn",
        filepath=f"{DATA_FOLDER}/transaction-data-{{{{ macros.ds_add(ds, -1) }}}}.csv",
        poke_interval=5,
        timeout=100,
    )

    upload_to_landing_zone = BashOperator(
        task_id="upload_to_landing_zone",
        bash_command=f"hdfs dfs -put -f {DATA_FOLDER}/transaction-data-{{{{ macros.ds_add(ds, -1) }}}}.csv {LANDING_ZONE}/transaction-data-{{{{ macros.ds_add(ds, -1) }}}}.csv",
    )

    remove_column_header = PythonOperator(
        task_id="remove_column_header",
        python_callable=_remove_column_header,
        op_args=["{{ macros.ds_add(ds, -1) }}"],
    )

    upload_to_cleaned_zone = BashOperator(
        task_id="upload_to_cleaned_zone",
        bash_command=f"hdfs dfs -put -f {DATA_FOLDER}/transaction-data-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv {CLEANED_ZONE}/transaction-data-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv",
    )

    create_transations_table = HiveOperator(
        task_id="create_transations_table",
        hive_cli_conn_id="my_hive_conn",
        hql="""
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
        """,
    )

    load_data_to_hive_table = HiveOperator(
        task_id="load_data_to_hive_table",
        hive_cli_conn_id="my_hive_conn",
        hql=f"""
            LOAD DATA INPATH '{CLEANED_ZONE}/transaction-data-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv' OVERWRITE INTO TABLE fact_transactions PARTITION (execution_date=date'{{{{ macros.ds_add(ds, -1) }}}}');
        """,
    )

    end = DummyOperator(task_id="end")

    start >> get_data_from_postgres_by_week_end_date >> is_file_available >> upload_to_landing_zone >> remove_column_header >> upload_to_cleaned_zone >> create_transations_table >> load_data_to_hive_table >> end